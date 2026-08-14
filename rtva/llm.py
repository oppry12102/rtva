"""Async MiniMax-M3 client with retry, JSON repair, and usage tracking.

Key learnings baked in (benchmarked against the live API):
  - M3 is a reasoning model; only `{"thinking": {"type": "disabled"}}` actually
    disables it (reasoning_effort / chat_template_kwargs are ignored).
  - Without that, reasoning tokens eat the output budget and `content` returns "".
  - JSON adherence is reliable when an explicit schema + "ONLY JSON" instruction
    is in the system prompt; we belt-and-suspenders with `response_format`.
  - `cached_tokens` is observed in `usage.prompt_tokens_details` — a STABLE
    system prompt prefix hits the cache and drops latency from ~4s to ~2s.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from PIL import Image

from .config import get_settings


# --- Types --------------------------------------------------------------------


@dataclass
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    calls: int = 0


@dataclass
class WindowResult:
    narrative: str
    events: list[dict]            # raw dicts; Event construction happens in pipeline
    key_entities: list[str]
    raw_text: str                 # for debugging
    usage: Usage
    latency_s: float
    parse_failures: int = 0


# --- Helpers ------------------------------------------------------------------


def encode_jpeg(rgb: np.ndarray, size: tuple[int, int]) -> str:
    """Downsample to `size` and JPEG-encode to base64 string."""
    import numpy as np
    im = Image.fromarray(rgb).convert("RGB").resize(size, Image.BILINEAR)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=72)
    return base64.b64encode(buf.getvalue()).decode()


def _strip_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    return s


def _extract_json(s: str) -> Optional[dict]:
    """Find the outermost {...} and parse it. Tolerant to surrounding prose."""
    s = _strip_fences(s)
    # first try direct
    try:
        return json.loads(s)
    except Exception:
        pass
    # find first { and matching close
    m = re.search(r"\{", s)
    if not m:
        return None
    depth = 0
    for i in range(m.start(), len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(s[m.start():i+1])
                except Exception:
                    return None
    return None


def _validate_window_response(d: dict) -> Optional[tuple[str, list, list[str]]]:
    """Return (narrative, events, key_entities) or None on schema mismatch."""
    if not isinstance(d, dict):
        return None
    narr = d.get("narrative")
    evs = d.get("events")
    ents = d.get("key_entities")
    if not isinstance(narr, str) or not isinstance(evs, list) or not isinstance(ents, list):
        return None
    cleaned: list[dict] = []
    for ev in evs:
        if not isinstance(ev, dict):
            continue
        try:
            cleaned.append({
                "type": str(ev.get("type", "action")),
                "t_start": float(ev.get("t_start", 0.0)),
                "t_end": float(ev.get("t_end", ev.get("t_start", 0.0))),
                "description": str(ev.get("description", "")),
                "confidence": float(ev.get("confidence", 0.5)),
                "actors": [str(x) for x in (ev.get("actors") or [])],
                "objects": [str(x) for x in (ev.get("objects") or [])],
                "location": str(ev.get("location", "")),
                "key_entities": [str(x) for x in (ev.get("key_entities") or [])],
                "is_continuation": bool(ev.get("is_continuation", False)),
            })
        except Exception:
            continue
    return narr.strip(), cleaned, [str(e) for e in ents]


# --- Client -------------------------------------------------------------------


@dataclass
class M3Client:
    api_key: str = field(default_factory=lambda: get_settings().minimax_api_key)
    base_url: str = field(default_factory=lambda: get_settings().minimax_base_url)
    timeout_s: float = field(default_factory=lambda: get_settings().request_timeout_s)
    max_retry: int = field(default_factory=lambda: get_settings().max_retry)
    cumulative_usage: Usage = field(default_factory=Usage)

    _client: Optional[httpx.AsyncClient] = field(default=None, init=False)

    async def __aenter__(self) -> "M3Client":
        self._client = httpx.AsyncClient(timeout=self.timeout_s)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _post(self, payload: dict) -> dict:
        assert self._client is not None, "use `async with M3Client() as c:`"
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retry + 1):
            try:
                resp = await self._client.post(
                    self.base_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {self.api_key}",
                             "Content-Type": "application/json"},
                )
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                data = resp.json()
                br = data.get("base_resp", {})
                if br.get("status_code") and br["status_code"] != 0:
                    raise RuntimeError(f"api error: {br}")
                return data
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.NetworkError, RuntimeError) as e:
                last_exc = e
                if attempt == self.max_retry:
                    break
                await asyncio.sleep(0.8 * (2 ** attempt))
        raise last_exc or RuntimeError("unknown error")

    def _accumulate(self, raw: dict) -> Usage:
        u = raw.get("usage", {})
        pt = u.get("prompt_tokens", 0)
        ct = u.get("completion_tokens", 0)
        cached = u.get("prompt_tokens_details", {}).get("cached_tokens", 0)
        self.cumulative_usage.prompt_tokens += pt
        self.cumulative_usage.completion_tokens += ct
        self.cumulative_usage.cached_tokens += cached
        self.cumulative_usage.total_tokens += u.get("total_tokens", pt + ct)
        self.cumulative_usage.calls += 1
        return Usage(
            prompt_tokens=pt, completion_tokens=ct,
            cached_tokens=cached, total_tokens=u.get("total_tokens", pt + ct), calls=1,
        )

    async def analyze_window(
        self,
        system_prompt: str,
        user_text: str,
        frames_b64: list[str],
        *,
        escalate: bool = False,
    ) -> WindowResult:
        """Send a window with thinking disabled (or enabled for escalation).

        Implements the JSON repair chain: try once, on failure retry once with a
        corrective user turn, on second failure return raw text and parse_failures=2.
        """
        content: list[dict] = [{"type": "text", "text": user_text}]
        for b64 in frames_b64:
            content.append({"type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})

        async def call_once(thinking_disabled: bool, msgs: list[dict]) -> dict:
            payload = {
                "model": "MiniMax-M3",
                "max_tokens": 800 if escalate else 250,
                "temperature": 0.2,
                "messages": msgs,
                "response_format": {"type": "json_object"},
            }
            if thinking_disabled:
                payload["thinking"] = {"type": "disabled"}
            return await self._post(payload)

        msgs = [{"role": "system", "content": system_prompt},
                {"role": "user", "content": content}]
        t0 = time.monotonic()
        try:
            raw = await call_once(not escalate, msgs)
        except Exception:
            # second try with reduced frame count would normally happen here;
            # for now propagate and let scheduler count it as failed.
            raise
        dt = time.monotonic() - t0
        usage = self._accumulate(raw)
        text = raw["choices"][0]["message"].get("content") or ""

        parsed = _extract_json(text)
        validated = _validate_window_response(parsed) if parsed else None
        parse_failures = 0

        if validated is None:
            parse_failures = 1
            # retry once with corrective user turn
            corrective = msgs + [{"role": "assistant", "content": text},
                                 {"role": "user", "content":
                                  "Your previous reply was not valid JSON. "
                                  "Return ONLY valid JSON matching the schema. "
                                  "Do not change the content."}]
            try:
                raw2 = await call_once(not escalate, corrective)
            except Exception:
                return WindowResult(narrative="", events=[], key_entities=[],
                                    raw_text=text, usage=usage, latency_s=dt,
                                    parse_failures=parse_failures)
            dt += time.monotonic() - t0 - dt
            u2 = self._accumulate(raw2)
            text2 = raw2["choices"][0]["message"].get("content") or ""
            parsed2 = _extract_json(text2)
            validated = _validate_window_response(parsed2) if parsed2 else None
            if validated is None:
                return WindowResult(narrative="", events=[], key_entities=[],
                                    raw_text=text2, usage=u2, latency_s=dt,
                                    parse_failures=2)
            usage = u2
            text = text2

        narrative, events, key_entities = validated
        return WindowResult(narrative=narrative, events=events, key_entities=key_entities,
                            raw_text=text, usage=usage, latency_s=dt,
                            parse_failures=parse_failures)
