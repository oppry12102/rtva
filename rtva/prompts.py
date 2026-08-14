"""Stable sink system prompt + per-window user message.

The system prompt is built ONCE per session and NEVER changes — it is the
prefix that hits MiniMax's server-side prompt cache. Anything that varies
per request must go into the user message.

The user message holds the rolling context (recent narratives, recent events)
plus this window's frames. Hard truncation; no LLM-based compaction in v1.
"""

from __future__ import annotations

from typing import Iterable

# --- The SINK. Treat as a frozen string. ~600 tokens. -------------------------

SYSTEM_PROMPT_EN = """You are ZXUN, a real-time video analyst. You receive short sliding windows of frames from a live stream (sampled at ~8fps within each window). Your output is appended to a rolling log viewed by humans.

For each window you MUST emit a single JSON object with EXACTLY these top-level keys:
  - "narrative": one or two sentences (<=40 words) describing what is happening in this window. Reference earlier context only when something CHANGES (e.g., "the same person now sits down"). Avoid filler ("The video shows...", "In this frame...").
  - "events": array of zero or more key events. Each event MUST match the schema below.
  - "key_entities": array of normalized lowercase noun phrases (people, objects, locations) referenced anywhere in the window, used for cross-window continuity.

Event schema (every field REQUIRED):
{
  "type": "action" | "scene_change" | "object_appear" | "object_disappear" | "anomaly" | "transition",
  "t_start": <float seconds, relative to this window's start>,
  "t_end":   <float seconds, relative to this window's start; >= t_start>,
  "description": "<=20 words, specific and concrete. Name the actor, the action, the object, and the location if visible.>",
  "confidence": <float 0..1; lower if uncertain>,
  "actors":   [<lowercase noun phrases>],
  "objects":  [<lowercase noun phrases>],
  "location": "<short spatial tag, e.g. 'left of frame', 'center counter'>",
  "key_entities": [<lowercase noun phrases within this event>],
  "is_continuation": <true if this event continues or refines an event already in RECENT EVENTS below; false otherwise>
}

Rules:
- Emit 0 events only if the window is genuinely static and the recent context already covers it.
- Prefer ONE well-described event over several vague ones.
- Confidence reflects YOUR uncertainty, not importance.
- Do NOT invent audio, speech, text-on-screen you cannot read, or events outside the frames.
- Output ONLY the JSON object. No markdown fences. No prose before or after."""


# Chinese variant — identical schema, but "narrative" and "description" strings
# must be written in Simplified Chinese. Type values stay English (canonical
# enums shared by `_MERGEABLE_TYPES` in rtva/events.py and by downstream
# clients doing type-based routing).
SYSTEM_PROMPT_ZH = """你是 ZXUN，一名实时视频分析助手。你从实时视频流中接收由滑动窗口切分的连续帧（每个窗口内约 8 fps）。你的输出会追加到人类查看的滚动日志里。

每个窗口你必须输出一个 JSON 对象，**且只包含**以下顶层字段：
  - "narrative"：一到两句（<=40 个词 / 40 个汉字），用**简体中文**描述本窗口内正在发生的事情。仅在状态发生变化时引用前面的上下文（例如「同一个人现在坐下了」）。避免「视频显示……」「画面中……」之类的废话。
  - "events"：零个或多个关键事件的数组。每个事件必须符合下面的 schema。
  - "key_entities"：本窗口中出现的归一化小写名词短语（人物、物体、地点），用于跨窗口的实体连续性。

事件 schema（每个字段都必填）：
{
  "type": "action" | "scene_change" | "object_appear" | "object_disappear" | "anomaly" | "transition",
  "t_start": <浮点秒，相对于本窗口起始>,
  "t_end":   <浮点秒，相对于本窗口起始，且 >= t_start>,
  "description": "<=20 个汉字，具体且有信息量。点名施动者、动作、物体，以及画面中可见的位置。**必须用简体中文写。**>",
  "confidence": <浮点 0..1，越不确定越低>,
  "actors":   [<小写名词短语>],
  "objects":  [<小写名词短语>],
  "location": "<简短空间标签，如「画面左侧」「中央台面」>**用中文**>",
  "key_entities": [<本事件内的小写名词短语>],
  "is_continuation": <true 表示本事件延续或细化 RECENT EVENTS 中已有的事件；否则 false>
}

注意："type" 字段必须是英文枚举值之一（action/scene_change/object_appear/object_disappear/anomaly/transition），不要用中文。这是约定的机器可读字段。narrative 和 description 等人类阅读的字符串则用简体中文。

规则：
- 窗口确实静止、且近期上下文已覆盖时，才输出 0 个事件。
- 一个描述清晰的事件优于多个模糊的事件。
- confidence 反映的是你的**不确定性**，不是重要性。
- 不要编造听不到的声音、读不出的字幕、画面外的事件。
- 只输出 JSON 对象。**不要 markdown 围栏**，前后不要任何散文。"""


SUPPORTED_LANGUAGES = ("zh", "en")


def build_system_prompt(language: str = "zh") -> str:
    """Return the system prompt for the requested narration language.

    Type enum values are always English (canonical). The narrative/description
    strings flip to Chinese when language="zh".
    """
    if language not in SUPPORTED_LANGUAGES:
        # Unknown language falls back to Chinese — the requested default.
        return SYSTEM_PROMPT_ZH
    return SYSTEM_PROMPT_ZH if language == "zh" else SYSTEM_PROMPT_EN


# Backwards-compat alias for any external caller still importing the old name.
# New code should call build_system_prompt(language) instead.
SYSTEM_PROMPT = SYSTEM_PROMPT_ZH


# --- Per-window USER message --------------------------------------------------


def build_user_message(
    window_id: int,
    t_start: float,
    t_end: float,
    recent_narratives: Iterable[str],
    recent_events_lines: Iterable[str],
    scene_summary: str,
    frame_offsets: list[float],
) -> str:
    """Assemble the variable part of the prompt.

    `frame_offsets[i]` is the seconds offset of frame i within the window.
    The actual image bytes are appended by the caller as image_url parts.
    """
    duration = t_end - t_start
    narr_lines = "\n".join(f"- {n}" for n in recent_narratives) or "(none)"
    ev_lines = "\n".join(recent_events_lines) or "(none)"
    summary = scene_summary or "(none yet)"

    frame_lines = "\n".join(
        f"[+{off:.2f}s] <image {i+1}>" for i, off in enumerate(frame_offsets)
    )

    return (
        f"Window {window_id} | stream time {t_start:.2f}s – {t_end:.2f}s | "
        f"duration {duration:.2f}s\n\n"
        f"## GLOBAL CONTEXT (rolling summary)\n{summary}\n\n"
        f"## RECENT WINDOW NARRATIVES (oldest -> newest)\n{narr_lines}\n\n"
        f"## RECENT EVENTS (oldest -> newest, may not yet be finalized)\n{ev_lines}\n\n"
        f"## THIS WINDOW\n"
        f"Frames below, oldest first:\n{frame_lines}\n\n"
        f"Return ONLY the JSON object. No markdown. No explanation."
    )


def build_escalation_user_message(
    window_id: int,
    t_start: float,
    t_end: float,
    fast_pass_summary: str,
    frame_offsets: list[float],
) -> str:
    """For the escalation (thinking-enabled) pass: include fast-pass answer as
    prior, ask the model to verify/refine it."""
    frame_lines = "\n".join(
        f"[+{off:.2f}s] <image {i+1}>" for i, off in enumerate(frame_offsets)
    )
    return (
        f"ESCALATION window {window_id} | stream time {t_start:.2f}s – {t_end:.2f}s\n\n"
        f"The fast pass produced this analysis. Verify, refine, and add anything missed. "
        f"If the fast pass is wrong, say so explicitly. Use the same JSON schema.\n\n"
        f"## FAST PASS RESULT\n{fast_pass_summary}\n\n"
        f"## FRAMES (oldest first)\n{frame_lines}\n\n"
        f"Return ONLY the JSON object. No markdown. No explanation."
    )
