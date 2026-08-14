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

SYSTEM_PROMPT = """You are ZXUN, a real-time video analyst. You receive short sliding windows of frames from a live stream (sampled at ~8fps within each window). Your output is appended to a rolling log viewed by humans.

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
