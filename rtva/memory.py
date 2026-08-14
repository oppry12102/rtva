"""StreamingMemory — bounded rolling context that travels with each LLM call.

Text-level analog of StreamingVLM's attention sink + sliding window KV cache:
the system prompt stays put (so it hits MiniMax prompt cache) while the user
message holds a hard-bounded window of recent narratives/events.

No LLM-based compaction in v1 — that would invalidate the cache. Hard truncation
keeps prompt length bounded forever.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from .events import Event, merge_events, should_merge


@dataclass
class StreamingMemory:
    max_recent_narratives: int = 5
    max_recent_events: int = 10

    recent_narratives: deque = field(
        default_factory=lambda: deque(maxlen=5)
    )
    recent_events: deque = field(
        default_factory=lambda: deque(maxlen=10)
    )
    pending_events: dict = field(default_factory=dict)
    scene_summary: str = ""

    def add_narrative(self, text: str) -> None:
        if text:
            self.recent_narratives.append(text)

    def add_event(self, ev: Event) -> None:
        self.recent_events.append(ev)

    def narrative_lines(self) -> list[str]:
        return list(self.recent_narratives)

    def event_lines(self) -> list[str]:
        return [
            f"[{ev.type}] {ev.description} [{ev.t_start:.1f}s-{ev.t_end:.1f}s]"
            for ev in self.recent_events
        ]

    def ingest_new_events(self, new_events: list[Event]) -> list[tuple[str, Event | None, Event | None]]:
        results: list[tuple[str, Event | None, Event | None]] = []
        for ev in new_events:
            if not ev.last_updated:
                ev.last_updated = time.time()
            match: Event | None = None
            for existing in self.pending_events.values():
                if should_merge(existing, ev):
                    if match is None or existing.t_end >= match.t_end:
                        match = existing
            if match is not None:
                merged = merge_events(match, ev)
                self.pending_events[merged.event_id] = merged
                results.append(("merged", match, merged))
            else:
                self.pending_events[ev.event_id] = ev
                results.append(("inserted", None, ev))
        return results

    def finalize_old(self, now_stream_t: float, grace_s: float = 2.0) -> list[Event]:
        finalized: list[Event] = []
        for ev_id, ev in list(self.pending_events.items()):
            if ev.t_end + grace_s < now_stream_t:
                finalized.append(ev)
                del self.pending_events[ev_id]
        return finalized

    def replace_event(self, ev: Event) -> None:
        self.pending_events[ev.event_id] = ev
