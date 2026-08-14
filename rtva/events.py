"""Event model + cross-window deduplication / merging."""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


# --- Event --------------------------------------------------------------------


@dataclass
class Event:
    event_id: str
    type: str                     # action / scene_change / object_appear / object_disappear / anomaly / transition
    t_start: float                # seconds, stream-relative
    t_end: float                  # seconds, stream-relative
    description: str
    confidence: float
    actors: list[str] = field(default_factory=list)
    objects: list[str] = field(default_factory=list)
    location: str = ""
    key_entities: list[str] = field(default_factory=list)
    is_continuation: bool = False
    source_windows: list[int] = field(default_factory=list)
    first_seen: float = 0.0
    last_updated: float = 0.0
    corroboration: int = 1
    provisional: bool = False     # True if this came from the CPU gate (not yet LLM-confirmed)

    @classmethod
    def provisional_from_gate(cls, t: float, reason: str | None = None,
                              *, language: str = "zh") -> "Event":
        # Default placeholder text, localised by language. The LLM normally
        # overwrites this with a concrete description on the next pass;
        # for streams with no fast-pass follow-up, the placeholder must still
        # be in the narration language so Chinese streams don't leak English.
        if reason is None:
            reason = "显著变化" if language == "zh" else "significant change"
        now = time.time()
        return cls(
            event_id=str(uuid.uuid4()),
            type="transition",
            t_start=t,
            t_end=t,
            description=reason,
            confidence=0.0,
            source_windows=[],
            first_seen=t,
            last_updated=now,
            provisional=True,
        )

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "type": self.type,
            "t_start": self.t_start,
            "t_end": self.t_end,
            "description": self.description,
            "confidence": self.confidence,
            "actors": self.actors,
            "objects": self.objects,
            "location": self.location,
            "key_entities": self.key_entities,
            "is_continuation": self.is_continuation,
            "source_windows": self.source_windows,
            "first_seen": self.first_seen,
            "last_updated": self.last_updated,
            "corroboration": self.corroboration,
            "provisional": self.provisional,
        }


# --- Dedup --------------------------------------------------------------------


_MERGEABLE_TYPES = {"action", "anomaly", "transition"}


def _trigrams(s: str) -> set[str]:
    s = re.sub(r"\s+", " ", s.lower()).strip()
    return {s[i:i+3] for i in range(len(s) - 2)} if len(s) >= 3 else {s}


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _event_text_set(ev: Event) -> set[str]:
    return _trigrams(ev.description) | {w.lower() for w in ev.description.split()}


def _event_entity_set(ev: Event) -> set[str]:
    s: set[str] = set()
    for w in ev.key_entities + ev.actors + ev.objects:
        s.add(w.lower().strip())
    return s


def should_merge(a: Event, b: Event) -> bool:
    """Returns True if `b` looks like a continuation of `a`."""
    if a.type != b.type or a.type not in _MERGEABLE_TYPES:
        return False
    # temporal overlap with +/- 0.5s jitter
    if b.t_start > a.t_end + 0.5 or b.t_end < a.t_start - 0.5:
        return False
    desc_sim = _jaccard(_event_text_set(a), _event_text_set(b))
    ent_sim = _jaccard(_event_entity_set(a), _event_entity_set(b))
    score = 0.5 * desc_sim + 0.5 * ent_sim
    return score > 0.35


def merge_events(a: Event, b: Event) -> Event:
    """Return the merged event (a with b's contribution)."""
    a.t_start = min(a.t_start, b.t_start)
    a.t_end = max(a.t_end, b.t_end)
    if len(b.description) > len(a.description):
        a.description = b.description
    a.confidence = max(a.confidence, b.confidence)
    a.actors = sorted(set(a.actors) | set(b.actors))
    a.objects = sorted(set(a.objects) | set(b.objects))
    a.key_entities = sorted(set(a.key_entities) | set(b.key_entities))
    if b.location and not a.location:
        a.location = b.location
    a.source_windows = sorted(set(a.source_windows) | set(b.source_windows))
    a.corroboration += 1
    a.last_updated = time.time()
    return a
