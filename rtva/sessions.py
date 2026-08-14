"""Session registry shared between legacy /sessions routes and /v1/* routes.

Owns the `SessionRecord` dataclass, the `SessionManager` class, and the singleton
`manager` instance. Lives in its own module so `rtva.api_v1` and `rtva.server`
can both import it without circularity.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .frame_source import FrameSource
from .pipeline import Pipeline


@dataclass
class SessionRecord:
    pipeline: Pipeline
    observers: set = field(default_factory=set)
    replay_buf: deque = field(default_factory=lambda: deque(maxlen=200))
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    channel: str = "file"            # "file" | "ws" | "http" | "kcp"
    ingestor: object = None          # WSIngestor | HttpPostIngestor | KcpIngestor | None
    owner_token_label: str = ""      # label of the bearer that created it


class SessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionRecord] = {}
        self._lock = asyncio.Lock()

    # ----- create paths -----

    async def create(self, source: str, options: Optional[dict] = None) -> SessionRecord:
        return await self._create_with(source_url=source, options=options,
                                       channel="file", ingestor=None,
                                       owner_token_label="")

    async def create_with_ingestor(self, *, frame_source: FrameSource,
                                   options: Optional[dict], channel: str,
                                   ingestor, owner_token_label: str) -> SessionRecord:
        return await self._create_with(frame_source=frame_source, options=options,
                                       channel=channel, ingestor=ingestor,
                                       owner_token_label=owner_token_label)

    async def _create_with(self, *, source_url: str = "",
                           frame_source: FrameSource | None = None,
                           options: Optional[dict], channel: str,
                           ingestor, owner_token_label: str) -> SessionRecord:
        options = options or {}

        async def emit(msg: dict) -> None:
            record = self._sessions.get(msg.get("session_id", ""))
            if not record:
                return
            record.replay_buf.append(msg)
            text = json.dumps(msg, default=str)
            dead = []
            for ws in list(record.observers):
                try:
                    await ws.send_text(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                record.observers.discard(ws)
            # KCP outbound: ingestor may be a KcpIngestor with a bound peer
            if hasattr(record.ingestor, "send_event"):
                try:
                    record.ingestor.send_event(msg)
                except Exception:
                    pass

        pipeline = Pipeline(source_url=source_url, frame_source=frame_source,
                            emit=emit, options=options)
        record = SessionRecord(pipeline=pipeline, channel=channel,
                               ingestor=ingestor, owner_token_label=owner_token_label)
        async with self._lock:
            self._sessions[pipeline.session_id] = record
        asyncio.create_task(self._run(record))
        return record

    # ----- lifecycle -----

    async def _run(self, record: SessionRecord) -> None:
        try:
            await record.pipeline.run()
        except Exception as exc:
            print(f"[server] pipeline crashed: {exc!r}")
        finally:
            async with self._lock:
                self._sessions.pop(record.pipeline.session_id, None)

    async def stop(self, session_id: str) -> bool:
        async with self._lock:
            rec = self._sessions.get(session_id)
            if not rec:
                return False
            await rec.pipeline.stop()
            for ws in list(rec.observers):
                try:
                    await ws.close()
                except Exception:
                    pass
            self._sessions.pop(session_id, None)
            return True

    def get(self, session_id: str) -> Optional[SessionRecord]:
        return self._sessions.get(session_id)

    def all(self) -> list[SessionRecord]:
        return list(self._sessions.values())


# Module-level singleton, imported by both server and api_v1.
manager = SessionManager()