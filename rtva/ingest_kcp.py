"""KCP ingest source — same shape as WSIngestor but for UDP/KCP-delivered frames.

Decoded RGB frames are fed in by `KcpPeer._on_frame`; the ingestor owns the
outbound channel for analysis events back to the same client.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator, Optional

import numpy as np

from .frame_source import FrameSource
from .source import VideoFrame


_SENTINEL = object()


class KcpIngestor(FrameSource):
    """A FrameSource fed by the KCP server.

    `feed()` accepts already-decoded RGB arrays (the KCP server does JPEG
    decode on the way in to avoid serializing a VideoFrame across the wire).

    `bind_peer()` / `unbind_peer()` set the outbound channel for analysis
    events; `send_event()` pushes them through.
    """

    def __init__(self, max_queue: int = 32) -> None:
        self._q: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._stop = asyncio.Event()
        self._peer: Any = None  # KcpPeer; set via bind_peer
        self._frames_in = 0

    # ----- ingest -----

    async def feed(self, *, pts_ms: int, rgb: np.ndarray) -> None:
        if self._stop.is_set():
            return
        item = VideoFrame(rgb=rgb, pts=pts_ms / 1000.0,
                          wall=asyncio.get_event_loop().time())
        self._frames_in += 1
        try:
            self._q.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self._q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            await self._q.put(item)

    async def close(self) -> None:
        self._stop.set()
        await self._q.put(_SENTINEL)

    # ----- outbound (analysis events) -----

    def bind_peer(self, peer) -> None:
        self._peer = peer

    def unbind_peer(self) -> None:
        self._peer = None

    def send_event(self, msg: dict) -> None:
        if self._peer is None:
            return
        try:
            self._peer.send(msg)  # KcpPeer.send takes (header, payload)
        except Exception:
            pass

    # ----- FrameSource -----

    async def frames(self) -> AsyncIterator[VideoFrame]:
        while True:
            item = await self._q.get()
            if item is _SENTINEL:
                return
            yield item

    async def aclose(self) -> None:
        await self.close()

    @property
    def frames_received(self) -> int:
        return self._frames_in

    @property
    def peer_bound(self) -> bool:
        return self._peer is not None