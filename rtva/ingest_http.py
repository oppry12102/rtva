"""HTTP single-frame POST ingest source.

Each `POST /v1/streams/{sid}/ingest/frame` request sends one JPEG frame and an
associated `pts_ms` form field. The ingestor decodes and enqueues it. The
pipeline runs as long as frames keep arriving; when no frame has been posted
for `idle_timeout_s`, the session can be auto-stopped (handled at the router
level, not here).
"""

from __future__ import annotations

import asyncio
import time
from io import BytesIO
from typing import AsyncIterator

import numpy as np
from PIL import Image

from .frame_source import FrameSource
from .source import VideoFrame


_SENTINEL = object()


class HttpPostIngestor(FrameSource):
    """A FrameSource fed one frame at a time by HTTP POSTs."""

    def __init__(self, max_queue: int = 32) -> None:
        self._q: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._stop = asyncio.Event()
        self._frames_in: int = 0
        self._last_t: float = time.monotonic()

    async def feed(self, pts_ms: int, jpeg: bytes) -> None:
        """Decode one JPEG and enqueue."""
        if self._stop.is_set():
            return
        rgb = await asyncio.to_thread(self._decode_jpeg, jpeg)
        item = VideoFrame(rgb=rgb, pts=pts_ms / 1000.0,
                          wall=asyncio.get_event_loop().time())
        self._frames_in += 1
        self._last_t = time.monotonic()
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

    @staticmethod
    def _decode_jpeg(jpeg: bytes) -> np.ndarray:
        img = Image.open(BytesIO(jpeg)).convert("RGB")
        return np.asarray(img, dtype=np.uint8)

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
    def idle_seconds(self) -> float:
        return time.monotonic() - self._last_t