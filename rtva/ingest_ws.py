"""WebSocket binary ingest source.

External apps connect to `WS /v1/streams/{sid}/ingest?token=...` and send binary
messages of the form `[u32 BE pts_ms][jpeg bytes]`. The ingestor decodes each
JPEG to an RGB ndarray and yields it into the pipeline at `pts_ms / 1000` seconds.

Text frames may be used for control (`{"type":"ping"}`, `{"type":"close"}`).
"""

from __future__ import annotations

import asyncio
import struct
import time
from io import BytesIO
from typing import AsyncIterator

import numpy as np
from PIL import Image

from .frame_source import FrameSource
from .source import VideoFrame


_SENTINEL = object()


class WSIngestor(FrameSource):
    """A FrameSource backed by a WebSocket connection.

    The FastAPI handler drives `feed()` for each binary frame and `close()` on
    disconnect. Frames are decoded JPEG → RGB ndarray in a thread (PIL is sync).
    """

    HEADER = struct.Struct(">I")  # u32 BE pts_ms

    def __init__(self, max_queue: int = 32) -> None:
        self._q: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._stop = asyncio.Event()
        self._peer: tuple[str, int] | None = None
        self._frames_in: int = 0
        self._last_t: float = time.monotonic()

    # ----- driver API (called by the WS endpoint) -----

    async def feed(self, data: bytes, *, peer: tuple[str, int] | None = None) -> None:
        """Decode one binary message and enqueue a frame."""
        if self._stop.is_set():
            return
        if peer is not None:
            self._peer = peer
        if len(data) < self.HEADER.size:
            return
        pts_ms = self.HEADER.unpack_from(data, 0)[0]
        jpeg = data[self.HEADER.size:]
        rgb = await asyncio.to_thread(self._decode_jpeg, jpeg)
        item = VideoFrame(rgb=rgb, pts=pts_ms / 1000.0, wall=asyncio.get_event_loop().time())
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

    # ----- FrameSource -----

    async def frames(self) -> AsyncIterator[VideoFrame]:
        while True:
            item = await self._q.get()
            if item is _SENTINEL:
                return
            yield item

    async def aclose(self) -> None:
        await self.close()

    # ----- helpers -----

    @property
    def peer(self) -> tuple[str, int] | None:
        return self._peer

    @property
    def frames_received(self) -> int:
        return self._frames_in

    @property
    def idle_seconds(self) -> float:
        """Seconds since the last successfully decoded frame arrived."""
        return time.monotonic() - self._last_t