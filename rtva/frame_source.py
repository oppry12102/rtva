"""FrameSource — uniform async iterable of VideoFrame for any ingest channel.

The pipeline consumes frames via `async for vf in source.frames(): ...`. Each
call to `frames()` starts a fresh producer; `aclose()` signals stop and frees
resources.

Implementations:
    PyAVSource           file / RTSP / RTMP / HTTP URL via PyAV (existing path)
    WSIngestor           binary WebSocket frames from external apps  (T3)
    HttpPostIngestor     single-frame HTTP POST fallback             (T3)
    KcpIngestor          KCP/UDP frames from Android apps            (T5/T6)
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import AsyncIterator

from .source import VideoFrame, stream_frames


class FrameSource(ABC):
    """A producer of (rgb, pts) video frames."""

    @abstractmethod
    def frames(self) -> AsyncIterator[VideoFrame]:
        """Return an async iterator over frames. Each call yields a fresh producer."""
        raise NotImplementedError
        yield  # noqa: unreachable — makes the method recognizably a generator for type checkers

    async def aclose(self) -> None:
        """Signal stop and release resources. Default: no-op."""
        return None


class PyAVSource(FrameSource):
    """Wraps `rtva.source.stream_frames` for file / URL / RTSP / RTMP inputs."""

    def __init__(self, url: str, *, target_fps: int = 8, max_queue: int = 32) -> None:
        self.url = url
        self.target_fps = target_fps
        self.max_queue = max_queue
        self._stop = asyncio.Event()

    async def frames(self) -> AsyncIterator[VideoFrame]:
        async for vf in stream_frames(
            self.url,
            target_fps=self.target_fps,
            max_queue=self.max_queue,
            stop_event=self._stop,
        ):
            yield vf

    async def aclose(self) -> None:
        self._stop.set()


# ----------------------------------------------------------------------------
# Factory: legacy `source_url: str` callers get a PyAVSource.
# ----------------------------------------------------------------------------


def source_from_url(url: str, *, target_fps: int = 8) -> PyAVSource:
    return PyAVSource(url, target_fps=target_fps)