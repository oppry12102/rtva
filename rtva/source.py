"""Video source — decode frames from file / RTSP / RTMP via PyAV.

Produces (frame_rgb: uint8 ndarray, pts_seconds: float) tuples, paced by the
stream's PTS so timestamps reflect real video time (not wallclock).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import av
import numpy as np


@dataclass
class VideoFrame:
    rgb: np.ndarray              # (H, W, 3) uint8
    pts: float                   # stream-relative seconds (authoritative)
    wall: float                  # wallclock when yielded (for stats)


def _open(url: str) -> av.container.input.InputContainer:
    return av.open(url, options={"rtsp_transport": "tcp"})


async def stream_frames(
    url: str,
    *,
    target_fps: int = 8,
    max_queue: int = 32,
    stop_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[VideoFrame]:
    """Decode and yield frames, downsampled to ~target_fps, paced by PTS.

    Pacing strategy:
      - Decode every frame from PyAV (cheap).
      - Sleep so wall-clock catch-up matches the source's PTS rate, capped at
        target_fps (don't burn cycles on a 60fps source).
      - This way `pts` always represents real video time.
    """
    q: asyncio.Queue[Optional[VideoFrame]] = asyncio.Queue(maxsize=max_queue)
    stop = stop_event or asyncio.Event()

    async def producer() -> None:
        try:
            container = await asyncio.to_thread(_open, url)
            stream = container.streams.video[0]
            stream.thread_type = "AUTO"
            start_wall = time.monotonic()
            start_pts: Optional[float] = None
            period = 1.0 / max(1, target_fps)
            last_yield_wall = start_wall

            for raw in container.demux(stream):
                if stop.is_set():
                    break
                if raw.size == 0:
                    continue
                for f in raw.decode():
                    if f is None:
                        continue
                    pts = float(f.pts * f.time_base) if f.pts is not None and f.time_base else None
                    if pts is None:
                        continue
                    if start_pts is None:
                        start_pts = pts
                    rel_pts = pts - start_pts
                    rgb = f.to_ndarray(format="rgb24")
                    # sleep so wall elapsed >= rel_pts (real-time pacing)
                    target_wall = start_wall + rel_pts
                    now = time.monotonic()
                    wait = target_wall - now
                    if wait > 0:
                        await asyncio.sleep(wait)
                    # rate-cap to target_fps minimum gap
                    now = time.monotonic()
                    min_gap = period - (now - last_yield_wall)
                    if min_gap > 0:
                        await asyncio.sleep(min_gap)
                        now = time.monotonic()
                    last_yield_wall = now
                    item = VideoFrame(rgb=rgb, pts=rel_pts, wall=now)
                    try:
                        q.put_nowait(item)
                    except asyncio.QueueFull:
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        await q.put(item)
            container.close()
        except Exception as exc:
            await q.put(None)
            print(f"[source] producer exited: {exc!r}")
        finally:
            await q.put(None)

    prod_task = asyncio.create_task(producer())
    try:
        while True:
            item = await q.get()
            if item is None:
                break
            yield item
    finally:
        stop.set()
        prod_task.cancel()
        try:
            await prod_task
        except (asyncio.CancelledError, Exception):
            pass
