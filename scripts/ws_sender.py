"""End-to-end test: read a video file, send JPEG frames over WS /v1/streams/{id}/ingest,
and print events received on the parallel observe WS.

Usage:
    python scripts/ws_sender.py --token <rtva_...> --stream <sid> --video test_videos/test60.mp4
    python scripts/ws_sender.py --token <rtva_...>  # create-stream + run
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import struct
import sys
import time
from pathlib import Path

import numpy as np
import websockets
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def create_stream(server: str, token: str, channel: str) -> dict:
    """POST /v1/streams to mint a session; return the response body."""
    import httpx
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"http://{server}/v1/streams",
            headers={"Authorization": f"Bearer {token}"},
            json={"source": "external", "channel": channel},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()


def jpeg_from_ndarray(rgb: np.ndarray, w: int, h: int) -> bytes:
    img = Image.fromarray(rgb).resize((w, h))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


async def push_frames(ingest_uri: str, frames_iter, dur_s: float,
                     fps: int = 10) -> int:
    async with websockets.connect(ingest_uri, max_size=8 * 1024 * 1024) as ws:
        period = 1.0 / fps
        count = 0
        end = asyncio.get_event_loop().time() + dur_s
        for pts_ms, rgb in frames_iter:
            if asyncio.get_event_loop().time() >= end:
                break
            jpeg = jpeg_from_ndarray(rgb, 320, 180)
            msg = struct.pack(">I", pts_ms) + jpeg
            await ws.send(msg)
            count += 1
            await asyncio.sleep(period)
        # graceful close — only after we've sent the duration
        try:
            await ws.send(json.dumps({"type": "close"}))
        except Exception:
            pass
        return count


async def observe(observe_uri: str, dur_s: float) -> list[dict]:
    events: list[dict] = []
    async with websockets.connect(observe_uri) as ws:
        end = asyncio.get_event_loop().time() + dur_s
        while asyncio.get_event_loop().time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=end - asyncio.get_event_loop().time())
                d = json.loads(raw)
                events.append(d)
            except asyncio.TimeoutError:
                break
    return events


def frames_from_video(path: str, max_frames: int | None = None, fps: int = 25):
    """Yield (pts_ms, rgb) from a video file using PyAV."""
    import av
    container = av.open(path)
    stream = container.streams.video[0]
    stream.thread_type = "AUTO"
    base_pts = None
    n = 0
    for raw in container.demux(stream):
        if max_frames is not None and n >= max_frames:
            break
        if raw.size == 0:
            continue
        for f in raw.decode():
            if f is None:
                continue
            pts = float(f.pts * f.time_base) if f.pts is not None and f.time_base else None
            if pts is None:
                continue
            if base_pts is None:
                base_pts = pts
            rel_ms = int((pts - base_pts) * 1000)
            rgb = f.to_ndarray(format="rgb24")
            yield rel_ms, rgb
            n += 1


def synthetic_frames(n: int = 30, fps: int = 10):
    """Yield (pts_ms, rgb) for a synthetic test pattern when no video file is given."""
    import math
    period_ms = 1000 // fps
    for i in range(n):
        # bright moving square
        rgb = np.zeros((180, 320, 3), dtype=np.uint8)
        x = int(20 + (i * 8) % 280)
        rgb[:, x:x + 40, :] = (i * 8) % 255
        rgb[60:120, x:x + 40, 0] = 250
        yield i * period_ms, rgb


async def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--server", default="127.0.0.1:8095")
    p.add_argument("--token", required=True)
    p.add_argument("--stream", default=None, help="existing sid; if omitted, create one")
    p.add_argument("--channel", default="ws", choices=["ws", "http"])
    p.add_argument("--video", default=None)
    p.add_argument("--max-frames", type=int, default=80)
    p.add_argument("--synthetic", action="store_true")
    args = p.parse_args()

    if args.stream:
        sid = args.stream
        # We need ingest/observe URLs; construct them
        ingest_url = f"ws://{args.server}/v1/streams/{sid}/ingest?token={args.token}"
        observe_url = f"ws://{args.server}/v1/streams/{sid}/observe?token={args.token}"
    else:
        info = await create_stream(args.server, args.token, args.channel)
        sid = info["session_id"]
        ingest_url = "ws://" + args.server + info["ingest"]["url"]
        observe_url = "ws://" + args.server + info["observe"]["url"]
        print(f"[+] created sid={sid}", file=sys.stderr)
        print(f"[+] ingest]  {ingest_url}", file=sys.stderr)
        print(f"[+] observe] {observe_url}", file=sys.stderr)

    if args.synthetic or not args.video:
        frames_iter = synthetic_frames(n=args.max_frames * 4)
    else:
        frames_iter = frames_from_video(args.video, max_frames=args.max_frames * 4)

    dur_s = max(15.0, args.max_frames * 0.1)
    sender_task = asyncio.create_task(push_frames(ingest_url, frames_iter, dur_s=dur_s))
    events = await observe(observe_url, dur_s=dur_s + 3.0)
    n_sent = await sender_task
    print(f"[+] sent {n_sent} frames over {dur_s:.1f}s", file=sys.stderr)

    # tally event types
    from collections import Counter
    types = Counter(e.get("type") for e in events)
    print(f"[+] observed {len(events)} messages, types={dict(types)}", file=sys.stderr)

    # pretty-print selected events
    for e in events:
        if e.get("type", "").startswith("event.") and not e.get("event", {}).get("provisional"):
            desc = e["event"].get("description", "")[:90]
            print(f"  [{e['type']}] {e['event'].get('type', '?'):18s} c={e['event'].get('confidence', 0):.2f}  {desc}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))