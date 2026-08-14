"""Reference Python KCP client for the RTVA external-app channel.

Connects to udp://server:8096 with KCP, sends a hello (token + session_id),
then forwards JPEG frames from a video file (or synthetic pattern) at a target
fps. Prints analysis events received back.

This is a reference implementation showing the wire protocol. The Android app
side would use l42111996/kcp (Java/Netty) speaking the same protocol.

    python examples/kcp_sender.py --token <rtva_...> --stream <sid>
    python examples/kcp_sender.py --token <rtva_...> --stream <sid> --video test_videos/test60.mp4
"""

from __future__ import annotations

import argparse
import io
import json
import struct
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def encode_msg(header: dict, payload: bytes = b"") -> bytes:
    """Mirror of rtva.kcp_server.encode_message."""
    if payload:
        header = {**header, "payload_len": len(payload)}
    body = json.dumps(header).encode()
    out = struct.pack(">I", len(body)) + body
    if payload:
        out += struct.pack(">I", len(payload)) + payload
    return out


def jpeg_from_ndarray(rgb: np.ndarray, w: int = 320, h: int = 180) -> bytes:
    img = Image.fromarray(rgb).resize((w, h))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def synthetic_frames(n: int = 100, fps: int = 10):
    period_ms = 1000 // fps
    for i in range(n):
        rgb = np.zeros((180, 320, 3), dtype=np.uint8)
        x = int(20 + (i * 8) % 280)
        rgb[60:120, x:x + 40, 0] = 250
        rgb[80:100, x:x + 40, :] = (i * 8) % 255
        yield i * period_ms, rgb


def video_frames(path: str, max_frames: int):
    import av
    container = av.open(path)
    stream = container.streams.video[0]
    stream.thread_type = "AUTO"
    base = None
    n = 0
    for raw in container.demux(stream):
        if max_frames and n >= max_frames:
            break
        if raw.size == 0:
            continue
        for f in raw.decode():
            if f is None:
                continue
            pts = float(f.pts * f.time_base) if f.pts is not None else None
            if pts is None:
                continue
            if base is None:
                base = pts
            yield int((pts - base) * 1000), f.to_ndarray(format="rgb24")
            n += 1
    container.close()


def main() -> int:
    import httpx
    import kcp  # type: ignore

    p = argparse.ArgumentParser()
    p.add_argument("--server", default="127.0.0.1:8095",
                   help="HTTP server for /v1/streams (host:port)")
    p.add_argument("--kcp-host", default=None,
                   help="KCP server host (default: same as HTTP host)")
    p.add_argument("--kcp-port", type=int, default=8096)
    p.add_argument("--token", required=True)
    p.add_argument("--stream", default=None)
    p.add_argument("--video", default=None)
    p.add_argument("--max-frames", type=int, default=120)
    p.add_argument("--fps", type=int, default=10)
    p.add_argument("--synthetic", action="store_true")
    args = p.parse_args()

    # Create stream if not given
    if not args.stream:
        r = httpx.post(f"http://{args.server}/v1/streams",
                       headers={"Authorization": f"Bearer {args.token}"},
                       json={"source": "external", "channel": "kcp"})
        r.raise_for_status()
        info = r.json()
        sid = info["session_id"]
        print(f"[+] created sid={sid}", file=sys.stderr)
    else:
        sid = args.stream

    kcp_host = args.kcp_host or args.server.split(":")[0]
    kcp_port = args.kcp_port

    # Build KCP client (sync; runs update in foreground thread)
    client = kcp.KCPClientSync(
        address=kcp_host,
        port=kcp_port,
        conv_id=1,
        no_delay=True,
        update_interval=10,
        resend_count=2,
        no_congestion_control=False,
        receive_window_size=128,
        send_window_size=128,
    )

    received: list[dict] = []
    hello_done = threading.Event()
    stop = threading.Event()

    def on_data(data: bytes) -> None:
        # parse framed messages
        msgs = []
        buf = data
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if len(buf) < 4 + ln:
                break
            msg = buf[4:4 + ln]
            buf = buf[4 + ln:]
            try:
                hdr = json.loads(msg)
            except json.JSONDecodeError:
                continue
            payload_len = int(hdr.get("payload_len", 0))
            if payload_len:
                if len(buf) < 4 + payload_len:
                    break
                hdr["_payload_size"] = struct.unpack(">I", buf[:4])[0]
                buf = buf[4 + payload_len:]
            msgs.append(hdr)
        for hdr in msgs:
            received.append(hdr)
            if hdr.get("type") == "hello_ok":
                hello_done.set()
            if hdr.get("type") == "bye":
                stop.set()

    client.on_data(on_data)

    # Start receiver in a background thread
    rcv = threading.Thread(target=client.receive_loop, daemon=True)
    rcv.start()

    updater = threading.Thread(target=client.update_loop, daemon=True)
    updater.start()

    # Send hello
    print("[+] sending hello…", file=sys.stderr)
    client.send(encode_msg({"type": "hello", "token": args.token, "session_id": sid}))

    if not hello_done.wait(timeout=5.0):
        print("[!] no hello_ok within 5s — server may not be reachable on UDP", file=sys.stderr)
        sys.exit(2)

    print("[+] hello OK — streaming frames", file=sys.stderr)

    # Stream frames
    if args.synthetic or not args.video:
        frames = synthetic_frames(n=args.max_frames, fps=args.fps)
    else:
        frames = video_frames(args.video, args.max_frames)

    period = 1.0 / args.fps
    n_sent = 0
    for pts_ms, rgb in frames:
        if stop.is_set():
            break
        jpeg = jpeg_from_ndarray(rgb)
        client.send(encode_msg({"type": "frame", "pts_ms": pts_ms,
                                "w": rgb.shape[1], "h": rgb.shape[0],
                                "codec": "jpeg"}, jpeg))
        n_sent += 1
        time.sleep(period)

    # Graceful close
    client.send(encode_msg({"type": "close", "reason": "end"}))
    time.sleep(2.0)

    types = Counter(m.get("type") for m in received)
    print(f"\n[+] sent {n_sent} frames; received {len(received)} messages, types={dict(types)}",
          file=sys.stderr)
    for m in received:
        mtype = m.get("type", "")
        if mtype in ("hello_ok", "stats", "pong"):
            continue
        print(f"  [{mtype}] {json.dumps({k: v for k, v in m.items() if k != 'event'})[:120]}")
        if mtype.startswith("event.") and not (m.get("event") or {}).get("provisional"):
            e = m["event"]
            desc = (e.get("description", "") or "")[:80]
            print(f"        {e.get('type', '?'):18s} c={e.get('confidence', 0):.2f}  {desc}")

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())