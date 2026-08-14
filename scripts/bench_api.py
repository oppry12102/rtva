"""Re-run the latency / throughput benchmark against the live MiniMax-M3 API.

Documents the numbers that drive the design choices in the README.

Usage:
    MINIMAX_API_KEY=sk-... python scripts/bench_api.py
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import statistics
import time
import urllib.request
from collections import Counter

from PIL import Image, ImageDraw


K = "PLACEHOLDER"  # user must set MINIMAX_API_KEY in env
EP = "https://api.minimaxi.com/v1/text/chatcompletion_v2"


def _frame(i: int, seed: int = 0) -> str:
    im = Image.new("RGB", (448, 252), (20 + seed % 40, 80, 25))
    d = ImageDraw.Draw(im)
    d.rectangle([30 + i * 40, 150, 60 + i * 40, 200], fill=(200, 50, 50))
    d.text((8, 8), f"F{i}-{seed}", fill=(255, 255, 255))
    b = io.BytesIO()
    im.save(b, "JPEG", quality=72)
    return base64.b64encode(b.getvalue()).decode()


def _post(payload: dict, timeout: int = 120) -> tuple[dict, float]:
    req = urllib.request.Request(
        EP,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {K}", "Content-Type": "application/json"},
    )
    t0 = time.time()
    r = json.load(urllib.request.urlopen(req, timeout=timeout))
    return r, time.time() - t0


def main() -> None:
    global K
    import os
    K = os.environ["MINIMAX_API_KEY"]
    import random

    def payload(n: int, mt: int, seed: int) -> dict:
        content = [{"type": "text", "text":
                    f'{n} frames 0.5s apart. ONLY JSON: {{"summary":"<=15w","events":[]}}'}]
        for i in range(n):
            content.append({"type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{_frame(i, seed)}"}})
        return {"model": "MiniMax-M3", "max_tokens": mt,
                "thinking": {"type": "disabled"}, "temperature": 0.2,
                "messages": [{"role": "user", "content": content}]}

    print("=== Single-call latency vs. frame count (uncached) ===")
    for n in [1, 4, 8, 16, 24]:
        latencies = []
        for k in range(3):
            seed = random.randint(1, 99999)
            r, dt = _post(payload(n, 150, seed))
            u = r.get("usage", {})
            latencies.append((dt, u.get("prompt_tokens"), u.get("completion_tokens")))
        dts = [x[0] for x in latencies]
        print(f"  n={n:>2}: min={min(dts):.2f}s  median={statistics.median(dts):.2f}s  "
              f"max={max(dts):.2f}s  ptok~{latencies[-1][1]}")

    print("\n=== Concurrency (8 parallel, 8 frames each) ===")
    import threading
    results: list[float] = []
    errs: list = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        try:
            _, dt = _post(payload(8, 60, random.randint(1, 99999)))
            with lock: results.append(dt)
        except Exception as e:
            with lock: errs.append(e)

    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    wall = time.time() - t0
    print(f"  wall={wall:.2f}s  ok={len(results)}/8  throughput={len(results)/wall:.2f} req/s")
    if results:
        print(f"  per-req median={statistics.median(results):.2f}s  "
              f"max={max(results):.2f}s")
    if errs:
        print(f"  errors: {Counter(type(e).__name__ for e in errs)}")

    print("\n=== Cache effect (identical content twice) ===")
    seed = random.randint(1, 99999)
    r1, dt1 = _post(payload(8, 150, seed))
    r2, dt2 = _post(payload(8, 150, seed))
    c1 = r1.get("usage", {}).get("prompt_tokens_details", {}).get("cached_tokens", 0)
    c2 = r2.get("usage", {}).get("prompt_tokens_details", {}).get("cached_tokens", 0)
    print(f"  call1: {dt1:.2f}s  cached={c1}")
    print(f"  call2: {dt2:.2f}s  cached={c2}  speedup={(dt1/dt2):.1f}x")


if __name__ == "__main__":
    main()
