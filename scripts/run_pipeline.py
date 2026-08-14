"""Run the pipeline against a file and print a final event report.

Usage:
    python scripts/run_pipeline.py test_videos/test60.mp4 --duration 60

Useful for evaluating how well the pipeline detected known events in the test
video. Compares detected events vs. ground truth timestamps.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rtva.pipeline import Pipeline


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="path or URL")
    parser.add_argument("--duration", type=int, default=None,
                        help="stop after N seconds (wall)")
    args = parser.parse_args()

    emitted: list[dict] = []

    async def emit(msg: dict) -> None:
        if msg["type"] in ("event.confirmed", "event.updated", "event.provisional"):
            emitted.append({"t": time.time(), "msg": msg})

    p = Pipeline(source_url=args.source, emit=emit)
    try:
        if args.duration:
            await asyncio.wait_for(p.run(), timeout=args.duration)
        else:
            await p.run()
    except asyncio.TimeoutError:
        await p.stop()

    print("\n=== Event Report ===")
    print(f"total emitted: {len(emitted)}")
    confirmed = [e for e in emitted if not e["msg"]["event"]["provisional"]]
    provisional = [e for e in emitted if e["msg"]["event"]["provisional"]]
    print(f"  provisional (gate): {len(provisional)}")
    print(f"  confirmed (LLM):    {len(confirmed)}")
    print(f"\nWindows: dispatched={p.stats.windows_dispatched} "
          f"completed={p.stats.windows_completed} failed={p.stats.windows_failed} "
          f"parse_failures={p.stats.total_parse_failures}")

    print("\n=== Confirmed events (chronological) ===")
    for e in sorted(confirmed, key=lambda x: x["msg"]["event"]["t_start"]):
        ev = e["msg"]["event"]
        print(f"  t={ev['t_start']:6.2f}-{ev['t_end']:6.2f}  "
              f"{ev['type']:18s}  c={ev['confidence']:.2f}  {ev['description'][:80]}")

    print(f"\nLatency p50={p.stats.latency_p50_ms:.0f}ms "
          f"p95={p.stats.latency_p95_ms:.0f}ms max={p.stats.latency_max_ms:.0f}ms")
    print(f"Cache: hits={p.stats.cache_hits} misses={p.stats.cache_misses}")
    print(f"Tokens: prompt={p.stats.total_prompt_tokens} "
          f"completion={p.stats.total_completion_tokens}")


if __name__ == "__main__":
    asyncio.run(main())
