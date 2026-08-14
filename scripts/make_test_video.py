"""Generate a synthetic test video with KNOWN ground-truth events.

No camera, no ffmpeg needed — uses PyAV (bundled). Produces ~60s of footage
with three event classes for evaluation:

    - object_appear:  red ball pops into frame at t=10s
    - action:         ball rolls across frame t=15-25s
    - scene_change:   frame 1 -> frame 2 cut at t=40s
    - object_disappear: ball exits at t=35s

Run:
    python scripts/make_test_video.py /tmp/test.mp4
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import av
import numpy as np


def _draw_static_scene(t: float) -> np.ndarray:
    """A neutral indoor scene: light wall, dark floor band, faint timestamp."""
    H, W = 360, 640
    img = np.full((H, W, 3), (220, 220, 230), dtype=np.uint8)
    img[280:, :] = (60, 60, 70)
    msg = f"scene A t={t:5.1f}s"
    for i, ch in enumerate(msg):
        x0 = 10 + i * 7
        y0 = 10
        glyph = _GLYPH.get(ch, _GLYPH[" "])
        for ry, row in enumerate(glyph):
            for rx, bit in enumerate(row):
                if bit:
                    y, x = y0 + ry, x0 + rx
                    if 0 <= y < H and 0 <= x < W:
                        img[y, x] = (20, 20, 20)
    return img


def _draw_ball_scene(t: float) -> np.ndarray:
    """High-contrast scene: bright orange wall, a red ball rolls horizontally."""
    H, W = 360, 640
    img = np.full((H, W, 3), (250, 140, 30), dtype=np.uint8)   # orange wall
    img[280:, :] = (40, 80, 40)                                # dark green floor
    if 15 <= t <= 25:
        x = int(50 + (t - 15) * 54)
    elif t < 15:
        x = None
    else:
        x = None
    msg = f"scene B t={t:5.1f}s"
    for i, ch in enumerate(msg):
        x0 = 10 + i * 7
        y0 = 10
        glyph = _GLYPH.get(ch, _GLYPH[" "])
        for ry, row in enumerate(glyph):
            for rx, bit in enumerate(row):
                if bit:
                    y, x_ = y0 + ry, x0 + rx
                    if 0 <= y < H and 0 <= x_ < W:
                        img[y, x_] = (20, 20, 20)
    if x is not None:
        cy, r = 180, 22
        yy, xx = np.ogrid[:H, :W]
        mask = (xx - x) ** 2 + (yy - cy) ** 2 <= r * r
        img[mask] = (255, 255, 255)  # bright white ball = maximum contrast on orange
    return img


_SCENES = {0.0: _draw_static_scene, 40.0: _draw_ball_scene}


def _scene_for(t: float):
    # pick latest scene start <= t
    starts = sorted(_SCENES.keys())
    chosen = 0.0
    for s in starts:
        if t >= s:
            chosen = s
    return _SCENES[chosen]


# tiny 5x7 ASCII glyphs (digits + a few letters)
_GLYPH = {
    " ": [[0]*5]*7,
    "0": [[0,1,1,1,0],[1,0,0,0,1],[1,0,0,1,1],[1,0,1,0,1],[1,1,0,0,1],[1,0,0,0,1],[0,1,1,1,0]],
    "1": [[0,0,1,0,0],[0,1,1,0,0],[0,0,1,0,0],[0,0,1,0,0],[0,0,1,0,0],[0,0,1,0,0],[0,1,1,1,0]],
    "2": [[0,1,1,1,0],[1,0,0,0,1],[0,0,0,0,1],[0,0,0,1,0],[0,0,1,0,0],[0,1,0,0,0],[1,1,1,1,1]],
    "3": [[1,1,1,1,0],[0,0,0,0,1],[0,0,0,0,1],[0,1,1,1,0],[0,0,0,0,1],[0,0,0,0,1],[1,1,1,1,0]],
    "4": [[0,0,0,1,0],[0,0,1,1,0],[0,1,0,1,0],[1,0,0,1,0],[1,1,1,1,1],[0,0,0,1,0],[0,0,0,1,0]],
    "5": [[1,1,1,1,1],[1,0,0,0,0],[1,1,1,1,0],[0,0,0,0,1],[0,0,0,0,1],[1,0,0,0,1],[0,1,1,1,0]],
    "6": [[0,1,1,1,0],[1,0,0,0,0],[1,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[1,0,0,0,1],[0,1,1,1,0]],
    "7": [[1,1,1,1,1],[0,0,0,0,1],[0,0,0,1,0],[0,0,1,0,0],[0,1,0,0,0],[0,1,0,0,0],[0,1,0,0,0]],
    "8": [[0,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[0,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[0,1,1,1,0]],
    "9": [[0,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[0,1,1,1,1],[0,0,0,0,1],[0,0,0,0,1],[0,1,1,1,0]],
    ".": [[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,1,1,0,0],[0,1,1,0,0]],
    "s": [[0,1,1,1,1],[1,0,0,0,0],[0,1,1,1,0],[0,0,0,0,1],[0,0,0,0,1],[1,0,0,0,1],[0,1,1,1,0]],
    "c": [[0,1,1,1,1],[1,0,0,0,0],[1,0,0,0,0],[1,0,0,0,0],[1,0,0,0,0],[1,0,0,0,0],[0,1,1,1,1]],
    "e": [[0,1,1,1,0],[1,0,0,0,1],[1,1,1,1,1],[1,0,0,0,0],[1,0,0,0,0],[1,0,0,0,1],[0,1,1,1,0]],
    "n": [[1,0,0,0,1],[1,1,0,0,1],[1,0,1,0,1],[1,0,0,1,1],[1,0,0,0,1],[1,0,0,0,1],[1,0,0,0,1]],
    "A": [[0,0,1,0,0],[0,1,0,1,0],[1,0,0,0,1],[1,0,0,0,1],[1,1,1,1,1],[1,0,0,0,1],[1,0,0,0,1]],
    "B": [[1,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[1,1,1,1,0],[1,0,0,0,1],[1,0,0,0,1],[1,1,1,1,0]],
    "t": [[0,1,0,0,0],[1,1,1,1,0],[0,1,0,0,0],[0,1,0,0,0],[0,1,0,0,0],[0,1,0,0,1],[0,0,1,1,0]],
    "=": [[0,0,0,0,0],[1,1,1,1,1],[0,0,0,0,0],[1,1,1,1,1],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]],
}


def main() -> None:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "test.mp4")
    duration_s = float(sys.argv[2]) if len(sys.argv) > 2 else 60.0
    fps = int(sys.argv[3]) if len(sys.argv) > 3 else 25

    container = av.open(str(out), mode="w")
    stream = container.add_stream("h264", rate=fps)
    stream.width = 640
    stream.height = 360
    stream.pix_fmt = "yuv420p"
    stream.options = {"preset": "ultrafast"}

    total_frames = int(duration_s * fps)
    print(f"writing {total_frames} frames ({duration_s}s @ {fps}fps) to {out}")
    for i in range(total_frames):
        t = i / fps
        rgb = _scene_for(t)(t)
        frame = av.VideoFrame.from_ndarray(rgb, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()
    print(f"done: {out} ({out.stat().st_size // 1024} KB)")
    print("ground truth events:")
    print("  t=10.0s  object_appear   red ball enters scene B")
    print("  t=15-25  action          ball rolls right")
    print("  t=25.0s  object_disappear ball exits")
    print("  t=40.0s  scene_change    cut to scene B (which has ball again)")
    print("  t=40-50  action          ball rolls right (second pass)")


if __name__ == "__main__":
    main()
