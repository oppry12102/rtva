"""CPU cognition gate — cheap per-frame motion + scene-cut detection.

Translates Mage-VL's "proactive streaming cognition gate" and VideoLLM-online's
EOS-trigger idea to a CPU-only setting.

For every frame:
    1. Downsample to 64x36 grayscale (<1ms).
    2. Update mean-abs-diff motion signal against previous frame.
    3. Update 256-bin histogram chi-square scene-cut signal.
    4. Adaptive threshold = EMA(motion) + k * std(motion); self-calibrates within
       ~20 frames to any scene's baseline, so the same parameters handle both
       static surveillance footage and high-motion sports.
    5. Fire (request an LLM analysis) when motion > threshold OR scene_cut,
       with a 1.5s refractory period to debounce.

Cost: <1ms/frame at 64x36 — leaves enormous headroom even for 25fps.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class GateSignal:
    t: float
    motion: float                 # 0..1, mean-abs-diff / 255
    threshold: float              # adaptive threshold at this moment
    chi: float                    # histogram chi-square distance vs previous frame
    scene_cut: bool
    fired: bool                   # should the LLM path be triggered?
    high_salience: bool           # escalate to thinking-enabled M3 call


class MotionGate:
    """Per-frame novelty detection. Thread-unsafe; one per session."""

    def __init__(
        self,
        ema_alpha: float = 0.05,
        k: float = 3.0,
        min_thresh: float = 0.015,
        scene_cut_thresh: float = 0.25,
        scene_cooldown_s: float = 1.5,
        high_salience_motion: float = 0.20,
        downsample_wh: tuple[int, int] = (64, 36),
    ) -> None:
        self.alpha = ema_alpha
        self.k = k
        self.min_thresh = min_thresh
        self.scene_cut_thresh = scene_cut_thresh
        self.scene_cooldown_s = scene_cooldown_s
        self.high_salience_motion = high_salience_motion
        self.downsample_wh = downsample_wh

        self._prev_gray: Optional[np.ndarray] = None
        self._prev_hist: Optional[np.ndarray] = None
        self._ema = 0.0
        self._ema_var = 0.0
        self._ema_initialized = False
        self._last_scene_cut_t = -1e9

    # --- public ---

    def update(self, frame_rgb: np.ndarray, t: Optional[float] = None) -> GateSignal:
        """`frame_rgb` is a (H, W, 3) uint8 ndarray. `t` is stream-time in seconds."""
        if t is None:
            t = time.monotonic()
        gray = self._downsample_gray(frame_rgb)

        if self._prev_gray is None:
            self._prev_gray = gray
            self._prev_hist = self._hist(gray)
            # first frame: nothing to compare; treat as a scene cut so the
            # very first window is always analyzed.
            return GateSignal(
                t=t, motion=0.0, threshold=0.0, chi=0.0,
                scene_cut=True, fired=True, high_salience=False,
            )

        motion = self._motion(gray)
        threshold = self._update_ema(motion)
        scene_cut, chi = self._scene_cut(gray, t)

        fired = (motion > threshold) or scene_cut
        high_salience = (motion > self.high_salience_motion) or chi > 0.5

        return GateSignal(
            t=t,
            motion=motion,
            threshold=threshold,
            chi=chi,
            scene_cut=scene_cut,
            fired=fired,
            high_salience=high_salience,
        )

    # --- internals ---

    def _downsample_gray(self, frame: np.ndarray) -> np.ndarray:
        # PIL is faster than cv2 for a one-shot resize; cv2 would force a heavy dep.
        from PIL import Image
        h, w = self.downsample_wh
        im = Image.fromarray(frame).convert("L").resize((w, h), Image.BILINEAR)
        return np.asarray(im, dtype=np.uint8)

    def _motion(self, gray: np.ndarray) -> float:
        diff = np.abs(gray.astype(np.int16) - self._prev_gray.astype(np.int16))
        motion = float(diff.mean()) / 255.0
        self._prev_gray = gray
        return motion

    def _update_ema(self, motion: float) -> float:
        # Welford-style online update for mean + variance.
        if not self._ema_initialized:
            self._ema = motion
            self._ema_var = 0.0
            self._ema_initialized = True
            return self.min_thresh
        delta = motion - self._ema
        self._ema += self.alpha * delta
        # variance estimate: keep a single-sided EMA of squared deltas
        self._ema_var = (1 - self.alpha) * (self._ema_var + self.alpha * delta * delta)
        std = math.sqrt(self._ema_var)
        return max(self.min_thresh, self._ema + self.k * std)

    def _hist(self, gray: np.ndarray) -> np.ndarray:
        h = np.bincount(gray.ravel(), minlength=256).astype(np.float32)
        return h / (h.sum() + 1e-6)

    def _scene_cut(self, gray: np.ndarray, t: float) -> tuple[bool, float]:
        hist = self._hist(gray)
        # chi-square distance between normalized histograms
        denom = self._prev_hist + hist + 1e-6
        chi = float(0.5 * np.sum((hist - self._prev_hist) ** 2 / denom))
        self._prev_hist = hist
        cut = chi > self.scene_cut_thresh and (t - self._last_scene_cut_t) > self.scene_cooldown_s
        if cut:
            self._last_scene_cut_t = t
        return cut, chi
