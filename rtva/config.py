"""Project configuration loaded from environment / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.environ.get("RTVA_DOTENV", ".env"), override=False)


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Required env var {name} is missing")
    return v


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _wh(name: str, default: tuple[int, int]) -> tuple[int, int]:
    raw = os.environ.get(name)
    if not raw:
        return default
    a, b = raw.lower().split("x")
    return int(a), int(b)


@dataclass
class Settings:
    # LLM
    minimax_api_key: str = field(default_factory=lambda: _env("MINIMAX_API_KEY"))
    minimax_base_url: str = field(
        default_factory=lambda: os.environ.get(
            "MINIMAX_BASE_URL", "https://api.minimaxi.com/v1/text/chatcompletion_v2"
        )
    )

    # Service
    host: str = field(default_factory=lambda: os.environ.get("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: _env_int("PORT", 8095))

    # Pipeline
    workers: int = field(default_factory=lambda: _env_int("WORKERS", 4))
    window_seconds: float = field(default_factory=lambda: _env_float("WINDOW_SECONDS", 1.5))
    target_fps: int = field(default_factory=lambda: _env_int("TARGET_FPS", 8))
    fast_max_frames: int = field(default_factory=lambda: _env_int("FAST_MAX_FRAMES", 8))
    fast_resolution: tuple[int, int] = field(
        default_factory=lambda: _wh("FAST_RESOLUTION", (448, 252))
    )
    escalate_max_frames: int = field(default_factory=lambda: _env_int("ESCALATE_MAX_FRAMES", 20))
    escalate_resolution: tuple[int, int] = field(
        default_factory=lambda: _wh("ESCALATE_RESOLUTION", (672, 378))
    )

    # Backpressure
    bp_l1: int = field(default_factory=lambda: _env_int("BP_L1", 4))
    bp_l2: int = field(default_factory=lambda: _env_int("BP_L2", 8))
    bp_l3: int = field(default_factory=lambda: _env_int("BP_L3", 12))

    # LLM HTTP
    request_timeout_s: float = field(default_factory=lambda: _env_float("REQUEST_TIMEOUT_S", 20.0))
    max_retry: int = field(default_factory=lambda: _env_int("MAX_RETRY", 2))

    # Auth / tokens (v1 public API)
    token_store_path: str = field(
        default_factory=lambda: os.environ.get("TOKEN_STORE", "data/tokens.json")
    )
    auth_disabled: bool = field(
        default_factory=lambda: os.environ.get("AUTH_DISABLED", "false").lower() in ("1", "true", "yes")
    )

    # KCP channel (Android fast video transport)
    kcp_host: str = field(default_factory=lambda: os.environ.get("KCP_HOST", "0.0.0.0"))
    kcp_port: int = field(default_factory=lambda: _env_int("KCP_PORT", 8096))
    kcp_enabled: bool = field(
        default_factory=lambda: os.environ.get("KCP_ENABLED", "true").lower() in ("1", "true", "yes")
    )

    # Ingest rate cap (server-side, drops frames faster than target_fps).
    # tolerance: drop when interval < (1/target_fps) * (1 - tolerance).
    ingest_fps_tolerance: float = field(default_factory=lambda: _env_float("INGEST_FPS_TOLERANCE", 0.10))

    # Session-create rate limit (per-token token bucket). capacity = burst size,
    # refill_per_sec ≈ 1 session per (1/refill) seconds in steady state.
    session_bucket_capacity: int = field(default_factory=lambda: _env_int("SESSION_BUCKET_CAPACITY", 3))
    session_bucket_refill_per_sec: float = field(
        default_factory=lambda: _env_float("SESSION_BUCKET_REFILL_PER_SEC", 0.1)
    )

    # Session reaper — sweeps zombies on a fixed interval.
    reaper_interval_s: float = field(default_factory=lambda: _env_float("REAPER_INTERVAL_S", 5.0))
    session_never_started_timeout_s: float = field(
        default_factory=lambda: _env_float("SESSION_NEVER_STARTED_TIMEOUT_S", 30.0)
    )
    session_idle_timeout_s: float = field(
        default_factory=lambda: _env_float("SESSION_IDLE_TIMEOUT_S", 60.0)
    )


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
