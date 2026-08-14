"""Token authentication for the public /v1 API.

Tokens are opaque 256-bit strings prefixed `rtva_`. They are stored in
`data/tokens.json` (configurable via `TOKEN_STORE`) and protected by
`fcntl.flock` so multi-worker servers stay consistent.

Scopes:
    - admin   : mint/revoke/list tokens via /v1/admin/tokens
    - ingest  : create streams + push video frames
    - observe : subscribe to analysis events/stats

CLI:
    python -m rtva.auth mint <label> [--scopes ingest,observe]
    python -m rtva.auth list
    python -m rtva.auth revoke <token-or-prefix>
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import secrets
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Optional

# ----------------------------------------------------------------------------
# Token model
# ----------------------------------------------------------------------------


SCOPES: tuple[str, ...] = ("admin", "ingest", "observe")
TOKEN_PREFIX = "rtva_"
# 32 random bytes = 64 hex chars = 256 bits of entropy
_TOKEN_BYTES = 32


@dataclass
class TokenRecord:
    token: str
    label: str
    scopes: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_used_at: float = 0.0
    disabled: bool = False

    def to_public_dict(self) -> dict:
        """Dict for /v1/admin/tokens responses (token masked)."""
        return {
            "label": self.label,
            "scopes": list(self.scopes),
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
            "disabled": self.disabled,
            "token_prefix": self.token[:12] + "…",
        }


# Synthetic token returned by require_scopes() when settings.auth_disabled is on.
# Has every scope so dev/testing never trips the auth checks.
BYPASS_TOKEN = TokenRecord(
    token="__bypass__", label="auth_disabled",
    scopes=["admin", "ingest", "observe"],
)


def generate_token() -> str:
    return TOKEN_PREFIX + secrets.token_hex(_TOKEN_BYTES)


def mask_token(token: str) -> str:
    if len(token) < 16:
        return token[:4] + "…"
    return token[:12] + "…"


# ----------------------------------------------------------------------------
# Store
# ----------------------------------------------------------------------------


class TokenStore:
    """File-backed token store with flock-guarded reads/writes.

    A simple JSON file works because:
    - volume is tiny (handful of tokens, not thousands)
    - writes are infrequent (admin actions only)
    - we don't need query indexes — linear scan over a dict is fine
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({})

    # ----- low-level IO -----

    def _read(self) -> dict[str, dict]:
        with self._open("r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def _write(self, data: dict[str, dict]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with self._open("w", file=tmp) as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        # chmod 0600 (best-effort; ignore on platforms without it)
        with contextlib.suppress(OSError):
            os.chmod(self.path, 0o600)

    @contextlib.contextmanager
    def _open(self, mode: str, file: Optional[Path] = None):
        p = file or self.path
        fd = os.open(str(p), os.O_RDWR | os.O_CREAT, 0o600)
        f = None
        try:
            f = os.fdopen(fd, mode)
        except Exception:
            os.close(fd)
            raise
        try:
            # exclusive lock for writes, shared for reads
            if mode.startswith("r"):
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            else:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            yield f
        finally:
            f.close()  # releases flock + closes file

    # ----- public API -----

    def mint(self, label: str, scopes: Iterable[str]) -> TokenRecord:
        scopes = list(scopes)
        for s in scopes:
            if s not in SCOPES:
                raise ValueError(f"unknown scope: {s}")
        rec = TokenRecord(token=generate_token(), label=label, scopes=scopes)
        data = self._read()
        data[rec.token] = asdict(rec)
        self._write(data)
        return rec

    def revoke(self, token_or_prefix: str) -> bool:
        data = self._read()
        full = self._resolve(data, token_or_prefix)
        if full is None:
            return False
        data[full]["disabled"] = True
        self._write(data)
        return True

    def list(self) -> list[TokenRecord]:
        data = self._read()
        return [TokenRecord(**v) for v in data.values()]

    def verify(self, token: str) -> Optional[TokenRecord]:
        if not token or not token.startswith(TOKEN_PREFIX):
            return None
        data = self._read()
        raw = data.get(token)
        if not raw or raw.get("disabled"):
            return None
        rec = TokenRecord(**raw)
        # best-effort: update last_used_at (don't fail verification on this)
        try:
            data[token]["last_used_at"] = time.time()
            self._write(data)
        except Exception:
            pass
        return rec

    @staticmethod
    def _resolve(data: dict[str, dict], token_or_prefix: str) -> Optional[str]:
        if token_or_prefix in data:
            return token_or_prefix
        # prefix match (>= 12 chars to avoid accidents)
        if len(token_or_prefix) >= 12:
            matches = [k for k in data if k.startswith(token_or_prefix)]
            if len(matches) == 1:
                return matches[0]
        return None


# ----------------------------------------------------------------------------
# Singleton + FastAPI integration
# ----------------------------------------------------------------------------


_store: Optional[TokenStore] = None


def get_store() -> TokenStore:
    global _store
    if _store is None:
        from .config import get_settings
        _store = TokenStore(Path(get_settings().token_store_path))
    return _store


def verify_bearer(token: Optional[str]) -> Optional[TokenRecord]:
    if not token:
        return None
    return get_store().verify(token)


def has_scope(rec: TokenRecord, scope: str) -> bool:
    return scope in rec.scopes or "admin" in rec.scopes


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def _cli_mint(args: argparse.Namespace) -> int:
    scopes = [s.strip() for s in (args.scopes or "ingest,observe").split(",") if s.strip()]
    for s in scopes:
        if s not in SCOPES:
            print(f"error: unknown scope '{s}' (valid: {', '.join(SCOPES)})", file=sys.stderr)
            return 2
    rec = get_store().mint(label=args.label, scopes=scopes)
    print(rec.token)
    print(f"# label={rec.label} scopes={','.join(rec.scopes)}", file=sys.stderr)
    return 0


def _cli_list(args: argparse.Namespace) -> int:
    rows = get_store().list()
    if not rows:
        print("(no tokens)", file=sys.stderr)
        return 0
    for r in rows:
        flag = "DISABLED" if r.disabled else "ok      "
        print(f"{mask_token(r.token)}  {flag}  {r.label:<24}  {','.join(r.scopes)}")
    return 0


def _cli_revoke(args: argparse.Namespace) -> int:
    ok = get_store().revoke(args.token)
    if not ok:
        print(f"error: token not found: {args.token}", file=sys.stderr)
        return 2
    print(f"revoked: {args.token}", file=sys.stderr)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m rtva.auth",
                                     description="RTVA token admin CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_mint = sub.add_parser("mint", help="mint a new token")
    p_mint.add_argument("label")
    p_mint.add_argument("--scopes", default="ingest,observe",
                        help=f"comma-separated scopes (default: ingest,observe). valid: {','.join(SCOPES)}")
    p_mint.set_defaults(func=_cli_mint)

    p_list = sub.add_parser("list", help="list tokens (masked)")
    p_list.set_defaults(func=_cli_list)

    p_revoke = sub.add_parser("revoke", help="revoke a token by full value or >=12-char prefix")
    p_revoke.add_argument("token")
    p_revoke.set_defaults(func=_cli_revoke)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())