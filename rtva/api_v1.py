"""/v1 public API: bearer-authenticated ingest, observe, and admin endpoints.

Mounted in `rtva/server.py` as `app.include_router(api_v1.router)`. The legacy
`/sessions` and `/ws/sessions` routes remain unaffected.

Channels:
    ws       — WS /v1/streams/{sid}/ingest, binary [u32 BE pts_ms][jpeg bytes]
    http     — POST /v1/streams/{sid}/ingest/frame, multipart pts_ms + file=jpeg
    kcp      — handled in rtva/kcp_server.py (wired in T6)
"""

from __future__ import annotations

import json
import time
from typing import Optional

from fastapi import (APIRouter, Depends, File, Form, HTTPException, Request,
                     UploadFile, WebSocket, WebSocketDisconnect)
from fastapi.responses import JSONResponse

import asyncio

from .auth import (BYPASS_TOKEN, SCOPES, TokenRecord, TokenStore,
                   generate_token, get_store, has_scope, verify_bearer)
from .config import get_settings
from .ingest_http import HttpPostIngestor
from .ingest_kcp import KcpIngestor
from .ingest_ws import WSIngestor
from .rate_limit import TokenBucket
from .sessions import manager


router = APIRouter(prefix="/v1", tags=["v1"])


# --- Per-token session-create rate limit ------------------------------------
# A simple token bucket per token label, lazily created. Buckets are process-
# local; under multi-worker uvicorn each worker has its own dict. That's
# acceptable here — the goal is "throttle noisy clients", not strict global QoS.
_session_buckets: dict[str, TokenBucket] = {}
_session_buckets_lock = asyncio.Lock()


async def _acquire_session_slot(label: str) -> bool:
    """Returns False if the caller has been rate-limited out of creating a session."""
    s = get_settings()
    async with _session_buckets_lock:
        b = _session_buckets.get(label)
        if b is None:
            b = TokenBucket(capacity=s.session_bucket_capacity,
                            refill_per_sec=s.session_bucket_refill_per_sec)
            _session_buckets[label] = b
    return await b.acquire()


# ============================================================================
# Auth dependency (mirror of rtva.server's require_scopes; kept local so /v1
# remains self-contained — the server.py copy is only used by /v1/whoami.)
# ============================================================================


def require_scopes(*required: str):
    async def _dep(request: Request) -> TokenRecord:
        settings = get_settings()
        if settings.auth_disabled:
            return BYPASS_TOKEN
        auth = request.headers.get("authorization") or request.headers.get("Authorization")
        token: Optional[str] = None
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
        if not token:
            raise HTTPException(401, "missing bearer token",
                                headers={"WWW-Authenticate": "Bearer"})
        rec = verify_bearer(token)
        if not rec:
            raise HTTPException(401, "invalid or revoked token",
                                headers={"WWW-Authenticate": "Bearer"})
        for scope in required:
            if not has_scope(rec, scope):
                raise HTTPException(403, f"missing scope: {scope}")
        return rec

    return _dep


def verify_query_token(token: Optional[str], *required: str) -> TokenRecord:
    """WS variant: extract token from query string instead of header."""
    settings = get_settings()
    if settings.auth_disabled:
        return BYPASS_TOKEN
    if not token:
        raise HTTPException(401, "missing ?token=")
    rec = verify_bearer(token)
    if not rec:
        raise HTTPException(401, "invalid or revoked token")
    for scope in required:
        if not has_scope(rec, scope):
            raise HTTPException(403, f"missing scope: {scope}")
    return rec


# ============================================================================
# Models
# ============================================================================


def _ingest_url(channel: str, sid: str, token: str) -> dict:
    if channel == "ws":
        return {"type": "ws", "url": f"/v1/streams/{sid}/ingest?token={token}"}
    if channel == "http":
        return {
            "type": "http",
            "url": f"/v1/streams/{sid}/ingest/frame",
            "method": "POST",
            "form": {"pts_ms": "uint64 (ms)", "file": "jpeg bytes"},
        }
    if channel == "kcp":
        s = get_settings()
        return {
            "type": "kcp",
            "host": "this-server.host",
            "port": s.kcp_port,
            # Wire conv is fixed; the KCP server routes by `session_id` in the
            # `hello` message (see docs/KCP_WIRE.md §4). `sid_to_u32(sid)` is
            # kept as the documented optional per-session conv for clients that
            # want it for firewall reasons, but the server still only accepts 1.
            "conv": 1,
            "wire": "see docs/KCP_WIRE.md",
        }
    raise ValueError(f"unknown channel: {channel}")


def _observe_url(sid: str, token: str) -> dict:
    return {"type": "ws", "url": f"/v1/streams/{sid}/observe?token={token}"}


def sid_to_u32(sid: str) -> int:
    """Map a UUID session id to a stable u32 KCP conversation id.

    Truncates the first 8 hex chars of the UUID and interprets as u32 BE.
    Collisions on the first 8 hex chars are statistically negligible.
    """
    h = sid.replace("-", "")[:8]
    return int(h, 16)


# ============================================================================
# /v1/streams — create, list, get, delete
# ============================================================================


@router.post("/streams")
async def create_stream(body: dict,
                        rec: TokenRecord = Depends(require_scopes("ingest"))) -> dict:
    """Create a new stream session.

    Body:
        source: "external" (default) | "file" | "url"
        channel: "ws" (default) | "http" | "kcp"
        options: dict (forwarded to the pipeline)
    Returns:
        session_id + ingest/observe URLs for every supported channel.
    """
    # Rate limit per-token. Admin scope bypasses the cap.
    if "admin" not in rec.scopes:
        if not await _acquire_session_slot(rec.label):
            raise HTTPException(429, "rate limit: too many sessions for this token")

    source = body.get("source", "external")
    channel = body.get("channel", "ws")
    options = body.get("options") or {}

    if source not in ("external", "file", "url"):
        raise HTTPException(400, f"unsupported source: {source}")
    if channel not in ("ws", "http", "kcp"):
        raise HTTPException(400, f"unsupported channel: {channel}")
    if source in ("file", "url") and channel != "ws":
        raise HTTPException(400, "source=file|url must use legacy /sessions endpoint")

    if channel == "ws":
        ingestor: WSIngestor | HttpPostIngestor = WSIngestor()
    elif channel == "http":
        ingestor = HttpPostIngestor()
    elif channel == "kcp":
        ingestor = KcpIngestor()

    record = await manager.create_with_ingestor(
        frame_source=ingestor, options=options, channel=channel,
        ingestor=ingestor, owner_token_label=rec.label,
    )
    sid = record.pipeline.session_id
    return {
        "session_id": sid,
        "channel": channel,
        "ingest": _ingest_url(channel, sid, rec.token),
        "observe": _observe_url(sid, rec.token),
        "ingest_alt": _ingest_url("http" if channel == "ws" else "ws", sid, rec.token),
    }


@router.get("/streams")
async def list_streams(rec: TokenRecord = Depends(require_scopes("observe", "ingest"))) -> dict:
    """List sessions visible to the caller's token label."""
    out = []
    for r in manager.all():
        # Token owner sees their own; admin sees all.
        if "admin" in rec.scopes or r.owner_token_label == rec.label:
            out.append({
                "session_id": r.pipeline.session_id,
                "channel": r.channel,
                "started_at": r.pipeline.started_at,
                "source": r.pipeline.source_url or f"external:{r.channel}",
                "stats": r.pipeline.stats.to_dict(),
            })
    return {"sessions": out}


@router.get("/streams/{sid}")
async def get_stream(sid: str,
                     rec: TokenRecord = Depends(require_scopes("observe", "ingest"))) -> dict:
    r = manager.get(sid)
    if not r:
        raise HTTPException(404, "session not found")
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        raise HTTPException(403, "not your session")
    return {
        "session_id": sid,
        "channel": r.channel,
        "source": r.pipeline.source_url or f"external:{r.channel}",
        "started_at": r.pipeline.started_at,
        "stats": r.pipeline.stats.to_dict(),
    }


@router.delete("/streams/{sid}")
async def delete_stream(sid: str,
                        rec: TokenRecord = Depends(require_scopes("ingest"))) -> dict:
    r = manager.get(sid)
    if not r:
        raise HTTPException(404, "session not found")
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        raise HTTPException(403, "not your session")
    ok = await manager.stop(sid)
    if not ok:
        raise HTTPException(409, "session already stopped")
    return {"stopped": True, "session_id": sid}


# ============================================================================
# /v1/streams/{sid}/ingest — frame ingest (WS, HTTP)
# ============================================================================


@router.post("/streams/{sid}/ingest/frame")
async def ingest_frame_http(sid: str,
                            pts_ms: int = Form(...),
                            file: UploadFile = File(...),
                            rec: TokenRecord = Depends(require_scopes("ingest"))) -> JSONResponse:
    r = manager.get(sid)
    if not r or r.channel != "http":
        raise HTTPException(404, "session not found or not in http channel")
    if not isinstance(r.ingestor, HttpPostIngestor):
        raise HTTPException(409, "session ingestor is not HttpPostIngestor")
    blob = await file.read()
    if not blob:
        raise HTTPException(400, "empty upload")
    await r.ingestor.feed(pts_ms=int(pts_ms), jpeg=blob)
    return JSONResponse({"accepted": True, "pts_ms": int(pts_ms), "bytes": len(blob)},
                        status_code=204)


@router.websocket("/streams/{sid}/ingest")
async def ingest_ws(websocket: WebSocket, sid: str, token: Optional[str] = None) -> None:
    # WS auth: token in query, validate before accept
    try:
        rec = verify_query_token(token, "ingest")
    except HTTPException as exc:
        await websocket.close(code=1008, reason=exc.detail)
        return

    r = manager.get(sid)
    if not r or r.channel != "ws":
        await websocket.close(code=1008, reason="session not in ws channel")
        return
    if not isinstance(r.ingestor, WSIngestor):
        await websocket.close(code=1008, reason="ingestor mismatch")
        return

    # Optional ownership check (admin bypasses)
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        await websocket.close(code=1008, reason="not your session")
        return

    await websocket.accept()
    ingestor = r.ingestor
    peer = (websocket.client.host, websocket.client.port) if websocket.client else None
    try:
        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            if "bytes" in msg and msg["bytes"] is not None:
                await ingestor.feed(msg["bytes"], peer=peer)
            elif "text" in msg and msg["text"]:
                # text frames are control — currently a no-op except close
                try:
                    cmd = json.loads(msg["text"])
                    if cmd.get("type") == "close":
                        break
                    if cmd.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong", "t": time.time()}))
                except json.JSONDecodeError:
                    pass
    except WebSocketDisconnect:
        pass
    finally:
        await ingestor.close()


# ============================================================================
# /v1/streams/{sid}/observe — event fan-out (WS), events polling (HTTP)
# ============================================================================


@router.websocket("/streams/{sid}/observe")
async def observe_ws_v1(websocket: WebSocket, sid: str, token: Optional[str] = None) -> None:
    try:
        rec = verify_query_token(token, "observe")
    except HTTPException as exc:
        await websocket.close(code=1008, reason=exc.detail)
        return

    r = manager.get(sid)
    if not r:
        await websocket.close(code=1008, reason="session not found")
        return
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        await websocket.close(code=1008, reason="not your session")
        return

    await websocket.accept()
    async with r.lock:
        r.observers.add(websocket)
    # replay buffer
    for msg in list(r.replay_buf):
        try:
            await websocket.send_text(json.dumps(msg, default=str))
        except Exception:
            break
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        pass
    finally:
        async with r.lock:
            r.observers.discard(websocket)


@router.get("/streams/{sid}/events")
async def stream_events(sid: str, since: Optional[int] = None,
                        rec: TokenRecord = Depends(require_scopes("observe"))) -> dict:
    r = manager.get(sid)
    if not r:
        raise HTTPException(404, "session not found")
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        raise HTTPException(403, "not your session")
    out = []
    for msg in list(r.replay_buf):
        if msg.get("type", "").startswith("event."):
            if since is None or int(msg.get("ts", 0) or 0) >= since:
                out.append(msg)
    return {"events": out}


@router.get("/streams/{sid}/stats")
async def stream_stats(sid: str,
                       rec: TokenRecord = Depends(require_scopes("observe"))) -> dict:
    r = manager.get(sid)
    if not r:
        raise HTTPException(404, "session not found")
    if "admin" not in rec.scopes and r.owner_token_label != rec.label:
        raise HTTPException(403, "not your session")
    return r.pipeline.stats.to_dict()


# ============================================================================
# /v1/admin/tokens — admin-only token CRUD
# ============================================================================


@router.get("/admin/tokens")
async def admin_list_tokens(rec: TokenRecord = Depends(require_scopes("admin"))) -> dict:
    rows = get_store().list()
    return {"tokens": [r.to_public_dict() for r in rows]}


@router.post("/admin/tokens")
async def admin_mint_token(body: dict,
                           rec: TokenRecord = Depends(require_scopes("admin"))) -> dict:
    label = body.get("label")
    if not label:
        raise HTTPException(400, "missing 'label'")
    scopes = body.get("scopes") or ["ingest", "observe"]
    for s in scopes:
        if s not in SCOPES:
            raise HTTPException(400, f"unknown scope: {s}")
    new = get_store().mint(label=label, scopes=scopes)
    # token is returned ONCE
    return {
        "token": new.token,
        "label": new.label,
        "scopes": new.scopes,
        "created_at": new.created_at,
        "_warning": "Save this token now — it will not be shown again.",
    }


@router.delete("/admin/tokens/{token}")
async def admin_revoke_token(token: str,
                             rec: TokenRecord = Depends(require_scopes("admin"))) -> dict:
    # URL-decode if needed
    ok = get_store().revoke(token)
    if not ok:
        raise HTTPException(404, "token not found")
    return {"revoked": True}