"""FastAPI + WebSocket server for the RTVA pipeline.

- POST /sessions: create a session, returns {session_id, observe_ws}
- GET /sessions: list sessions
- GET /sessions/{id}: detail
- DELETE /sessions/{id}: stop
- WS /ws/sessions/{id}/observe: fan-out of analysis results, with replay buffer
- GET /: serve the demo UI

The /v1/* routes (bearer-authenticated) are mounted from `rtva.api_v1`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import api_v1
from . import kcp_server
from .auth import BYPASS_TOKEN, TokenRecord, has_scope, verify_bearer
from .config import get_settings
from .sessions import manager


# --- App ---------------------------------------------------------------------


app = FastAPI(title="RTVA — Real-Time Video Analysis", version="0.1")

# Public /v1 API (bearer-auth). The legacy /sessions + /ws/sessions routes below
# remain unauthenticated for the demo UI.
app.include_router(api_v1.router)


@app.on_event("startup")
async def _start_kcp() -> None:
    """Launch the KCP server alongside uvicorn."""
    try:
        await kcp_server.start_server()
    except Exception as exc:
        print(f"[server] KCP server failed to start: {exc!r}")
    # Start the periodic session reaper (zombie-sweeper).
    await manager.start_reaper()


@app.on_event("shutdown")
async def _stop_kcp() -> None:
    await kcp_server.stop_server()
    await manager.stop_reaper()


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "sessions": len(manager.all())}


@app.post("/sessions")
async def create_session(req: Request) -> JSONResponse:
    body = await req.json()
    source = body.get("source")
    if not source:
        raise HTTPException(400, "missing 'source'")
    options = body.get("options") or {}
    rec = await manager.create(source=source, options=options)
    return JSONResponse({
        "session_id": rec.pipeline.session_id,
        "observe_ws": f"/ws/sessions/{rec.pipeline.session_id}/observe",
        "stats_url": f"/sessions/{rec.pipeline.session_id}/stats",
    })


@app.get("/sessions")
async def list_sessions() -> dict:
    return {
        "sessions": [
            {"session_id": r.pipeline.session_id,
             "source": r.pipeline.source_url,
             "started_at": r.pipeline.started_at,
             "stats": r.pipeline.stats.to_dict()}
            for r in manager.all()
        ]
    }


@app.get("/sessions/{sid}")
async def session_detail(sid: str) -> dict:
    rec = manager.get(sid)
    if not rec:
        raise HTTPException(404, "session not found")
    return {"session_id": sid, "source": rec.pipeline.source_url,
            "stats": rec.pipeline.stats.to_dict()}


@app.get("/sessions/{sid}/events")
async def session_events(sid: str, since: Optional[int] = None) -> dict:
    rec = manager.get(sid)
    if not rec:
        raise HTTPException(404, "session not found")
    out = []
    for msg in list(rec.replay_buf):
        if msg.get("type", "").startswith("event."):
            if since is None or int(msg.get("ts", 0)) >= since:
                out.append(msg)
    return {"events": out}


@app.get("/sessions/{sid}/stats")
async def session_stats(sid: str) -> dict:
    rec = manager.get(sid)
    if not rec:
        raise HTTPException(404, "session not found")
    return rec.pipeline.stats.to_dict()


@app.delete("/sessions/{sid}")
async def delete_session(sid: str) -> dict:
    ok = await manager.stop(sid)
    if not ok:
        raise HTTPException(404, "session not found")
    return {"stopped": True, "session_id": sid}


@app.websocket("/ws/sessions/{sid}/observe")
async def observe_ws(websocket: WebSocket, sid: str) -> None:
    await websocket.accept()
    rec = manager.get(sid)
    if not rec:
        await websocket.send_text(json.dumps({"type": "error", "code": "no_session"}))
        await websocket.close()
        return
    async with rec.lock:
        rec.observers.add(websocket)
    # replay buffer
    for msg in list(rec.replay_buf):
        try:
            await websocket.send_text(json.dumps(msg, default=str))
        except Exception:
            break
    try:
        while True:
            # keep the socket alive; we don't expect inbound except ping/pong
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with rec.lock:
            rec.observers.discard(websocket)


# --- Static demo UI ----------------------------------------------------------

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@app.get("/")
async def index() -> FileResponse:
    f = WEB_DIR / "index.html"
    if not f.exists():
        raise HTTPException(404, "demo UI not built yet")
    return FileResponse(f)


# --- /v1 auth-protected demo route (T1 verification only) ----------------------
#
# Real /v1/* routes (streams, ingest, observe, admin/tokens) land in T3.
# This single route exists to verify the auth wiring end-to-end. It is removed
# when the full /v1 router is mounted.


def require_scopes(*required: str):
    """FastAPI dependency factory: require a valid bearer token with all scopes.

    `admin` scope satisfies any requirement (admin tokens are superusers).
    If `settings.auth_disabled` is true, returns a synthetic bypass token.
    """
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


@app.get("/v1/whoami")
async def whoami(rec: TokenRecord = Depends(require_scopes())) -> dict:
    return {
        "label": rec.label,
        "scopes": rec.scopes,
        "created_at": rec.created_at,
        "last_used_at": rec.last_used_at,
    }
