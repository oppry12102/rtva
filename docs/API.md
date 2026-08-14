# RTVA Public API Reference

> **Audience**: external app developers integrating with the RTVA real-time
> video analysis service.

The RTVA service accepts a live video stream from an external source (Android
app, web client, edge box, etc.), runs it through a frame-windowing pipeline,
and emits structured analysis events back to the caller. Three transport
options are offered — pick whichever fits your deployment:

| Channel | Best for | Cost | Notes |
| --- | --- | --- | --- |
| **HTTP `POST /ingest/frame`** | Server-to-server, retry-friendly | Simple | One HTTP request per frame, multipart upload |
| **WebSocket `/ingest`** | Browser/JS or low-latency web clients | Simple | Binary frames, low overhead, single TCP conn |
| **KCP over UDP `/8096`** | Mobile / jitter-sensitive links | Most work to integrate | UDP-based, configurable loss recovery; matches `kcp-go` / `l42111996/kcp` byte-for-byte |

The pipeline keeps analysis results uniform across channels — all three emit
the same `event.provisional` / `event.confirmed` / `narrative` / `stats` JSON
shape (over WebSocket observe, polling `/events`, or the KCP outbound stream).

---

## 1. Authentication

RTVA uses opaque bearer tokens. Tokens are minted by an admin via the CLI
(`python -m rtva.auth mint <label> --scopes ingest,observe`) and stored
locally in `data/tokens.json` (gitignored, `0600` permissions). The token
**is shown once** — the server keeps only a hash index plus metadata.

```
Authorization: Bearer rtva_<64 hex chars>
```

### Scopes

| Scope | Permissions |
| --- | --- |
| `ingest` | `POST /v1/streams`, frame ingest (HTTP / WS / KCP hello), `DELETE /v1/streams/{id}` |
| `observe` | `GET /v1/streams/{id}/events`, `GET /v1/streams/{id}/stats`, `WS /v1/streams/{id}/observe`, `GET /v1/streams` (read-only list) |
| `admin` | superset of `ingest` + `observe`; also `GET/POST/DELETE /v1/admin/tokens` |

`admin` satisfies any scope check. Tokens default to `["ingest","observe"]`
when minted without `--scopes`.

### Escaping auth (development only)

Set `AUTH_DISABLED=true` in `.env` to bypass auth entirely (any request
passes). **Never ship this to production.**

### Auth errors

```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
{"detail": "missing bearer token"}

HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
{"detail": "invalid or revoked token"}

HTTP/1.1 403 Forbidden
{"detail": "missing scope: ingest"}
```

---

## 2. Stream lifecycle

A **stream session** is a one-to-one mapping between an external source and
the analysis pipeline. Once created, the session id (`sid`) is used for all
subsequent ingest and observe calls.

```
   client                         server
     │ ─── POST /v1/streams ─────▶ │   create session
     │ ◀─── { session_id, ... } ─ │   sid is a UUID
     │                              │
     │ ──── ingest frames ──────▶ │   any of {HTTP POST, WS, KCP}
     │                              │
     │ ◀─── analysis events ───── │   any of {WS observe, polling, KCP outbound}
     │                              │
     │ ─── DELETE /v1/streams/sid ▶ │   stop
```

Sessions are auto-deleted when the source closes or after `DELETE`. A session
that receives no frames for ~10 s is also reaped (configurable).

### Multi-channel selection

`POST /v1/streams` accepts a `channel` field — `ws` (default), `http`, or
`kcp`. The server creates the pipeline for that channel and returns the
**full set of access points** in the response:

```json
{
  "session_id": "1cddc885-d9ae-49a5-820e-305d2ac713c6",
  "channel": "kcp",
  "ingest":     { "type": "kcp",  "host": "...", "port": 8096, ... },
  "observe":    { "type": "ws",   "url":  "/v1/streams/<sid>/observe?token=..." },
  "ingest_alt": { "type": "ws",   "url":  "/v1/streams/<sid>/ingest?token=..." }
}
```

Choose `channel=kcp` to lock the session's pipeline to the KCP framing
protocol (frames arriving over WS or HTTP will be rejected for that session).
The `ingest_alt` field always gives the alternate WebSocket path so the app
can implement automatic fallback when UDP is firewalled.

---

## 3. HTTP endpoints

All `/v1/*` paths require a bearer token in the `Authorization` header. The
base URL is the same as the demo (`http://server:8095`).

### `POST /v1/streams` — create a session

**Scope**: `ingest`

```http
POST /v1/streams
Authorization: Bearer rtva_abc...
Content-Type: application/json

{
  "source":  "external",                // "external" | "file" | "url"
  "channel": "ws",                      // "ws" | "http" | "kcp"
  "options": {
    "fps":             8,
    "language":        "zh",            // narration language (zh | en)
    "escalation":      true,            // enable slow-and-accurate path
    "window_seconds":  1.5
  }
}
```

Response `200`:

```json
{
  "session_id": "1cddc885-d9ae-49a5-820e-305d2ac713c6",
  "channel":    "kcp",
  "ingest":     { "type": "kcp", "host": "...", "port": 8096, "conv": 587855705,
                  "wire": "see docs/KCP_WIRE.md" },
  "observe":    { "type": "ws",  "url": "/v1/streams/<sid>/observe?token=rtva_..." },
  "ingest_alt": { "type": "ws",  "url": "/v1/streams/<sid>/ingest?token=rtva_..." }
}
```

Errors: `400` (bad source/channel), `401`, `403`.

### `GET /v1/streams` — list sessions visible to your token

**Scope**: `ingest` or `observe`

Returns sessions owned by the calling token, or all sessions if the token has
`admin` scope.

```json
{
  "sessions": [
    { "session_id": "...", "channel": "kcp", "started_at": 1786697695.17,
      "source": "external:kcp", "stats": { "frames_received": 80, ... } }
  ]
}
```

### `GET /v1/streams/{sid}` — session detail

**Scope**: `observe` or `ingest`

Returns the same shape as one element of the list, scoped to one sid.

### `DELETE /v1/streams/{sid}` — stop a session

**Scope**: `ingest`

```http
DELETE /v1/streams/1cddc885-d9ae-49a5-820e-305d2ac713c6
Authorization: Bearer rtva_abc...
```

Response `200`:

```json
{ "stopped": true, "session_id": "1cddc885-..." }
```

### `POST /v1/streams/{sid}/ingest/frame` — single-frame HTTP ingest

**Scope**: `ingest`

Only valid for sessions created with `channel="http"`.

```http
POST /v1/streams/<sid>/ingest/frame
Authorization: Bearer rtva_abc...
Content-Type: multipart/form-data; boundary=----X

------X
Content-Disposition: form-data; name="pts_ms"

1000
------X
Content-Disposition: form-data; name="file"; filename="frame.jpg"
Content-Type: image/jpeg

<jpeg bytes>
------X--
```

Response `204 No Content` on success. Errors: `400` empty upload, `404`
session not in HTTP channel, `409` ingestor mismatch.

This is the simplest possible client — useful for shell scripts or
server-to-server relay. Throughput is bounded by HTTP overhead (~100–500
req/s practical per client).

### `GET /v1/streams/{sid}/events?since=<ts_ms>` — event polling

**Scope**: `observe`

Returns events with `ts >= since` (defaults to all). Used as a fallback when
WebSocket observe isn't reachable.

```json
{
  "events": [
    { "type": "event.provisional", "ts": 1234567890,
      "session_id": "...", "event": { "type": "object", "label": "person",
        "confidence": 0.91, "description": "..." } }
  ]
}
```

### `GET /v1/streams/{sid}/stats` — session stats

**Scope**: `observe`

```json
{
  "frames_received": 80,
  "windows_dispatched": 3,
  "windows_completed": 2,
  "events_emitted": 1,
  "latency_p50_ms": 7563,
  "cache_hits": 2,
  "queue_depth": 0,
  "backpressure_level": 0,
  ...
}
```

### Admin endpoints (`/v1/admin/tokens`)

**Scope**: `admin` for all

```http
GET /v1/admin/tokens
POST /v1/admin/tokens      { "label": "android-app-1",
                              "scopes": ["ingest","observe"] }
DELETE /v1/admin/tokens/<token>
```

`POST` returns the token once, with a `_warning` field reminding you to save
it immediately.

---

## 4. WebSocket protocols

### `/v1/streams/{sid}/ingest` — frame ingest (binary)

**Scope**: `ingest`

Token is passed via the query string because WebSocket clients can't always
set headers in the browser:

```
ws://server:8095/v1/streams/<sid>/ingest?token=rtva_...
```

Each binary message is a single JPEG frame, prefixed with a 32-bit
big-endian timestamp:

```
+-----------------+----------------------+
| pts_ms (u32 BE) | jpeg bytes (...)     |
+-----------------+----------------------+
```

Recommended cadence: 5–15 fps. Frames outside the configured window are
dropped at the server (queue length ≤ 32 by default).

Text frames are control commands:

```json
{ "type": "close" }
{ "type": "ping" }   // server replies { "type": "pong", "t": <echo> }
```

Errors are reported by closing with a 1008 code and a reason string in the
close frame (e.g. `not your session`, `session not in ws channel`).

### `/v1/streams/{sid}/observe` — event fan-out (JSON)

**Scope**: `observe`

```
ws://server:8095/v1/streams/<sid>/observe?token=rtva_...
```

Server sends JSON messages of these types:

| `type` | Meaning | Fields |
| --- | --- | --- |
| `event.provisional` | A tentative classification (lower confidence) | `ts`, `event.type`, `event.confidence`, `event.description`, `event.bbox` |
| `event.confirmed`   | A confident classification | as above |
| `narrative`         | Periodic natural-language summary | `ts`, `text` |
| `stats`             | Updated pipeline counters | `ts`, fields from `/stats` |
| `error`             | Server-side error | `code`, `message` |

`event.confidence ≥ 0.7` is the typical threshold for confirmed vs
provisional; tune via the `ESCALATE_THRESHOLD` env var.

On connect, the server replays the last 200 buffered events so the client
catches up immediately (no need to poll `/events` afterwards).

---

## 5. KCP channel (UDP/8096)

See **[KCP_WIRE.md](KCP_WIRE.md)** for the byte layout, Java/Kotlin client
example, and a reference Python sender (`examples/kcp_sender.py`).

**TL;DR** — the KCP layer is wire-compatible with `kcp-go` and
`l42111996/kcp`. On top of KCP we run a length-prefixed JSON+payload
framing:

```
[u32 BE len][JSON header][optional [u32 BE len][jpeg payload]]
```

The first message after connecting **must** be a `hello` carrying the bearer
token and session id; the server rejects everything else with `bye`.

---

## 6. Error codes

| Code | When |
| --- | --- |
| `400` | Malformed body (missing `source`, unknown channel, empty upload) |
| `401` | Missing or revoked bearer token |
| `403` | Token lacks required scope; or token not owner of session (and not admin) |
| `404` | Session not found (or wrong channel for ingest) |
| `409` | Session already stopped, or ingestor/channel mismatch |
| `5xx` | Pipeline failure (rare; details logged on server side) |

---

## 7. Limits and backpressure

| Limit | Default | Where |
| --- | --- | --- |
| Per-session queue | 32 frames | `Pipeline.queue_maxsize` |
| KCP window size | 128 segments | KCP wire params |
| HTTP frame size | unbounded (multipart) | nginx/uvicorn limits |
| Event replay buffer | 200 messages per session | `SessionRecord.replay_buf` |
| Token lifetime | indefinite until `DELETE /v1/admin/tokens/<token>` | revoke-only |

When the ingest queue fills, the server drops the oldest frame (latest-wins)
and increments `backpressure_level` — clients can watch `/stats` to detect
sustained pressure.

---

## 8. Examples

### cURL

```bash
# Mint a token (admin first, then a per-app token)
ADMIN=$(python -m rtva.auth mint admin-1 --scopes admin | grep ^rtva_)
APP=$(python -m rtva.auth mint android-1 --scopes ingest,observe | grep ^rtva_)

# Create a stream
SID=$(curl -s -X POST -H "Authorization: Bearer $APP" -H 'content-type: application/json' \
  -d '{"source":"external","channel":"ws"}' \
  http://localhost:8095/v1/streams | jq -r .session_id)

# Ingest a JPEG frame
curl -X POST -H "Authorization: Bearer $APP" \
  -F "pts_ms=1000" -F "file=@frame.jpg" \
  http://localhost:8095/v1/streams/$SID/ingest/frame

# Poll events
curl -H "Authorization: Bearer $APP" \
  "http://localhost:8095/v1/streams/$SID/events?since=0" | jq

# Stop
curl -X DELETE -H "Authorization: Bearer $APP" \
  http://localhost:8095/v1/streams/$SID
```

### Python WS sender

```python
import asyncio, json, struct, websockets, httpx

TOKEN = "rtva_..."
r = httpx.post("http://localhost:8095/v1/streams",
               headers={"Authorization": f"Bearer {TOKEN}"},
               json={"source": "external", "channel": "ws"})
sid = r.json()["session_id"]

async def main():
    uri = f"ws://localhost:8095/v1/streams/{sid}/ingest?token={TOKEN}"
    async with websockets.connect(uri) as ws:
        # send one JPEG frame at pts=1000ms
        with open("frame.jpg", "rb") as f:
            jpeg = f.read()
        await ws.send(struct.pack(">I", 1000) + jpeg)
asyncio.run(main())
```

### Python KCP sender

See `examples/kcp_sender.py` — full reference implementation that reads a
video file, encodes frames to JPEG, and streams over KCP.

### Java/Kotlin Android client

The on-wire protocol is documented in **[KCP_WIRE.md](KCP_WIRE.md)** — use
[`l42111996/kcp`](https://github.com/l42111996/kcp) (Netty/Java/Unity
compatible) as the KCP transport.

---

## 9. Operational notes

- **HTTPS**: bearer tokens are sent in cleartext over HTTP. Deploy behind
  a TLS-terminating reverse proxy (nginx, Caddy) for production.
- **UDP exposure**: the KCP server listens on `0.0.0.0:8096` by default.
  If you don't need the KCP channel, set `KCP_ENABLED=false` in `.env`.
- **No KCP-layer encryption**: the KCP payload is not encrypted. For
  end-to-end confidentiality, run the service behind WireGuard / VPN.
- **Token rotation**: there is no refresh endpoint — mint a new token,
  update clients, then `DELETE` the old one. Tokens have no expiry by
  default (operator revokes manually).
- **Disk usage**: `data/tokens.json` is the entire persistent state. No
  databases required.