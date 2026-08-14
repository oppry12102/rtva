# Changelog

All notable changes to the RTVA public v1 API. The server is the source of
truth; clients should consult this when upgrading.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com).
Each entry is tagged:

- **compat** — purely additive or behavioural change; clients that follow the
  documented contract do not need to change.
- **soft** — server behaves differently under edge conditions; clients should
  ideally handle the new error / behaviour but a degraded experience is
  acceptable if they don't.
- **breaking** — would require a client code change to keep working.

---

## 2026-08-14 — server-side hardening

Five server-side fixes observed in production. No client code changes are
required to keep working; the items below document new fields / errors /
behaviours worth handling.

### KCP `conv` now reliably `1` — compat

- **`POST /v1/streams`** now always returns `"ingest.conv": 1` for
  `channel="kcp"`. Previously it returned a per-session value derived from
  `sid_to_u32(sid)`, which the KCP server (which only accepts `conv=1`)
  rejected with `KCPConvMismatchError` — leaving KCP sessions unable to
  deliver a single frame.
- The `KCP_WIRE.md` §4 contract was always "wire conv is `1`, routing is
  by `session_id` in `hello`"; this change makes the implementation match
  the doc.
- **Client impact**: none, as long as you treat the response's `conv` as
  informational and either hardcode `1` or use whatever the server returns.
  If your client was caching `conv` from one session and reusing it on the
  next, fix that — always use the value from the most recent response, or
  just use `1`.

### Session reaper — soft

- The server now runs a periodic sweep (`REAPER_INTERVAL_S`, default 5 s)
  that kills two classes of zombie sessions:
  - **never-started**: no frames received after
    `SESSION_NEVER_STARTED_TIMEOUT_S` (default 30 s).
  - **idle**: no observers AND no frames on the ingestor for
    `SESSION_IDLE_TIMEOUT_S` (default 60 s).
- A `GET /v1/streams/{sid}` or `DELETE /v1/streams/{sid}` on a reaped
  session returns `404`. This is the documented behaviour for sessions
  that don't exist — no new code path.
- **Client impact**: an Android app that creates a session but never sends
  frames (camera permission denied, app crashed mid-init, network down)
  will see the session disappear from `GET /v1/streams/{sid}` after ~30 s.
  Showing "session expired" rather than just an empty screen is a nice-to-
  have but not required.

### `POST /v1/streams` rate limit — soft

- New per-token token bucket. Default: burst `SESSION_BUCKET_CAPACITY=3`,
  refill `SESSION_BUCKET_REFILL_PER_SEC=0.1` (≈ 1 session per 10 s steady
  state). `admin` scope bypasses.
- When the bucket is empty, the endpoint returns **`429 Too Many Requests`**
  with `{"detail": "rate limit: too many sessions for this token"}`.
- **Client impact**: any client that hammers session creation in a tight
  loop will now see 429s. Recommended handling: backoff + retry, or just
  create a single session and reuse it for the lifetime of the capture.

### Server-side fps cap — soft

- New: `pipeline._consume` drops frames whose `pts` gap is below
  `(1 / TARGET_FPS) * (1 - INGEST_FPS_TOLERANCE)`. Default with
  `TARGET_FPS=8` and `INGEST_FPS_TOLERANCE=0.10` ⇒ frames closer than
  ~112 ms apart are rejected.
- New `stats` fields:
  - `frames_dropped_fps` — frames rejected by the cap.
  - `frames_dropped_queue` — frames rejected by the ingestor's drop-oldest
    (queue full, currently always `0` from the pipeline's view; reported by
    the ingestor itself).
- The existing `frames_received` continues to count what the client sent,
  so the dropped counts are *additive*.
- **Client impact**: none if you already target ≤ 8 fps. If you ever bump
  capture to 30 fps (e.g., for a high-motion scene), the pipeline won't
  speed up — watch `frames_dropped_fps` to confirm you're not silently
  wasting bandwidth.

### `idle_seconds` unified across ingestors — compat

- `WSIngestor` and `KcpIngestor` now expose `idle_seconds` like
  `HttpPostIngestor` already did. Used by the reaper; no behaviour change
  for clients.

---

## 2026-08-14 — event broadcast + escalation threshold

Two bugs found while running observe WS end-to-end with a synthetic high-
motion source.

### Event broadcast on observe WS — **fix** (was: silently dropped)

- The `emit` closure in `SessionManager` looked up the session record by
  `msg["session_id"]` — but `event.provisional`, `event.confirmed`,
  `event.updated`, and `narrative` messages did **not** carry a
  `session_id`. The lookup returned `None` and the message was silently
  dropped, so observers and KCP peers never received them even though the
  server-side `events_emitted` counter still incremented.
- Fix: closure now captures the `record` directly and injects
  `session_id` via `msg.setdefault(...)` before fan-out. All event and
  narrative messages now reach `replay_buf`, observers, and the KCP
  outbound channel.
- **Client impact**: `event.*` and `narrative` messages now flow. Messages
  now always carry `session_id`; clients that ignore unknown fields are
  unaffected.

### `escalations_dispatched` was effectively zero — **fix** (was: hardcoded)

- `MotionGate.high_salience_motion` was hardcoded at `0.20` (mean-abs-diff
  per pixel vs previous frame). Real-world content rarely exceeds this
  threshold, so escalations almost never fired.
- Fix: new config setting `escalate_motion_threshold` (env
  `ESCALATE_MOTION_THRESHOLD`, default **0.20** — behaviour preserved).
  `Pipeline` now reads the setting and constructs the gate accordingly.
- **Client impact**: none unless the operator lowers the env. Recommended
  starting point for high-motion scenes: `0.05`.

---

## Historical

The v1 API was introduced in commit `97b6d14`. This is the first CHANGELOG
entry.