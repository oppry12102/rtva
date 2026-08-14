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

## 2026-08-14 — Chinese narration in `event.*` / `narrative` — **fix** (was: English)

The video-analysis output was hardcoded English regardless of the caller's
language. Two related defects in the prompt wiring:

1. `prompts.SYSTEM_PROMPT` was a single English constant. The pipeline read
   `options["language"]` and only echoed it in `session.started` — it never
   reached the LLM.
2. `Event.provisional_from_gate` hardcoded `description="significant change"`
   in English, which the LLM echoed back when there was nothing more
   specific to say.

Fix:

- `prompts.build_system_prompt(language)` returns a `zh` or `en` system
  prompt. Chinese variant instructs the model to write `narrative` and
  event `description` in Simplified Chinese; type enum values
  (`action` / `scene_change` / `object_appear` / `object_disappear` /
  `anomaly` / `transition`) stay English (canonical, shared with
  `_MERGEABLE_TYPES` and downstream client routing).
- `Pipeline.__post_init__` resolves `self.language` from
  `options.get("language", "zh")` once and stores the resulting
  `self.system_prompt`. The system prompt is the prefix that hits
  MiniMax's server-side prompt cache, so it must be stable for the
  lifetime of a session — never re-resolved per window.
- `Event.provisional_from_gate(..., *, language="zh")` now localises the
  fallback reason (`"显著变化"` for `zh`, `"significant change"` for `en`).

**Client impact**: any client that was passing `"language"` in `options` and
seeing English now sees the requested language. Default remains `"zh"`
(the operator-requested language). Type-based routing on the client side
is unaffected — type values are still the six English enums.

---

## 2026-08-15 — KCP framer accepts Android wire format — **fix** (was: frames silently dropped)

The KCP framer (`rtva/kcp_server.py::Framer`) had two compounding bugs that
left `frames_received=0` for any client following `docs/KCP_WIRE.md`
(including the Android app). The user-visible symptom: KCP-level ACKs
worked (server responded to every packet), `session.started` fired, but
no `event.*` / `narrative` ever arrived; observe WS reported
`windows_dispatched=1` and `windows_completed=0` until the session
idled out.

1. **Wire-format mismatch**: the framer only honored the
   `hdr.payload_len` JSON field. The Python `kcp_sender.py` sets that
   field; the Android client (and the docs) don't. Android frames
   therefore hit the `else: payload = b""` branch and the JPEG decode
   failed silently.
2. **Unhandled `UnicodeDecodeError`**: when the JPEG bytes were then
   treated as the next JSON body, `json.loads` raised
   `UnicodeDecodeError` (JPEG bytes aren't valid UTF-8). The framer
   only caught `JSONDecodeError`, so the exception propagated up,
   killed the peer's data path, and silently dropped every subsequent
   frame. KCP-level ACKs were unaffected, hiding the failure.

Fix:

- Framer now treats `type=="frame"` as always carrying a wire-prefixed
  payload. New `_consume_frame_payload` accepts both forms:
    - **Form A** (Python `kcp_sender.py`): `hdr.payload_len` set +
      matching wire `[u32 BE len][bytes]` prefix.
    - **Form B** (Android + docs): wire prefix only, no JSON field.
- `feed()` now catches `(json.JSONDecodeError, UnicodeDecodeError)` so
  a single garbage message logs a warning and the framer resyncs
  instead of killing the peer's data path.
- Drive-by fix for a latent push-back bug
  (`self._buf = msg_with_len(msg) + self._buf`) that would have
  crashed the next `feed()` call with `AttributeError` if a frame
  header arrived fragmented across packets.

**Client impact**: Android KCP sessions now produce events end-to-end.
Verified with `examples/kcp_sender.py` running in Form B
(`payload_len` stripped) — full event flow including Chinese
narratives, all the way through to the KCP outbound channel.

---

## Historical

The v1 API was introduced in commit `97b6d14`. This is the first CHANGELOG
entry.