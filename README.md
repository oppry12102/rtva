# RTVA — Real-Time Video Analysis (MiniMax-M3)

A real-time streaming video content analysis service. Ingest a live or recorded
video stream, get a **rolling natural-language commentary** plus **structured
key-event records** pushed live over WebSocket.

Built around `MiniMax-M3` for the LLM and a CPU-only cognition gate for sub-second
pre-alerts. Inspired by [StreamingVLM](https://github.com/mit-han-lab/streaming-vlm)
and [Microsoft Mage](https://github.com/microsoft/Mage) — adapted for an
**API-only, no-GPU** setting.

---

## Two-speed output (the core idea)

Pure cloud M3 calls take ~3-4.5s p50. Pretending otherwise gives a fake real-time
system. Instead we provide **two delivery speeds**:

| Moment | Output | Source | Latency |
|---|---|---|---|
| t + 0.1s | `event.provisional` (UI marks "analyzing") | CPU gate | **< 0.2s** |
| t + 4-5s | `event.confirmed` (typed description, confidence) | M3 fast pass | ~4.5s |
| t + ~10s | `event.updated` (refined description) | M3 escalation pass (if triggered) | async |

The user sees the system react *immediately*; the semantic description fills in
~5s later. If you need sub-second semantic latency, run StreamingVLM/Mage-VL
locally on a GPU box and feed the LLM only when that local layer flags high
salience.

---

## Quickstart

```bash
# 1. Install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# edit .env to set MINIMAX_API_KEY

# 3. Generate a synthetic test video (no camera/ffmpeg needed)
python scripts/make_test_video.py test_videos/test60.mp4 60 25

# 4. Run the pipeline against it
python scripts/run_pipeline.py test_videos/test60.mp4 --duration 90
# → prints events, stats, and timing

# 5. Or boot the server + Demo UI
uvicorn rtva.server:app --host 0.0.0.0 --port 8095
# open http://localhost:8095
```

---

## Architecture

```
video stream
   │
   ▼
[PyAV source]  --pacing-->  [MotionGate]  ── fast alert --> WS: event.provisional (~0.1s)
                              │  (CPU <5ms/frame)
                              ▼
                        [FrameRing] ── windowed ──> [Scheduler]
                                                       │
                                              4-worker pool + backpressure
                                                       │
                                                       ▼
                                            [M3Client.analyze_window]
                                                       │
                                          thinking disabled, JSON forced
                                                       │
                                                       ▼
                                                [StreamingMemory]
                                          sink + sliding context (bounded)
                                                       │
                                                       ▼
                                                  [EventStore]
                                          temporal+lexical dedup/merge
                                                       │
                                                       ▼
                                                WS fan-out to clients
```

**Files:**
```
rtva/
  config.py        # env-driven config
  source.py        # PyAV decoder, PTS-paced
  gate.py          # MotionGate (CPU <5ms/frame)
  llm.py           # async M3 client + JSON repair
  prompts.py       # stable sink system prompt + per-window user message
  memory.py        # rolling context (bounded, no LLM compaction in v1)
  events.py        # Event model + trigram-Jaccard dedup
  scheduler.py     # worker pool + backpressure ladder
  pipeline.py      # session orchestration
  server.py        # FastAPI + WebSocket
web/
  index.html       # demo UI (camera not required — file URL works)
scripts/
  make_test_video.py  # synthetic test video with ground-truth events
  run_pipeline.py     # CLI runner + event report
  bench_api.py        # reproduce latency/throughput numbers
```

---

## Measured performance (benchmarked against live M3)

| Metric | Value |
|---|---|
| M3 single call (uncached, 8 frames) | **p50 3-4.5s, p95 4.7s** |
| M3 single call (cached) | as low as 1.9s |
| Frames per request cost | ~150 tokens @ 448x252 — cheap |
| Concurrency (16-way) | 2.30 req/s, 15/16 success |
| Gate CPU cost | **1.4ms/frame** (single-core, 224fps headroom) |
| Pipeline end-to-end | ~4.5s confirmed latency, sub-second pre-alert |

See `scripts/bench_api.py` to reproduce.

---

## Configuration (`.env`)

```
MINIMAX_API_KEY=sk-...           # required
MINIMAX_BASE_URL=https://api.minimaxi.com/v1/text/chatcompletion_v2

HOST=0.0.0.0
PORT=8000

WORKERS=4                        # parallel M3 calls
WINDOW_SECONDS=1.5               # analysis window span
TARGET_FPS=8                     # ingest fps to the gate
FAST_MAX_FRAMES=8
FAST_RESOLUTION=448x252
ESCALATE_MAX_FRAMES=20
ESCALATE_RESOLUTION=672x378

BP_L1=4                          # backpressure thresholds (queue depth)
BP_L2=8
BP_L3=12

REQUEST_TIMEOUT_S=20
MAX_RETRY=2
```

---

## HTTP / WebSocket API

The service exposes two parallel surfaces:

- **Legacy `/sessions` + `/ws/sessions/*`** — unauthenticated, for the demo UI
- **`/v1/*`** — bearer-authenticated public API for external apps

Full reference: **[docs/API.md](docs/API.md)** (HTTP, WS, token scopes, admin
endpoints, error codes) and **[docs/KCP_WIRE.md](docs/KCP_WIRE.md)** (UDP/KCP
byte-level protocol for mobile clients).

Quick summary:

```
# Legacy (demo UI — no auth)
GET  /healthz
POST /sessions              {source, options?} → {session_id, observe_ws, stats_url}
GET  /sessions              list
GET  /sessions/{id}
GET  /sessions/{id}/events?since=
GET  /sessions/{id}/stats
DELETE /sessions/{id}
GET  /                      # demo UI
WS   /ws/sessions/{id}/observe

# /v1 — bearer auth (mint via: python -m rtva.auth mint <label> --scopes ...)
POST   /v1/streams                            {source, channel} → {session_id, ingest, observe}
GET    /v1/streams                            list visible sessions
GET    /v1/streams/{sid}                      session detail
DELETE /v1/streams/{sid}                      stop
POST   /v1/streams/{sid}/ingest/frame         multipart JPEG (channel=http)
WS     /v1/streams/{sid}/ingest?token=...     binary JPEG frames (channel=ws)
WS     /v1/streams/{sid}/observe?token=...    event fan-out (JSON)
GET    /v1/streams/{sid}/events?since=        event polling (observe scope)
GET    /v1/streams/{sid}/stats                pipeline stats
GET    /v1/admin/tokens                       list (admin scope)
POST   /v1/admin/tokens                       mint new token (admin scope)
DELETE /v1/admin/tokens/{token}               revoke (admin scope)

# KCP — UDP/8096, byte-compatible with kcp-go / l42111996/kcp
# (handshake + frame wire format: docs/KCP_WIRE.md)
```

### Choosing a transport

- **HTTP single-frame ingest**: server-to-server relay, shell scripts, debug
  tools. Simple but slow (~100-500 req/s per client).
- **WebSocket `/ingest`**: browser/JS clients, low-latency web dashboards.
  One TCP connection, binary frames, ~1k fps ceiling.
- **KCP over UDP**: mobile, edge boxes, jitter-sensitive links. Drops
  recover with `kcp-go` semantics; conv-id is fixed at 1 (routing by
  `session_id` inside `hello`). Configurable loss recovery.

WS /observe and KCP outbound emit the **same** `event.provisional` /
`event.confirmed` / `narrative` / `stats` JSON, so a client can switch
transports without changing parsers.

---

## Important caveat about MiniMax-M3

M3 is a **reasoning model**. Without disabling thinking, its reasoning tokens
eat the output budget and `content` returns empty strings. Pass
`{"thinking": {"type": "disabled"}}` — that is the ONLY spelling that works
(`reasoning_effort` and `chat_template_kwargs` are ignored, verified by
benchmark).

JSON reliability is good when the system prompt contains an explicit schema and
the instruction "ONLY minified JSON, no markdown." `response_format` is also
accepted. The client adds a repair + retry chain as belt-and-suspenders.

---

## ⚠️ Key security

You pasted an API key in plaintext during planning. Rotate it in MiniMax's
console before any other use. This repo only reads the key from `.env`
(gitignored).

---

## References

- StreamingVLM (MIT-HAN-Lab) — https://github.com/mit-han-lab/streaming-vlm
- Microsoft Mage — https://github.com/microsoft/Mage
- Awesome Streaming Video Understanding — https://github.com/sotayang/Awesome-Streaming-Video-Understanding

## License

MIT.
