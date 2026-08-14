# RTVA KCP Wire Protocol

> Companion document to **[API.md](API.md)**. Required reading for anyone
> integrating an external app (Android, iOS, embedded) with the UDP/KCP
> video channel.

The RTVA service listens on `udp://server:8096` for KCP connections. The
KCP segment layer is byte-identical to [`xtaci/kcp-go`](https://github.com/xtaci/libkcp-go)
and [`l42111996/kcp`](https://github.com/l42111996/kcp) — you can drop in
either library as-is.

---

## 1. Layering

```
┌──────────────────────────────────────────────────────────┐
│  Application framing  [len][hdr][payload]                 │  ← this doc, §3
├──────────────────────────────────────────────────────────┤
│  KCP segment          24-byte header + payload           │  ← ikcp.c / kcp-go, §2
├──────────────────────────────────────────────────────────┤
│  UDP datagram         8-byte header + KCP segment        │  ← kernel
└──────────────────────────────────────────────────────────┘
```

The application layer runs **on top of** KCP. KCP is just a reliable byte
stream, so the app adds its own length-prefixed framing.

---

## 2. KCP segment header (24 bytes)

Reference: [`ikcp.c`](https://github.com/xtaci/libkcp/blob/master/ikcp.c)
(`ikcp_encode_seg`). All fields little-endian.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         conv (u32)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          cmd (u8)            |          frg (u8)              |
+-------------------------------+-------------------------------+
|         wnd (u16)             |                                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
+                          ts (u32)                             +
+                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         sn (u32)                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         una (u32)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         len (u32)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
/                        payload (len bytes)                    /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field | Type | Notes |
| --- | --- | --- |
| `conv` | u32 LE | conversation ID. **RTVA uses `1` for all sessions**; routing is by `session_id` inside the hello message. |
| `cmd` | u8 | `81` IKCP_CMD_PUSH, `82` IKCP_CMD_ACK, `83` IKCP_CMD_WASK, `84` IKCP_CMD_WINS |
| `frg` | u8 | fragment index; 0 means last fragment. RTVA's `kcp-go`-compatible MTU is 1400. |
| `wnd` | u16 | remaining receive window |
| `ts` | u32 | sending timestamp (ms) |
| `sn` | u32 | send sequence number |
| `una` | u32 | unacknowledged sequence number |
| `len` | u32 | length of payload (≤ MTU) |
| `payload` | bytes | opaque to KCP — RTVA uses application framing (§3) |

### Recommended KCP parameters (matching kcp-go defaults)

```c
nodelay   = 1;    // low-latency mode
interval  = 10;   // ms between update ticks
resend    = 2;    // fast retransmit trigger
nc        = 0;    // turn off congestion control for intra-LAN; leave on for WAN
sndwnd    = 128;
rcvwnd    = 128;
mtu       = 1400;
```

RTVA's server sets these in [`rtva/kcp_server.py`](../rtva/kcp_server.py)
via `kcp.KCPServerAsync(no_delay=True, update_interval=10, resend=2, ...)` —
the Python `kcp` package is a Cython binding to `ikcp.c` and exposes the
same wire format.

---

## 3. Application framing

KCP is a byte stream — it has no message boundaries. RTVA adds:

```
One frame:
  +-----------------+
  | len (u32 BE)    |
  +-----------------+
  | json header     |   (variable; exactly `len` bytes)
  +-----------------+
  | (optional)      |
  | +-------------+ |
  | | payload_len | |   (u32 BE)
  | +-------------+ |
  | | payload     | |   (variable; exactly payload_len bytes)
  | +-------------+ |
  +-----------------+
```

If `header.payload_len` (a JSON field, not the prefix) is set, the server
reads `payload_len` more bytes from the stream immediately after the JSON
header, prefixed by another `u32 BE`.

### Headers

All headers are JSON objects. The only required field is `type`.

| Direction | `type` | Fields | Notes |
| --- | --- | --- | --- |
| client → server | `hello` | `token`, `session_id`, `conv?` | Must be the first message |
| server → client | `hello_ok` | `send_wnd`, `recv_wnd`, `mtu`, `session_id` | |
| server → client | `hello_err` | `reason` | followed by `bye` + disconnect |
| client → server | `frame` | `pts_ms` (u64), `w` (u16), `h` (u16), `codec` ("jpeg") | followed by JPEG payload |
| client → server | `ping` | `t` (u64) | |
| server → client | `pong` | `t` (u64) | |
| client → server | `close` | `reason` | graceful shutdown |
| server → client | `bye` | — | server-initiated close |
| server → client | `event.provisional` | (see API.md §4) | analysis event |
| server → client | `event.confirmed` | (see API.md §4) | analysis event |
| server → client | `narrative` | (see API.md §4) | analysis summary |
| server → client | `stats` | (see API.md §4) | pipeline counters |
| server → client | `error` | `code`, `message` | server-side failure |

### Hello handshake

The first message a client sends **must** be `hello`:

```json
{
  "type":        "hello",
  "token":       "rtva_54ef...",
  "session_id":  "1cddc885-d9ae-49a5-820e-305d2ac713c6",
  "conv":        1
}
```

The `session_id` must come from a prior `POST /v1/streams` with
`channel="kcp"` — the server rejects hello if the session does not exist,
the token is invalid, or the token label does not own the session (admins
bypass ownership). The server then binds the KCP peer as the outbound
channel for that session's analysis events.

Server response is one of:

```json
{ "type": "hello_ok", "send_wnd": 128, "recv_wnd": 128, "mtu": 1400,
  "session_id": "1cddc885-d9ae-49a5-820e-305d2ac713c6" }
```

or

```json
{ "type": "hello_err", "reason": "session not found" }
```

followed by `{"type":"bye"}` and an immediate KCP close.

### Frame upload

```json
{ "type": "frame", "pts_ms": 1000, "w": 1280, "h": 720, "codec": "jpeg" }
```

followed by `payload_len` JPEG bytes. The server decodes the JPEG, feeds
the RGB array into the analysis pipeline, and replies asynchronously with
`event.*` messages on the same KCP connection.

### Pong / bye

`ping`/`pong` is a simple latency probe. The server always echoes back.
The server sends `bye` on graceful shutdown (e.g. session `DELETE` by
another client).

---

## 4. Conversation ID routing

RTVA's KCP server uses a **fixed wire conv id of `1`**. All sessions share
the same UDP listener, and the `hello` message's `session_id` field is
what routes frames to the right pipeline.

If you need per-session isolation at the KCP layer (some clients want it
for firewall reasons), the wire conv can be set to the truncated u32 hash
of the session id — see `sid_to_u32()` in `rtva/api_v1.py` — but the
**server always accepts conv=1** regardless. Pick whatever is convenient
on the client side.

---

## 5. Java / Kotlin client (Android)

Use [`l42111996/kcp`](https://github.com/l42111996/kcp), a Netty/Java port
that is byte-compatible with kcp-go. Wire format: identical.

```kotlin
// Gradle: implementation("com.github.l42111996:kcp:1.0-SNAPSHOT")

class RtvKcpClient(
    private val serverHost: String,
    private val serverPort: Int = 8096,
    private val token: String,
    private val sessionId: String,
) {
    private val kcp: Kcp = Kcp(1, /* out */ { bytes, off, len ->
        // send UDP packet to serverHost:serverPort
        udpSocket.send(DatagramPacket(bytes, off, len, serverHost, serverPort))
    }).also { it.noDelay(1, 10, 2, 0); it.wndSize(128, 128); it.mtu = 1400 }

    private val framer = RtvFramer()

    init {
        // start a flush loop: every 10 ms call kcp.update(ts)
        executor.scheduleAtFixedRate({
            kcp.update(System.currentTimeMillis())
            val sz = kcp.recv(byteArrayOf())
            // (read into a buffer)
        }, 0, 10, TimeUnit.MILLISECONDS)
    }

    suspend fun start() {
        // 1. write hello
        writeFramed("""{"type":"hello","token":${jsonStr(token)},"session_id":${jsonStr(sessionId)}}""")
    }

    suspend fun sendFrame(ptsMs: Long, jpeg: ByteArray) {
        writeFramed("""{"type":"frame","pts_ms":$ptsMs,"w":1280,"h":720,"codec":"jpeg"}""", jpeg)
    }

    private fun writeFramed(json: String, payload: ByteArray? = null) {
        val hdr = json.toByteArray(Charsets.UTF_8)
        val out = ByteArrayOutputStream()
        out.writeByteBuf(hdr.size); out.write(hdr)
        if (payload != null) {
            out.writeByteBuf(payload.size); out.write(payload)
        }
        kcp.send(out.toByteArray())
    }
}
```

The `RtvFramer` does the inverse — buffer inbound KCP bytes, pull off
`u32 BE` length-prefixed JSON + optional payload, parse, dispatch.

---

## 6. Reference Python sender

`examples/kcp_sender.py` ships with the repo:

```bash
pip install kcp pillow av httpx websockets numpy
python -m rtva.auth mint android-1 --scopes ingest,observe | grep ^rtva_ > /tmp/tok
python examples/kcp_sender.py --token "$(cat /tmp/tok)" --video test_videos/test60.mp4
```

It does everything a real client needs: opens the UDP socket, runs KCP,
sends `hello`, streams JPEG frames from a video file at 8 fps, prints
`event.*` messages it receives back. Useful as both a smoke test and a
template.

---

## 7. Operational notes

- **UDP NAT**: KCP does not survive NAT rebinding well. Keep the keepalive
  ping cadence ≥ 1 Hz or reconnect aggressively on send failure.
- **MTU**: 1400 is safe on most paths. If you have a captive portal, lower
  to 1200 or 512.
- **Encrypted payloads**: KCP itself does not encrypt (matches `kcp-go`
  default). For E2E confidentiality, run the link inside WireGuard or
  wrap the JPEG payload with your own AEAD (key out of band).
- **One session per UDP source port**: the server keys peers by
  `host:port`. If your client needs multiple sessions, open multiple UDP
  sockets — don't try to multiplex on a single source port.
- **Idle timeout**: server peers are reaped after 10 min of inactivity.
  Send a `ping` every 30 s to stay alive.