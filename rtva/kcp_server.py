"""KCP server — UDP listener with application-level framing for the RTVA
external-app channel.

Wire compatibility: the KCP segment layer is the standard 24-byte header
provided by the `kcp` PyPI package (Cython binding to ikcp.c; byte-identical to
xtaci/kcp-go, l42111996/kcp). On top of that, this module implements an
application framing protocol:

    [u32 BE len][JSON header][optional payload bytes]

Message types:

    {"type":"hello","token":"rtva_...","session_id":"<uuid>"}
        → {"type":"hello_ok","send_wnd":N,"recv_wnd":N,"mtu":N}
        → {"type":"hello_err","reason":"..."}   followed by disconnect

    {"type":"frame","pts_ms":<u64>,"w":<u16>,"h":<u16>,"codec":"jpeg"}
        followed by `len(payload)` JPEG bytes
        → forwarded into the session's pipeline

    {"type":"ping","t":<u64>}
        → {"type":"pong","t":<u64>}

    {"type":"close","reason":"..."}
        → server closes the peer

Outgoing analysis events (event.provisional / event.confirmed / narrative /
stats) are framed identically and pushed to the peer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from io import BytesIO
from typing import Awaitable, Callable, Optional

import numpy as np
from PIL import Image

from .auth import verify_bearer
from .sessions import manager
from .config import get_settings

import kcp  # type: ignore[import-not-found]


log = logging.getLogger("rtva.kcp")

LEN_FMT = struct.Struct(">I")  # u32 BE
HELLO_TIMEOUT_S = 5.0
IDLE_TIMEOUT_S = 600.0  # 10 min — clients reconnect to recover
UPDATE_INTERVAL_MS = 10


# ============================================================================
# Frame parsing (pull one length-prefixed message off the byte stream)
# ============================================================================


class Framer:
    """Buffers incoming KCP bytes and yields complete (header, payload) pairs.

    A peer may send multiple frames per KCP packet, and KCP may deliver one
    logical message across multiple packets. Framer handles both boundaries.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._expected: Optional[int] = None

    def feed(self, data: bytes) -> list[tuple[dict, bytes]]:
        """Append data and return any complete messages decoded so far."""
        out: list[tuple[dict, bytes]] = []
        self._buf.extend(data)
        while True:
            if self._expected is None:
                if len(self._buf) < LEN_FMT.size:
                    break
                self._expected = LEN_FMT.unpack_from(self._buf, 0)[0]
                del self._buf[:LEN_FMT.size]
                if self._expected == 0:
                    self._expected = None
                    continue
            if len(self._buf) < self._expected:
                break
            msg = bytes(self._buf[:self._expected])
            del self._buf[:self._expected]
            self._expected = None
            try:
                hdr = json.loads(msg)
            except json.JSONDecodeError:
                continue
            payload_len = int(hdr.get("payload_len", 0))
            if payload_len:
                # next len-prefixed block is the payload
                if len(self._buf) < LEN_FMT.size + payload_len:
                    # wait for payload — push back
                    self._buf = msg_with_len(msg) + self._buf  # crude restore
                    self._expected = None
                    break
                del self._buf[:LEN_FMT.size]
                payload = bytes(self._buf[:payload_len])
                del self._buf[:payload_len]
            else:
                payload = b""
            out.append((hdr, payload))
        return out


def msg_with_len(data: bytes) -> bytes:
    return LEN_FMT.pack(len(data)) + data


def encode_message(header: dict, payload: bytes = b"") -> bytes:
    """Serialize one outbound message."""
    if payload:
        header = {**header, "payload_len": len(payload)}
    return msg_with_len(json.dumps(header).encode()) + (msg_with_len(payload) if payload else b"")


# ============================================================================
# Per-peer state
# ============================================================================


class KcpPeer:
    """One client (host,port) over KCP."""

    def __init__(self, server: "KcpServer", conn) -> None:
        self._server = server
        self._conn = conn
        self._framer = Framer()
        self._hello_done = False
        self._session_id: Optional[str] = None
        self._token_label: str = ""
        self._last_active = time.monotonic()
        self._frame_count = 0
        self.peer_addr = f"{conn.address}:{conn.port}"

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    @property
    def is_helloed(self) -> bool:
        return self._hello_done

    def send(self, header: dict, payload: bytes = b"") -> None:
        """Encode and enqueue an outbound message."""
        self._conn.enqueue(encode_message(header, payload))

    def close(self) -> None:
        """Force-close the peer."""
        try:
            self._conn.enqueue(encode_message({"type": "bye"}))
        except Exception:
            pass
        self._server._peers.pop(self.peer_addr, None)

    async def handle_data(self, data: bytes) -> None:
        """Process a fully-reassembled message from the client."""
        self._last_active = time.monotonic()
        for hdr, payload in self._framer.feed(data):
            await self._dispatch(hdr, payload)

    async def _dispatch(self, hdr: dict, payload: bytes) -> None:
        kind = hdr.get("type")
        if kind == "hello":
            await self._on_hello(hdr)
        elif kind == "frame":
            await self._on_frame(hdr, payload)
        elif kind == "ping":
            self.send({"type": "pong", "t": hdr.get("t", 0)})
        elif kind == "close":
            self.close()
        else:
            log.warning("kcp peer %s: unknown msg type %r", self.peer_addr, kind)

    async def _on_hello(self, hdr: dict) -> None:
        token = hdr.get("token", "")
        sid = hdr.get("session_id", "")
        rec = verify_bearer(token)
        if not rec or rec.disabled:
            self.send({"type": "hello_err", "reason": "invalid token"})
            self.close()
            return
        # check session exists
        record = manager.get(sid)
        if not record:
            self.send({"type": "hello_err", "reason": "session not found"})
            self.close()
            return
        # ownership check (admin bypasses)
        if "admin" not in rec.scopes and record.owner_token_label != rec.label:
            self.send({"type": "hello_err", "reason": "not your session"})
            self.close()
            return
        if record.channel != "kcp":
            self.send({"type": "hello_err", "reason": f"session is channel={record.channel}"})
            self.close()
            return
        self._session_id = sid
        self._token_label = rec.label
        self._hello_done = True
        # bind outgoing events to this peer
        record.ingestor.bind_peer(self)
        self.send({
            "type": "hello_ok",
            "send_wnd": 128,
            "recv_wnd": 128,
            "mtu": 1400,
            "session_id": sid,
        })
        log.info("kcp peer %s: hello OK, sid=%s", self.peer_addr, sid)

    async def _on_frame(self, hdr: dict, payload: bytes) -> None:
        if not self._hello_done:
            return
        sid = self._session_id
        record = manager.get(sid)
        if not record or not record.ingestor:
            return
        pts_ms = int(hdr.get("pts_ms", 0))
        try:
            rgb = await asyncio.to_thread(_jpeg_to_rgb, payload)
        except Exception as exc:
            log.warning("kcp peer %s: bad jpeg: %r", self.peer_addr, exc)
            return
        await record.ingestor.feed(pts_ms=pts_ms, rgb=rgb)
        self._frame_count += 1


def _jpeg_to_rgb(jpeg: bytes) -> np.ndarray:
    img = Image.open(BytesIO(jpeg)).convert("RGB")
    return np.asarray(img, dtype=np.uint8)


# ============================================================================
# Server
# ============================================================================


class KcpServer:
    """UDP KCP server for RTVA external-app channel.

    Manages a single `kcp.KCPServerAsync` instance + per-peer KcpPeer objects +
    a periodic update loop. Wire conv_id is a fixed constant (KCP wire uses 1
    by default); our application layer routes by session_id inside the hello
    message.
    """

    WIRE_CONV = 1  # KCP wire conversation ID (clients connect with this value)

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._inner = kcp.KCPServerAsync(
            address=host,
            port=port,
            conv_id=self.WIRE_CONV,
            delay=UPDATE_INTERVAL_MS,
            no_delay=True,
            resend_count=2,
            no_congestion_control=False,
            send_window_size=128,
            receive_window_size=128,
        )
        self._peers: dict[str, KcpPeer] = {}
        self._closed = False
        self._listen_task: Optional[asyncio.Task] = None
        # hook data handler
        self._inner.on_data(self._on_data)

    async def start(self) -> None:
        self._listen_task = asyncio.create_task(self._inner.listen())
        log.info("kcp server listening on udp://%s:%d (conv=%d)",
                 self.host, self.port, self.WIRE_CONV)

    async def stop(self) -> None:
        self._closed = True
        self._inner.stop()
        if self._listen_task:
            try:
                await asyncio.wait_for(self._listen_task, timeout=2)
            except (asyncio.TimeoutError, Exception):
                self._listen_task.cancel()
        for peer in list(self._peers.values()):
            peer.close()

    async def _on_data(self, conn, data: bytes) -> None:
        addr = conn.address_tuple if hasattr(conn, "address_tuple") else (conn.address, conn.port)
        key = f"{addr[0]}:{addr[1]}"
        peer = self._peers.get(key)
        if peer is None:
            peer = KcpPeer(self, conn)
            self._peers[key] = peer
        try:
            await peer.handle_data(data)
        except Exception as exc:
            log.warning("kcp peer %s handle_data error: %r", key, exc)


# Module-level singleton, started by server.py lifespan
_server: Optional[KcpServer] = None


async def start_server(host: Optional[str] = None,
                       port: Optional[int] = None) -> Optional[KcpServer]:
    """Start the KCP server on the configured port. Idempotent."""
    global _server
    settings = get_settings()
    if not settings.kcp_enabled:
        log.info("kcp disabled by config; not starting")
        return None
    if _server is not None:
        return _server
    _server = KcpServer(host or settings.kcp_host,
                       port if port is not None else settings.kcp_port)
    await _server.start()
    return _server


async def stop_server() -> None:
    global _server
    if _server is not None:
        await _server.stop()
        _server = None


def get_server() -> Optional[KcpServer]:
    return _server