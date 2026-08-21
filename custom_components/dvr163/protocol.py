"""Async client for this camera family's HTTP-tunneled RTSP video/audio feed.

Reverse-engineered protocol -- see README for the full writeup. Summary:

A plain HTTP GET to /livestream/N (Basic Auth) gets a 200 response that
never closes. After the headers there's a short text preamble (an RTSP
SETUP-style response with embedded SDP), then a continuous sequence of
frames, each:

    '$' (0x24) + channel(1 byte) + reserved(2 bytes) + length(4-byte BE)
    + <length> bytes of a standard 12(+)-byte-header RTP packet

This is NOT the standard 4-byte RFC 2326 interleave header -- it's 8 bytes,
confirmed by walking real captures with zero desyncs. All tracks arrive on
channel 0; video vs audio is distinguished by the RTP payload type (97 =
H.265, 104 = AAC on every unit tested so far).

Neither track uses real RTP payload framing on top of that: video payloads
already start with their own Annex-B start code, and audio payloads already
start with a complete 7-byte ADTS header. So depacketization is just: strip
the RTP header, pass the rest through.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import struct
from collections.abc import AsyncIterator

from .const import PT_AUDIO_AAC, PT_VIDEO_H265

_LOGGER = logging.getLogger(__name__)

_HEADER_SIZE = 8
_CONNECT_TIMEOUT = 10
_RECV_CHUNK = 65536


class Dvr163ProtocolError(Exception):
    """Raised on any unrecoverable problem with the video/audio session."""


def _strip_rtp_header(rtp_packet: bytes) -> bytes | None:
    if len(rtp_packet) < 12:
        return None
    cc = rtp_packet[0] & 0x0F
    header_len = 12 + cc * 4
    if len(rtp_packet) < header_len:
        return None
    return rtp_packet[header_len:]


class Dvr163Client:
    """One connection to /livestream/N, yielding depacketized (pt, payload) frames."""

    def __init__(self, host: str, port: int, path: str, username: str, password: str) -> None:
        self._host = host
        self._port = port
        self._path = path
        self._auth = base64.b64encode(f"{username}:{password}".encode()).decode()

    async def stream(self) -> AsyncIterator[tuple[int, bytes]]:
        """Connect once and yield frames until the camera closes the connection.

        Raises Dvr163ProtocolError on any handshake failure. A clean or
        unclean disconnect after the handshake just ends iteration --
        callers are expected to reconnect (see stream.py's supervisor loop).
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), timeout=_CONNECT_TIMEOUT
        )
        try:
            request = (
                f"GET {self._path} HTTP/1.1\r\n"
                f"Host: {self._host}\r\n"
                f"Authorization: Basic {self._auth}\r\n"
                f"User-Agent: ha-dvr163/0.1\r\n"
                f"Connection: keep-alive\r\n"
                f"\r\n"
            )
            writer.write(request.encode())
            await writer.drain()

            buf = await self._read_until(reader, b"\r\n\r\n", buf=b"")
            header_end = buf.index(b"\r\n\r\n") + 4
            status_line = buf[:header_end].decode(errors="replace").splitlines()[0]
            if " 200 " not in status_line:
                raise Dvr163ProtocolError(f"non-200 response: {status_line}")

            leftover = buf[header_end:]
            leftover = await self._read_until(reader, b"$", buf=leftover)
            dollar_idx = leftover.index(b"$")
            _LOGGER.debug("session preamble: %s", leftover[:dollar_idx].decode(errors="replace"))
            leftover = leftover[dollar_idx:]

            while True:
                while len(leftover) < _HEADER_SIZE:
                    chunk = await reader.read(_RECV_CHUNK)
                    if not chunk:
                        return
                    leftover += chunk

                if leftover[0] != 0x24:
                    idx = leftover.find(b"$", 1)
                    leftover = leftover[idx:] if idx != -1 else b""
                    continue

                length = struct.unpack(">I", leftover[4:8])[0]
                while len(leftover) < _HEADER_SIZE + length:
                    chunk = await reader.read(_RECV_CHUNK)
                    if not chunk:
                        return
                    leftover += chunk

                rtp_packet = leftover[_HEADER_SIZE : _HEADER_SIZE + length]
                leftover = leftover[_HEADER_SIZE + length :]

                if len(rtp_packet) < 2:
                    continue
                pt = rtp_packet[1] & 0x7F
                if pt not in (PT_VIDEO_H265, PT_AUDIO_AAC):
                    continue
                payload = _strip_rtp_header(rtp_packet)
                if payload:
                    yield pt, payload
        finally:
            writer.close()

    @staticmethod
    async def _read_until(reader: asyncio.StreamReader, marker: bytes, buf: bytes) -> bytes:
        while marker not in buf:
            chunk = await reader.read(_RECV_CHUNK)
            if not chunk:
                raise Dvr163ProtocolError("connection closed before handshake completed")
            buf += chunk
        return buf
