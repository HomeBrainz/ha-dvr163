"""Turns the camera's proprietary tunneled feed into a plain local stream.

Runs a supervised background pipeline per camera:

    Dvr163Client (protocol.py, pure Python)
        --[raw H.265 Annex-B]--> ffmpeg stdin
        --[raw AAC ADTS]-------> ffmpeg via a named pipe
    ffmpeg (remux only, no transcode) --[MPEG-TS]--> local http listener

Home Assistant's own `stream` integration then opens that local
http://127.0.0.1:<port>/stream.ts URL as this camera's stream_source, and
handles all the actual HLS/fan-out-to-multiple-viewers work itself -- this
pipeline only ever has to serve that one internal consumer.

ffmpeg's `-listen 1` http output accepts exactly one client for the process
lifetime, then exits. That's fine here since HA's stream integration is the
only client, but it does mean the pipeline needs to be relaunched whenever
it (or the upstream camera connection) drops -- handled by the supervisor
loop below, same restart-on-unhealthy approach validated during protocol
research (see README).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import socket
import tempfile

from homeassistant.core import HomeAssistant

from .const import PT_AUDIO_AAC, PT_VIDEO_H265
from .protocol import Dvr163Client, Dvr163ProtocolError

_LOGGER = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1
_MAX_BACKOFF = 30


def _free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class StreamManager:
    """Owns one supervised ffmpeg remux pipeline for one camera config entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        port: int,
        path: str,
        username: str,
        password: str,
    ) -> None:
        self._hass = hass
        self._host = host
        self._port = port
        self._path = path
        self._username = username
        self._password = password
        self._task: asyncio.Task | None = None
        self._stream_url: str | None = None

    @property
    def stream_url(self) -> str | None:
        """The local URL to hand to HA's stream_source, or None if not (yet) up."""
        return self._stream_url

    def start(self) -> None:
        if self._task is None:
            self._task = self._hass.async_create_background_task(
                self._supervisor_loop(), name="dvr163_stream_supervisor"
            )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._stream_url = None

    async def _supervisor_loop(self) -> None:
        backoff = _INITIAL_BACKOFF
        while True:
            try:
                await self._run_once()
                backoff = _INITIAL_BACKOFF
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - any failure just triggers a retry
                _LOGGER.warning(
                    "%s: stream pipeline stopped (%s), retrying in %ss", self._host, err, backoff
                )
            self._stream_url = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)

    async def _run_once(self) -> None:
        loop = asyncio.get_running_loop()
        tmp_dir = tempfile.mkdtemp(prefix="dvr163_")
        fifo_path = os.path.join(tmp_dir, "audio.fifo")
        os.mkfifo(fifo_path)
        local_port = _free_local_port()

        proc: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task | None = None
        audio_open_task: asyncio.Task | None = None
        audio_file = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-y",
                "-thread_queue_size", "1024", "-f", "hevc", "-i", "pipe:0",
                "-thread_queue_size", "1024", "-f", "aac", "-i", fifo_path,
                "-c:v", "copy",
                "-c:a", "aac", "-b:a", "32k", "-bsf:a", "aac_adtstoasc",
                "-f", "mpegts", "-listen", "1", f"http://127.0.0.1:{local_port}/stream.ts",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(self._drain_stderr(proc))
            # Opening a FIFO for writing blocks until a reader connects. A
            # bare blocking open() handed to a thread executor is NOT
            # cleanly cancellable -- Future.cancel() doesn't stop a thread
            # already inside a blocking syscall, so if we tear down this
            # pipeline before ffmpeg gets around to opening its end (very
            # possible: ffmpeg can take several seconds to probe stdin
            # before it even looks at its second input), that thread leaks
            # forever. Confirmed by testing: it's still stuck 300s later.
            # A real asyncio Task wrapping a non-blocking-open retry loop
            # doesn't have this problem -- cancellation is delivered at the
            # next `await asyncio.sleep()`, which actually interrupts it.
            audio_open_task = asyncio.create_task(self._open_fifo_writer(fifo_path))

            client = Dvr163Client(
                self._host, self._port, self._path, self._username, self._password
            )
            candidate_url = f"http://127.0.0.1:{local_port}/stream.ts"

            async for pt, payload in client.stream():
                if proc.returncode is not None:
                    raise Dvr163ProtocolError(f"ffmpeg exited early (code {proc.returncode})")
                if pt == PT_VIDEO_H265:
                    proc.stdin.write(payload)
                    await proc.stdin.drain()
                    # Only publish stream_url once we know video is
                    # actually flowing into ffmpeg -- publishing it
                    # immediately after spawn (before ffmpeg has even
                    # probed the input) hands HA a URL that isn't ready
                    # yet.
                    if self._stream_url is None:
                        self._stream_url = candidate_url
                elif pt == PT_AUDIO_AAC:
                    if audio_file is None and audio_open_task.done():
                        audio_file = audio_open_task.result()
                    if audio_file is not None:
                        await loop.run_in_executor(None, audio_file.write, payload)
        except (BrokenPipeError, ConnectionResetError) as err:
            raise Dvr163ProtocolError(f"ffmpeg pipe broke: {err}") from err
        finally:
            self._stream_url = None
            if stderr_task is not None:
                stderr_task.cancel()
            if audio_file is not None:
                audio_file.close()
            elif audio_open_task is not None:
                audio_open_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await audio_open_task
            if proc is not None and proc.returncode is None:
                proc.kill()
                await proc.wait()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @staticmethod
    async def _open_fifo_writer(path: str):
        """Open a FIFO for writing, without an uncancellable blocking call.

        See the long comment at the call site for why this can't just be
        `loop.run_in_executor(None, open, path, "wb")`.
        """
        while True:
            try:
                fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError:
                await asyncio.sleep(0.2)
                continue
            os.set_blocking(fd, True)  # normal blocking writes from here on
            return os.fdopen(fd, "wb")

    @staticmethod
    async def _drain_stderr(proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for line in proc.stderr:
            _LOGGER.debug("ffmpeg: %s", line.decode(errors="replace").rstrip())
