"""Turns the camera's proprietary tunneled feed into a plain local stream.

Architecture:

    Dvr163Client (protocol.py, pure Python)
        --[raw H.265 Annex-B]--> ffmpeg stdin
        --[raw AAC ADTS]-------> ffmpeg via a named pipe
    ffmpeg (remux only, no transcode) --[MPEG-TS]--> ffmpeg's own stdout
        --[this class reads it]--> fan-out server --[MPEG-TS]--> Home Assistant

Home Assistant's own `stream` integration opens this fan-out server's URL as
this camera's stream_source.

Two things this deliberately does NOT do, both tried first and abandoned:

1. Hand ffmpeg's own `-listen 1` HTTP output URL straight to Home
   Assistant. `-listen 1` accepts exactly ONE client for the process's
   entire lifetime and doesn't bind until ffmpeg has finished probing both
   inputs -- for a real live/piped source (as opposed to an instant
   non-realtime synthetic test input) that reliably took 30s+ and
   sometimes never happened at all within any reasonable bound. Publishing
   the URL before that point gets Home Assistant a "Connection refused"
   (confirmed against a real instance).

2. Have this class connect to that `-listen` socket itself instead (safe
   in principle -- no external consumer can race a deliberate single
   client for the one accept slot, unlike letting Home Assistant connect
   directly). This sidesteps the *race* but not the underlying slowness:
   still bounded by the same unpredictable ffmpeg-internal delay before
   the socket even opens.

The actual fix: don't use a socket for this hop at all. ffmpeg writes its
MPEG-TS output to its own stdout (`pipe:1`) instead -- a subprocess pipe
has no separate listen/accept handshake to wait on, it's available the
instant the process starts, exactly like the stdin pipe already used to
feed it video. This class reads that pipe directly and re-serves the bytes
via its own persistent asyncio server, which accepts any number of Home
Assistant (re)connections independently of camera/ffmpeg restarts
underneath it. The fan-out server starts once, immediately, when the
config entry loads -- stream_url is available right away and never
depends on camera/ffmpeg timing at all.

(The single-shot-upstream/many-downstream-clients split itself is the same
fix used building the standalone dashboard for this camera -- see the
`oossxx` repo's NOTES.md for that original writeup. Only the specific
"how do we get bytes out of ffmpeg" mechanism changed here, after finding
`-listen` too slow to open for a real camera source.)
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import tempfile

from homeassistant.core import HomeAssistant

from .const import PT_AUDIO_AAC, PT_VIDEO_H265
from .protocol import Dvr163Client, Dvr163ProtocolError

_LOGGER = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1
_MAX_BACKOFF = 30
# Generous: real-world startup (camera reconnect + ffmpeg dual-input probe)
# has been observed taking anywhere from ~6s to 45s+. This bounds how long
# a stalled attempt (no output at all, no error either) is tolerated
# before the supervisor loop abandons it and retries -- see the long
# comment at the read loop below.
_STALL_TIMEOUT = 45


class StreamManager:
    """Owns the fan-out server + supervised upstream pipeline for one camera."""

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
        self._server: asyncio.base_events.Server | None = None
        self._clients: set[asyncio.StreamWriter] = set()
        self._stream_url: str | None = None

    @property
    def stream_url(self) -> str | None:
        """The local fan-out URL to hand to HA's stream_source.

        Available as soon as the fan-out server itself is listening --
        this does NOT wait on the camera/ffmpeg pipeline, which connects
        and reconnects independently in the background. Home Assistant can
        connect immediately; it'll just see no data until the upstream
        pipeline catches up, which is a much softer failure mode than the
        "Connection refused" the old single-shot-ffmpeg design produced.
        """
        return self._stream_url

    def start(self) -> None:
        if self._task is None:
            self._task = self._hass.async_create_background_task(
                self._main(), name="dvr163_stream_supervisor"
            )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for writer in list(self._clients):
            writer.close()
        self._clients.clear()
        self._stream_url = None

    async def _main(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, "127.0.0.1", 0
        )
        port = self._server.sockets[0].getsockname()[1]
        self._stream_url = f"http://127.0.0.1:{port}/stream.ts"
        async with self._server:
            await self._supervisor_loop()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve one Home Assistant (re)connection with a live MPEG-TS tail.

        No history/buffering -- a new client just starts receiving
        whatever arrives from the upstream pipeline from this point on,
        same as any other live camera feed.
        """
        try:
            # Drain the HTTP request line + headers; we don't need
            # anything from them, just enough to know the request finished.
            while True:
                line = await reader.readline()
                if not line or line == b"\r\n":
                    break
            writer.write(
                b"HTTP/1.0 200 OK\r\n"
                b"Content-Type: video/mp2t\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Connection: close\r\n\r\n"
            )
            await writer.drain()
            self._clients.add(writer)
            await reader.read()  # blocks until the client disconnects
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self._clients.discard(writer)
            with contextlib.suppress(Exception):
                writer.close()

    async def _broadcast(self, data: bytes) -> None:
        # Iterate a snapshot, not the live set: _handle_client's finally
        # block discards from self._clients concurrently on disconnect,
        # and mutating a set while iterating it directly raises
        # RuntimeError ("Set changed size during iteration") -- which,
        # uncaught, took down the entire upstream pipeline on every client
        # disconnect (confirmed by testing: a second client's connection
        # would hang with no data, because the pipeline had just been
        # killed and was mid-restart).
        dead: list[asyncio.StreamWriter] = []
        for writer in list(self._clients):
            try:
                writer.write(data)
                await writer.drain()
            except (OSError, RuntimeError):
                dead.append(writer)
        for writer in dead:
            self._clients.discard(writer)

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
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)

    async def _run_once(self) -> None:
        loop = asyncio.get_running_loop()
        tmp_dir = tempfile.mkdtemp(prefix="dvr163_")
        fifo_path = os.path.join(tmp_dir, "audio.fifo")
        os.mkfifo(fifo_path)

        proc: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task | None = None
        audio_open_task: asyncio.Task | None = None
        feed_task: asyncio.Task | None = None
        audio_file = None
        try:
            # ffmpeg writes MPEG-TS to its own stdout (pipe:1) rather than
            # an HTTP -listen socket. Tried the -listen approach first --
            # even connecting to it ourselves (see the module docstring),
            # it could take 30s+ for ffmpeg to actually open that socket
            # for a real live/piped source (vs. an instant, non-realtime
            # synthetic test input), and sometimes didn't within any
            # reasonable bound at all. A subprocess's stdout pipe has none
            # of that: it's available the instant the process starts, no
            # separate listen/accept handshake to wait on -- exactly the
            # same piping pattern already used for feeding it video on
            # stdin, just in the other direction.
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-y",
                # -use_wallclock_as_timestamps: our raw piped elementary
                # streams carry no timing info at all (RTP headers,
                # including timestamps, were already stripped in
                # protocol.py). The RTSP muxer tolerates that with just a
                # deprecation warning, but MPEG-TS does not -- confirmed by
                # testing: without this, ffmpeg fatally errors ("first pts
                # and dts value must be set") and dies after ~0.4s of
                # output. Wall-clock timestamping (i.e. "stamp each packet
                # with when ffmpeg actually received it") is the standard
                # fix for exactly this class of input.
                "-thread_queue_size", "1024", "-use_wallclock_as_timestamps", "1",
                "-f", "hevc", "-i", "pipe:0",
                "-thread_queue_size", "1024", "-use_wallclock_as_timestamps", "1",
                "-f", "aac", "-i", fifo_path,
                "-c:v", "copy",
                "-c:a", "aac", "-b:a", "32k", "-bsf:a", "aac_adtstoasc",
                "-f", "mpegts", "pipe:1",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(self._drain_stderr(proc))
            audio_open_task = asyncio.create_task(self._open_fifo_writer(fifo_path))

            client = Dvr163Client(
                self._host, self._port, self._path, self._username, self._password
            )

            async def feed_ffmpeg() -> None:
                nonlocal audio_file
                async for pt, payload in client.stream():
                    if proc.returncode is not None:
                        raise Dvr163ProtocolError(
                            f"ffmpeg exited early (code {proc.returncode})"
                        )
                    if pt == PT_VIDEO_H265:
                        proc.stdin.write(payload)
                        await proc.stdin.drain()
                    elif pt == PT_AUDIO_AAC:
                        if audio_file is None and audio_open_task.done():
                            audio_file = audio_open_task.result()
                        if audio_file is not None:
                            await loop.run_in_executor(None, audio_file.write, payload)

            feed_task = asyncio.create_task(feed_ffmpeg())

            # ffmpeg's own dual-input probing (raw piped video + FIFO
            # audio, both live/variable-rate, no container-level timing
            # hints) has turned out to be quite variable in testing --
            # anywhere from ~6s to indefinitely stalled waiting on the
            # audio side, with no error raised while stuck. Bounding every
            # read with a stall timeout means a stuck attempt always gets
            # abandoned and retried by the supervisor loop instead of
            # potentially hanging forever.
            while True:
                if feed_task.done():
                    feed_task.result()  # re-raise if it failed
                    raise Dvr163ProtocolError("camera feed ended")
                try:
                    chunk = await asyncio.wait_for(
                        proc.stdout.read(65536), timeout=_STALL_TIMEOUT
                    )
                except asyncio.TimeoutError as err:
                    raise Dvr163ProtocolError(
                        f"no ffmpeg output for {_STALL_TIMEOUT}s, treating as stalled"
                    ) from err
                if not chunk:
                    raise Dvr163ProtocolError("ffmpeg output ended")
                await self._broadcast(chunk)
        except (BrokenPipeError, ConnectionResetError) as err:
            raise Dvr163ProtocolError(f"ffmpeg pipe broke: {err}") from err
        finally:
            if feed_task is not None:
                feed_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await feed_task
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

        A plain blocking open() on a FIFO waits for a reader and can't be
        cancelled if handed to a thread executor (Future.cancel() doesn't
        stop a thread already blocked in a syscall) -- confirmed by testing
        it leaks the thread forever if torn down before ffmpeg opens its
        end. Non-blocking open + retry is properly cancellable instead.
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
