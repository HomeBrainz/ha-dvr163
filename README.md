# DVR163 IP Camera — Home Assistant integration

[![Add to HACS via My Home Assistant](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=HomeBrainz&repository=ha-dvr163&category=integration)

A self-contained [HACS](https://hacs.xyz/) custom integration for the family of
budget PTZ/IP cameras built on **HiSilicon Hi3510** silicon with a
**Tmezon / EseeCloud** (also rebranded as **IP Pro** / **VR Cam**) cloud/P2P
backend. These ship under dozens of unrelated storefront brand names —
OOSSXX/HonestView is the one this was reverse-engineered against, but nothing
here is brand-specific.

No RTSP, ONVIF, or cloud account required. This talks directly to the
camera's local HTTP port.

## Why this exists

This camera family advertises RTSP but frequently doesn't actually run an
RTSP service on the LAN. What *is* always on, locally, unauthenticated setup
required, is a single HTTP endpoint — `GET /livestream/11` — that tunnels a
live RTSP session (SDP + RTP) over one long-lived HTTP response, using a
custom 8-byte framing that isn't the standard RTSP interleave format. This
is the same protocol referenced (but unimplemented) in
[go2rtc issue #511](https://github.com/AlexxIT/go2rtc/issues/511),
"Adding Support for DVR163/EseeCloud" — hence the name.

This integration implements that protocol in pure Python, remuxes it
locally with `ffmpeg`, and hands Home Assistant's own `stream` component a
plain local MPEG-TS URL. No external bridge scripts, no separate RTSP
server, no cloud dependency — install via HACS, add the camera, done.

## Requirements

- `ffmpeg` available on the Home Assistant host (used for remux only — no
  transcoding, so this is cheap even on low-power hosts). Most Home
  Assistant installs already have it for other integrations.
- The camera's local HTTP port (usually 80) reachable from Home Assistant.
- Default credentials on this camera family are `admin` / *(blank
  password)*.

## What you get

- **Two camera entities per device — "Main" and "Sub"** — live video **and
  audio** for both, via Home Assistant's normal stream/HLS pipeline (H.265
  video is copied through as-is; audio is remuxed from AAC). This firmware
  family always exposes both a full-resolution main stream and a lower-
  resolution sub stream at fixed paths, so both are set up automatically —
  no need to pick just one at setup time. Use "Sub" wherever a lighter feed
  is enough (dashboard tiles, motion-triggered snapshots, constrained
  bandwidth/CPU) and "Main" for actual viewing.
- **Buttons** — PTZ (left/right/up/down/stop) and 4 presets.
- **Number** entities — hue, brightness, saturation, contrast.
- **Switch** entities — flip, mirror.

There is deliberately no still-image/snapshot entity — this firmware family
has no snapshot endpoint at all; Home Assistant derives a preview frame from
the live stream instead.

## Installation

1. HACS → Custom repositories → add this repo URL, category "Integration".
2. Install "DVR163 IP Camera", restart Home Assistant.
3. Settings → Devices & services → Add integration → "DVR163 IP Camera".
4. Enter the camera's IP, port (default 80), and username/password. Both
   stream entities are created automatically.

## Known firmware quirks this integration works around

- **`setimageattr` is all-or-nothing.** This firmware silently resets any
  image attribute *not* included in a given `setimageattr` call back to
  `127`. Every write from this integration always resends the full set
  (hue/brightness/saturation/contrast/scene/flip/mirror) rather than just
  the one value that changed.
- **ffmpeg needs to be handed a real container format, and the camera's raw
  feed carries no timing info at all** (RTP headers, including
  timestamps, are stripped well before ffmpeg ever sees the data — see
  `protocol.py`). MPEG-TS muxing fatally errors without timestamps
  ("first pts and dts value must be set"); this integration tells ffmpeg
  to stamp every packet by wall-clock arrival time instead
  (`-use_wallclock_as_timestamps 1`), which is the standard fix for a raw
  piped/live source like this one.
- **ffmpeg's own `-listen 1` HTTP output is a dead end for this use case.**
  It accepts exactly one client for the process's entire lifetime and
  doesn't bind until ffmpeg has finished probing *both* inputs — for a
  real live/piped camera source (as opposed to an instant synthetic test
  source) that reliably took 30s+ and sometimes didn't happen at all
  within any reasonable bound. Handing that URL straight to Home Assistant
  produces an intermittent "Connection refused" (confirmed against a real
  instance); even connecting to it internally first doesn't fix the
  underlying slowness, just the race. Instead, ffmpeg writes MPEG-TS to
  its own stdout pipe (available the instant the process starts, no
  listen/accept handshake at all), and this integration re-serves those
  bytes via its own persistent local server, which Home Assistant connects
  to. That server starts immediately when the integration loads and
  accepts any number of Home Assistant (re)connections independently of
  camera/ffmpeg restarts underneath it.
- **ffmpeg's default probing is needlessly slow for the video track, and
  the fix has to be scoped carefully.** ffmpeg's default `-probesize`/
  `-analyzeduration` are conservative (multiple MB / several seconds)
  because normally it doesn't know the codec ahead of time — we already do
  (`-f hevc`), so a much smaller probe size is set explicitly for the
  video input, cutting typical startup from tens of seconds down to
  ~0.3s. Applying the same reduction to the audio input made things
  *worse*, not better — audio here is very low-bitrate (~16kbps), so a
  "small" byte-based probe size takes far longer in wall-clock time to
  satisfy than for video, even though the number looks identical. Audio
  keeps ffmpeg's regular defaults; only video is tuned.
- **Both streams hitting the camera in the same instant measurably
  worsens startup contention** — confirmed by testing (a solo stream's
  ~2-8s startup became 90s+ with both starting together). The two stream
  pipelines are staggered a few seconds apart rather than started
  simultaneously. Some run-to-run variability remains under concurrent
  load even with staggering (roughly 2 of 3 attempts fast, 1 of 3 needing
  a retry, in testing) — likely inherent to how this camera's firmware
  handles concurrent connections rather than something fixable purely
  client-side. Every read from ffmpeg's output is bounded by a generous
  internal stall timeout (90s, sized for the dual-stream case), so a
  stuck attempt always gets abandoned and retried by the supervisor loop
  with exponential backoff rather than potentially hanging forever.
- **The audio FIFO writer needs unbuffered I/O, or ffmpeg's audio probe can
  starve indefinitely.** Python's `open()`/`os.fdopen()` default to
  buffered I/O — each `write()` call fills an internal userspace buffer
  (~a few KB) that is only flushed to the actual OS pipe once it's full.
  Audio frames here are tiny (a few hundred bytes each), so dozens of
  writes could accumulate in that buffer without a single byte physically
  reaching ffmpeg's end of the FIFO, while ffmpeg sits waiting on `Input
  #1` to probe. This reproduced reliably on Alpine/musl builds (e.g. HAOS)
  even though video probed and streamed fine — confirmed via a local
  Docker Alpine container replaying captured camera data through the exact
  same pipeline, isolating the stall to this one missing `buffering=0`.
  Fixed by opening the audio FIFO unbuffered, matching what the output
  FIFO reader already did for the same reason.

## Compatibility

Confirmed against an OOSSXX 5323-W6-L2. Likely to work unmodified on any
other camera from this same Tmezon/EseeCloud/IP Pro/VR Cam OEM family — try
it and open an issue either way.

## License

MIT — see [LICENSE](LICENSE).
