# DVR163 IP Camera — Home Assistant integration

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

- **Camera** entity — live video **and audio**, via Home Assistant's normal
  stream/HLS pipeline (H.265 video is copied through as-is; audio is
  remuxed from AAC).
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
4. Enter the camera's IP, port (default 80), username/password, and stream
   path (default `/livestream/11`; use `/livestream/12` for the lighter
   substream instead).

## Known firmware quirks this integration works around

- **`setimageattr` is all-or-nothing.** This firmware silently resets any
  image attribute *not* included in a given `setimageattr` call back to
  `127`. Every write from this integration always resends the full set
  (hue/brightness/saturation/contrast/scene/flip/mirror) rather than just
  the one value that changed.
- **`ffmpeg`'s local http listener accepts exactly one client for its
  process lifetime.** The stream pipeline is supervised and automatically
  restarted whenever it drops — this is expected behavior on every
  reconnect, not a bug, and Home Assistant's `stream` integration is the
  only intended client of it.

## Compatibility

Confirmed against an OOSSXX 5323-W6-L2. Likely to work unmodified, or with
just a different `stream_path`, on any other camera from this same
Tmezon/EseeCloud/IP Pro/VR Cam OEM family — try it and open an issue either
way.

## License

MIT — see [LICENSE](LICENSE).
