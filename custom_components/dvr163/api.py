"""Thin async client for this camera family's hi3510 cgi-bin control API.

Endpoints and exact parameter names below are all empirically confirmed
against a real device (OOSSXX 5323-W6-L2 / Tmezon / EseeCloud family,
HiSilicon Hi3510 platform) -- not guessed from generic Hi3510 docs, which
often don't match a given OEM's actual firmware build.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import aiohttp

_ATTR_RE = re.compile(r'(\w+)="([^"]*)"')


class Dvr163ApiError(Exception):
    """Raised when the camera rejects or fails to answer a control request."""


@dataclass
class ImageAttrs:
    hue: int
    brightness: int
    saturation: int
    contrast: int
    scene: str
    flip: str
    mirror: str


class Dvr163Api:
    """Talks to /cgi-bin/hi3510/*.cgi for PTZ, presets, and image attributes."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        host: str,
        port: int,
        username: str,
        password: str,
    ) -> None:
        self._session = session
        self._base = f"http://{host}:{port}"
        self._auth = aiohttp.BasicAuth(username, password)

    async def _get(self, path: str) -> str:
        try:
            async with self._session.get(
                f"{self._base}{path}", auth=self._auth, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    raise Dvr163ApiError(f"{path} -> HTTP {resp.status}")
                return await resp.text(errors="replace")
        except aiohttp.ClientError as err:
            raise Dvr163ApiError(f"{path} -> {err}") from err

    async def get_image_attrs(self) -> ImageAttrs:
        text = await self._get("/cgi-bin/hi3510/param.cgi?cmd=getimageattr")
        values = dict(_ATTR_RE.findall(text))
        try:
            return ImageAttrs(
                hue=int(values["hue"]),
                brightness=int(values["brightness"]),
                saturation=int(values["saturation"]),
                contrast=int(values["contrast"]),
                scene=values.get("scene", "auto"),
                flip=values.get("flip", "on"),
                mirror=values.get("mirror", "on"),
            )
        except (KeyError, ValueError) as err:
            raise Dvr163ApiError(f"unexpected getimageattr response: {text!r}") from err

    async def set_image_attrs(self, attrs: ImageAttrs) -> None:
        # Firmware bug (see README): omitting any of these resets it to 127.
        # Always send hue/brightness/saturation/contrast/scene/flip/mirror
        # together, every time, even if only one value actually changed.
        await self._get(
            "/cgi-bin/hi3510/param.cgi?cmd=setimageattr"
            f"&-hue={attrs.hue}&-brightness={attrs.brightness}"
            f"&-saturation={attrs.saturation}&-contrast={attrs.contrast}"
            f"&-scene={attrs.scene}&-flip={attrs.flip}&-mirror={attrs.mirror}"
        )

    async def ptz(self, action: str) -> None:
        if action == "stop":
            await self._get("/cgi-bin/hi3510/ptzctrl.cgi?-act=stop")
        else:
            await self._get(f"/cgi-bin/hi3510/ptzctrl.cgi?-step=1&-act={action}")

    async def preset_goto(self, number: int) -> None:
        await self._get(f"/cgi-bin/hi3510/preset.cgi?-act=goto&-number={number}")
