"""Shared cache for this firmware's all-or-nothing image attribute writes.

setimageattr silently resets any attribute left out of a call back to 127
(confirmed bug, see README) -- so every write here re-sends the full set of
hue/brightness/saturation/contrast/scene/flip/mirror, using this cache for
whichever fields the caller didn't just change.
"""
from __future__ import annotations

from dataclasses import replace

from .api import Dvr163Api, ImageAttrs


class ImageAttrsStore:
    def __init__(self, api: Dvr163Api) -> None:
        self._api = api
        self.current: ImageAttrs | None = None

    async def async_load(self) -> None:
        self.current = await self._api.get_image_attrs()

    async def async_update(self, **changes: object) -> None:
        if self.current is None:
            await self.async_load()
        assert self.current is not None
        updated = replace(self.current, **changes)
        await self._api.set_image_attrs(updated)
        self.current = updated
