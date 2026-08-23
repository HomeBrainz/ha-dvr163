"""The DVR163 IP Camera integration."""
from __future__ import annotations

from dataclasses import dataclass

from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import Dvr163Api
from .const import STREAMS
from .image_attrs import ImageAttrsStore
from .stream import StreamManager

PLATFORMS: list[Platform] = [Platform.CAMERA, Platform.BUTTON, Platform.NUMBER, Platform.SWITCH]


@dataclass
class Dvr163RuntimeData:
    api: Dvr163Api
    # Keyed by stream id ("main", "sub") -- see const.STREAMS. Every camera
    # on this firmware family always has both, at fixed paths, so both are
    # always set up rather than making the user choose one at config time.
    stream_managers: dict[str, StreamManager]
    image_attrs: ImageAttrsStore


type Dvr163ConfigEntry = ConfigEntry[Dvr163RuntimeData]


async def async_setup_entry(hass: HomeAssistant, entry: Dvr163ConfigEntry) -> bool:
    session = async_get_clientsession(hass)
    api = Dvr163Api(
        session,
        entry.data[CONF_HOST],
        entry.data[CONF_PORT],
        entry.data[CONF_USERNAME],
        entry.data[CONF_PASSWORD],
    )

    stream_managers: dict[str, StreamManager] = {}
    for stream_id, (stream_path, _label) in STREAMS.items():
        manager = StreamManager(
            hass,
            entry.data[CONF_HOST],
            entry.data[CONF_PORT],
            stream_path,
            entry.data[CONF_USERNAME],
            entry.data[CONF_PASSWORD],
        )
        manager.start()
        stream_managers[stream_id] = manager

    image_attrs = ImageAttrsStore(api)
    await image_attrs.async_load()

    entry.runtime_data = Dvr163RuntimeData(
        api=api, stream_managers=stream_managers, image_attrs=image_attrs
    )
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: Dvr163ConfigEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        for manager in entry.runtime_data.stream_managers.values():
            await manager.stop()
    return unloaded
