"""The DVR163 IP Camera integration."""
from __future__ import annotations

from dataclasses import dataclass

from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import Dvr163Api
from .const import CONF_STREAM_PATH
from .image_attrs import ImageAttrsStore
from .stream import StreamManager

PLATFORMS: list[Platform] = [Platform.CAMERA, Platform.BUTTON, Platform.NUMBER, Platform.SWITCH]


@dataclass
class Dvr163RuntimeData:
    api: Dvr163Api
    stream_manager: StreamManager
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
    stream_manager = StreamManager(
        hass,
        entry.data[CONF_HOST],
        entry.data[CONF_PORT],
        entry.data[CONF_STREAM_PATH],
        entry.data[CONF_USERNAME],
        entry.data[CONF_PASSWORD],
    )
    stream_manager.start()

    image_attrs = ImageAttrsStore(api)
    await image_attrs.async_load()

    entry.runtime_data = Dvr163RuntimeData(
        api=api, stream_manager=stream_manager, image_attrs=image_attrs
    )
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: Dvr163ConfigEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.stream_manager.stop()
    return unloaded
