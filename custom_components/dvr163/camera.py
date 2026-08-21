"""Camera entity for the DVR163 IP Camera integration."""
from __future__ import annotations

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.const import CONF_HOST
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import Dvr163ConfigEntry
from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant, entry: Dvr163ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    async_add_entities([Dvr163Camera(entry)])


class Dvr163Camera(Camera):
    """Live view backed by the local remux pipeline in stream.py.

    This firmware has no still-image/snapshot endpoint at all (confirmed
    absent, see README) -- there is no still_image_url to give this entity.
    HA derives a still frame from the stream itself when one is needed.
    """

    _attr_has_entity_name = True
    _attr_name = None
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(self, entry: Dvr163ConfigEntry) -> None:
        super().__init__()
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_camera"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.title,
            manufacturer="Tmezon / EseeCloud (Hi3510-family OEM)",
            model="DVR163 protocol",
            configuration_url=f"http://{entry.data[CONF_HOST]}",
        )

    async def stream_source(self) -> str | None:
        return self._entry.runtime_data.stream_manager.stream_url
