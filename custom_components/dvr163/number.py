"""Image attribute sliders for the DVR163 IP Camera integration."""
from __future__ import annotations

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import Dvr163ConfigEntry
from .const import DOMAIN, IMAGE_ATTR_KEYS

_ICONS = {
    "hue": "mdi:palette",
    "brightness": "mdi:brightness-6",
    "saturation": "mdi:water-percent",
    "contrast": "mdi:contrast-box",
}


async def async_setup_entry(
    hass: HomeAssistant, entry: Dvr163ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    async_add_entities(
        Dvr163ImageAttrNumber(entry, key) for key in IMAGE_ATTR_KEYS
    )


class Dvr163ImageAttrNumber(NumberEntity):
    _attr_has_entity_name = True
    _attr_native_min_value = 0
    _attr_native_max_value = 100
    _attr_native_step = 1
    _attr_mode = NumberMode.SLIDER

    def __init__(self, entry: Dvr163ConfigEntry, key: str) -> None:
        self._entry = entry
        self._key = key
        self._attr_name = key.capitalize()
        self._attr_icon = _ICONS[key]
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, entry.entry_id)})

    @property
    def native_value(self) -> float | None:
        current = self._entry.runtime_data.image_attrs.current
        return getattr(current, self._key) if current else None

    async def async_set_native_value(self, value: float) -> None:
        await self._entry.runtime_data.image_attrs.async_update(**{self._key: int(value)})
