"""Flip/mirror switches for the DVR163 IP Camera integration."""
from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import Dvr163ConfigEntry
from .const import DOMAIN

_SWITCHES = {
    "flip": ("Flip", "mdi:flip-vertical"),
    "mirror": ("Mirror", "mdi:flip-horizontal"),
}


async def async_setup_entry(
    hass: HomeAssistant, entry: Dvr163ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    async_add_entities(
        Dvr163ImageAttrSwitch(entry, key, name, icon)
        for key, (name, icon) in _SWITCHES.items()
    )


class Dvr163ImageAttrSwitch(SwitchEntity):
    _attr_has_entity_name = True

    def __init__(self, entry: Dvr163ConfigEntry, key: str, name: str, icon: str) -> None:
        self._entry = entry
        self._key = key
        self._attr_name = name
        self._attr_icon = icon
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, entry.entry_id)})

    @property
    def is_on(self) -> bool | None:
        current = self._entry.runtime_data.image_attrs.current
        return getattr(current, self._key) == "on" if current else None

    async def async_turn_on(self, **kwargs: object) -> None:
        await self._entry.runtime_data.image_attrs.async_update(**{self._key: "on"})

    async def async_turn_off(self, **kwargs: object) -> None:
        await self._entry.runtime_data.image_attrs.async_update(**{self._key: "off"})
