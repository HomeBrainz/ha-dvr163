"""PTZ and preset buttons for the DVR163 IP Camera integration."""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import Dvr163ConfigEntry
from .const import DOMAIN, PRESET_COUNT

_PTZ_ACTIONS = {
    "left": ("Pan Left", "mdi:arrow-left-bold"),
    "right": ("Pan Right", "mdi:arrow-right-bold"),
    "up": ("Tilt Up", "mdi:arrow-up-bold"),
    "down": ("Tilt Down", "mdi:arrow-down-bold"),
    "stop": ("Stop", "mdi:stop"),
}


async def async_setup_entry(
    hass: HomeAssistant, entry: Dvr163ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    entities: list[ButtonEntity] = [
        Dvr163PtzButton(entry, action, name, icon)
        for action, (name, icon) in _PTZ_ACTIONS.items()
    ]
    entities += [
        Dvr163PresetButton(entry, number) for number in range(PRESET_COUNT)
    ]
    async_add_entities(entities)


def _device_info(entry: Dvr163ConfigEntry) -> DeviceInfo:
    return DeviceInfo(identifiers={(DOMAIN, entry.entry_id)})


class Dvr163PtzButton(ButtonEntity):
    _attr_has_entity_name = True

    def __init__(self, entry: Dvr163ConfigEntry, action: str, name: str, icon: str) -> None:
        self._entry = entry
        self._action = action
        self._attr_name = name
        self._attr_icon = icon
        self._attr_unique_id = f"{entry.entry_id}_ptz_{action}"
        self._attr_device_info = _device_info(entry)

    async def async_press(self) -> None:
        await self._entry.runtime_data.api.ptz(self._action)


class Dvr163PresetButton(ButtonEntity):
    _attr_has_entity_name = True
    _attr_icon = "mdi:map-marker-star"

    def __init__(self, entry: Dvr163ConfigEntry, number: int) -> None:
        self._entry = entry
        self._number = number
        self._attr_name = f"Preset {number + 1}"
        self._attr_unique_id = f"{entry.entry_id}_preset_{number}"
        self._attr_device_info = _device_info(entry)

    async def async_press(self) -> None:
        await self._entry.runtime_data.api.preset_goto(self._number)
