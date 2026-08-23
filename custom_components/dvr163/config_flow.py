"""Config flow for the DVR163 IP Camera integration."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import Dvr163Api, Dvr163ApiError
from .const import DEFAULT_PORT, DEFAULT_USERNAME, DOMAIN, STREAM_PATH_MAIN
from .protocol import Dvr163Client, Dvr163ProtocolError

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): str,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
        vol.Optional(CONF_USERNAME, default=DEFAULT_USERNAME): str,
        vol.Optional(CONF_PASSWORD, default=""): str,
    }
)


class Dvr163ConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for a single camera."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            await self.async_set_unique_id(user_input[CONF_HOST])
            self._abort_if_unique_id_configured()

            try:
                await self._async_validate(user_input)
            except _InvalidAuth:
                errors["base"] = "invalid_auth"
            except _CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                return self.async_create_entry(
                    title=user_input[CONF_HOST], data=user_input
                )

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_SCHEMA, errors=errors
        )

    async def _async_validate(self, data: dict[str, Any]) -> None:
        session = async_get_clientsession(self.hass)
        api = Dvr163Api(
            session,
            data[CONF_HOST],
            data[CONF_PORT],
            data[CONF_USERNAME],
            data[CONF_PASSWORD],
        )
        try:
            await api.get_image_attrs()
        except Dvr163ApiError as err:
            if "401" in str(err) or "403" in str(err):
                raise _InvalidAuth from err
            raise _CannotConnect from err

        # Both streams live at fixed, well-known paths on this firmware
        # family (see const.py) -- validating the main one is enough proof
        # the camera and credentials are good; the sub stream is set up
        # the same way at runtime and doesn't need a separate check here.
        client = Dvr163Client(
            data[CONF_HOST],
            data[CONF_PORT],
            STREAM_PATH_MAIN,
            data[CONF_USERNAME],
            data[CONF_PASSWORD],
        )
        agen = client.stream()
        try:
            await asyncio.wait_for(agen.__anext__(), timeout=15)
        except (Dvr163ProtocolError, StopAsyncIteration, asyncio.TimeoutError, OSError) as err:
            raise _CannotConnect from err
        finally:
            await agen.aclose()


class _CannotConnect(Exception):
    """Could not reach the camera or the stream endpoint rejected us."""


class _InvalidAuth(Exception):
    """Credentials were rejected."""
