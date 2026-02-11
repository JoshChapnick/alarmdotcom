"""Interfaces with Alarm.com alarm control panels."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic

from . import pyalarmdotcomajax as pyadc
from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity,
    AlarmControlPanelEntityDescription,
    AlarmControlPanelEntityFeature,
    AlarmControlPanelState,
    CodeFormat,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import InvalidStateError, ServiceValidationError
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import DiscoveryInfoType
from .pyalarmdotcomajax.controllers.partitions import PartitionController

from .const import (
    CONF_ARM_AWAY,
    CONF_ARM_CODE,
    CONF_ARM_HOME,
    CONF_ARM_NIGHT,
    CONF_FORCE_BYPASS,
    CONF_NO_ENTRY_DELAY,
    CONF_SILENT_ARM,
    DATA_HUB,
    DOMAIN,
)
from .entity import AdcControllerT, AdcEntity, AdcEntityDescription, AdcManagedDeviceT
from .util import cleanup_orphaned_entities_and_devices

if TYPE_CHECKING:
    from .hub import AlarmHub

log = logging.getLogger(__name__)

DISARM = "disarm"
ARM_AWAY = "arm_away"
ARM_STAY = "arm_stay"
ARM_NIGHT = "arm_night"


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up the light platform."""

    hub: AlarmHub = hass.data[DOMAIN][config_entry.entry_id][DATA_HUB]

    entities = [
        AdcAlarmControlPanelEntity(hub=hub, resource_id=device.id, description=entity_description)
        for entity_description in ENTITY_DESCRIPTIONS
        for device in hub.api.partitions
        if entity_description.supported_fn(hub, device.id)
    ]
    async_add_entities(entities)

    current_entity_ids = {entity.entity_id for entity in entities}
    current_unique_ids = {uid for uid in (entity.unique_id for entity in entities) if uid is not None}
    await cleanup_orphaned_entities_and_devices(
        hass,
        config_entry,
        current_entity_ids,
        current_unique_ids,
        "alarm_control_panel",
    )


@callback
def code_format_fn(hub: AlarmHub) -> CodeFormat | None:
    """Return the code format for the device."""

    arm_code = hub.config_entry.options.get(CONF_ARM_CODE)

    if arm_code in [None, ""]:
        return None

    return CodeFormat.NUMBER if re.fullmatch(r"\d+", str(arm_code)) else CodeFormat.TEXT


@callback
def extra_state_attributes(hub: AlarmHub, partition_id: str) -> Mapping[str, Any]:
    """Collect extra state attributes."""

    resource = hub.api.partitions.get(partition_id)
    if resource is None:
        return {}

    return {
        "uncleared_issues": resource.attributes.needs_clear_issues_prompt,
    }


@callback
def state_fn(hub: AlarmHub, partition_id: str) -> AlarmControlPanelState | None:
    """Return the state of a partition."""

    resource = hub.api.partitions.get(partition_id)
    if resource is None:
        return None

    if resource.attributes.is_malfunctioning:
        return None

    # Mapping of PartitionState to AlarmControlPanelState
    state_mapping = {
        pyadc.partition.PartitionState.DISARMED: AlarmControlPanelState.DISARMED,
        pyadc.partition.PartitionState.ARMED_STAY: AlarmControlPanelState.ARMED_HOME,
        pyadc.partition.PartitionState.ARMED_AWAY: AlarmControlPanelState.ARMED_AWAY,
        pyadc.partition.PartitionState.ARMED_NIGHT: AlarmControlPanelState.ARMED_NIGHT,
    }

    if resource.attributes.state == resource.attributes.desired_state:
        return state_mapping.get(resource.attributes.state)

    desired_state_mapping = {
        pyadc.partition.PartitionState.DISARMED: AlarmControlPanelState.DISARMING,
        pyadc.partition.PartitionState.ARMED_STAY: AlarmControlPanelState.ARMING,
        pyadc.partition.PartitionState.ARMED_AWAY: AlarmControlPanelState.ARMING,
        pyadc.partition.PartitionState.ARMED_NIGHT: AlarmControlPanelState.ARMING,
    }

    return desired_state_mapping.get(resource.attributes.desired_state) if resource.attributes.desired_state else None


@callback
def supported_features_fn(controller: PartitionController, partition_id: str) -> AlarmControlPanelEntityFeature:
    """Return the supported features for the device."""

    resource = controller.get(partition_id)

    if not resource:
        return AlarmControlPanelEntityFeature(0)

    return (
        AlarmControlPanelEntityFeature.ARM_HOME
        | AlarmControlPanelEntityFeature.ARM_AWAY
        | (AlarmControlPanelEntityFeature.ARM_NIGHT if resource.attributes.supports_night_arming else 0)
    )


@callback
async def control_fn(
    hub: AlarmHub,
    controller: pyadc.PartitionController,
    partition_id: str,
    command: str,
    options: dict[str, Any],
) -> None:
    """Arm/disarm the device."""

    config_options = hub.config_entry.options
    arm_code = config_options.get(CONF_ARM_CODE)

    user_entered_code = options.get("code")

    if user_entered_code != arm_code and arm_code not in [None, ""]:
        raise ServiceValidationError("Invalid code.")

    try:
        async with asyncio.timeout(30):  # 30-second timeout for alarm commands
            if command == DISARM:
                await controller.disarm(partition_id)

            elif command == ARM_AWAY:
                cmd_options = config_options.get(CONF_ARM_AWAY, {})
                await controller.arm_away(
                    partition_id,
                    force_bypass=CONF_FORCE_BYPASS in cmd_options,
                    no_entry_delay=CONF_NO_ENTRY_DELAY in cmd_options,
                    silent_arming=CONF_SILENT_ARM in cmd_options,
                )

            elif command == ARM_STAY:
                cmd_options = config_options.get(CONF_ARM_HOME, {})
                await controller.arm_stay(
                    partition_id,
                    force_bypass=CONF_FORCE_BYPASS in cmd_options,
                    no_entry_delay=CONF_NO_ENTRY_DELAY in cmd_options,
                    silent_arming=CONF_SILENT_ARM in cmd_options,
                )

            elif command == ARM_NIGHT:
                cmd_options = config_options.get(CONF_ARM_NIGHT, {})
                await controller.arm_night(
                    partition_id,
                    force_bypass=CONF_FORCE_BYPASS in cmd_options,
                    no_entry_delay=CONF_NO_ENTRY_DELAY in cmd_options,
                    silent_arming=CONF_SILENT_ARM in cmd_options,
                )

            else:
                raise ServiceValidationError("Unsupported command.")

    except TimeoutError as ex:
        log.error("Command %s timed out after 30 seconds", command)
        raise InvalidStateError(f"Command {command} timed out - check alarm panel status") from ex
    except (pyadc.ServiceUnavailable, pyadc.UnexpectedResponse) as ex:
        raise InvalidStateError("Failed to execute alarm command.") from ex


@dataclass(frozen=True, kw_only=True)
class AdcAlarmControlPanelEntityDescription(
    Generic[AdcManagedDeviceT, AdcControllerT],
    AdcEntityDescription[AdcManagedDeviceT, AdcControllerT],
    AlarmControlPanelEntityDescription,
):
    """Base Alarm.com entity description."""

    # fmt: off
    code_format_fn: Callable[[AlarmHub], CodeFormat | None]
    """Return the code format for the device."""
    supported_features_fn: Callable[[AdcControllerT, str], AlarmControlPanelEntityFeature]
    """Return the supported features for the device."""
    control_fn: Callable[[AlarmHub, AdcControllerT, str, str, dict[str, Any]], Coroutine[Any, Any, None]]
    # Hub, Controller, Device ID, Command, Options
    """Arm/disarm the device."""
    state_fn: Callable[[AlarmHub, str], AlarmControlPanelState | None]
    """Return the state of the device."""
    # fmt: on


ENTITY_DESCRIPTIONS: list[AdcAlarmControlPanelEntityDescription] = [
    AdcAlarmControlPanelEntityDescription[pyadc.partition.Partition, pyadc.PartitionController](
        key="partitions",
        controller_fn=lambda hub, _: hub.api.partitions,
        state_fn=state_fn,
        code_format_fn=code_format_fn,
        supported_features_fn=supported_features_fn,
        control_fn=control_fn,
    )
]


class AdcAlarmControlPanelEntity(AdcEntity[AdcManagedDeviceT, AdcControllerT], AlarmControlPanelEntity):
    """Base Alarm.com alarm control panel entity."""

    entity_description: AdcAlarmControlPanelEntityDescription

    # Fix 2: Set class-level defaults to prevent keypad from ever showing
    # HA access is the authorization - no need to re-enter PIN
    _attr_code_arm_required: bool = False
    _attr_code_format: CodeFormat | None = None

    # Track pending command verification
    _pending_command_target: pyadc.partition.PartitionState | None = None
    _pending_command_task: asyncio.Task | None = None

    def _validate_code(self, code: str | None) -> bool:
        """Validate code - always passes since HA access is the authorization."""
        return True

    def _set_optimistic_desired_state(self, target_state: pyadc.partition.PartitionState) -> None:
        """Set optimistic desired_state for immediate UI feedback.

        This updates the local desired_state while keeping the actual state unchanged,
        which triggers state_fn() to return ARMING or DISARMING for immediate UI feedback.
        """
        resource = self.hub.api.partitions.get(self.resource_id)
        if resource:
            log.info(
                "[HA ENTITY] Setting optimistic desired_state: partition=%s, current_state=%s, target_desired=%s",
                self.resource_id,
                resource.attributes.state.name if resource.attributes.state else "None",
                target_state.name,
            )
            # Update desired_state locally (state remains unchanged)
            # This triggers state_fn() to return ARMING or DISARMING
            resource.attributes.desired_state = target_state
            # Push state change to HA immediately
            self.update_state(pyadc.ResourceEventMessage(
                topic=pyadc.EventBrokerTopic.RESOURCE_UPDATED,
                id=self.resource_id
            ))
            self.async_write_ha_state()

    async def _verify_state_transition(
        self,
        target_state: pyadc.partition.PartitionState,
        initial_wait: float = 30.0,
        retry_interval: float = 10.0,
        max_retries: int = 2,
    ) -> None:
        """Verify state transition completes, refresh from API if needed.

        This method runs as a background task after sending a command. It waits for
        the WebSocket event to confirm state transition. If the state doesn't transition
        within the timeout, it forces an API refresh to correct any stuck states.
        """
        log.info(
            "[HA ENTITY] Starting state verification: partition=%s, target=%s, initial_wait=%ss",
            self.resource_id,
            target_state.name,
            initial_wait,
        )
        retries = 0

        while retries <= max_retries:
            # Wait for WebSocket event
            wait_time = initial_wait if retries == 0 else retry_interval
            await asyncio.sleep(wait_time)

            # Check if verification was cancelled (new command issued or WebSocket delivered)
            if self._pending_command_target != target_state:
                log.info("[HA ENTITY] State verification cancelled - target changed from %s", target_state.name)
                return

            # Check current state
            resource = self.hub.api.partitions.get(self.resource_id)
            if not resource:
                log.warning("[HA ENTITY] Resource not found during verification for partition %s", self.resource_id)
                return

            # If state already transitioned, we're done
            if resource.attributes.state == target_state:
                log.info("[HA ENTITY] State transition verified via WebSocket: partition=%s, state=%s", self.resource_id, target_state.name)
                self._pending_command_target = None
                return

            # State hasn't transitioned - force API refresh
            log.warning(
                "[HA ENTITY] State stuck after %ss (partition=%s, current=%s, target=%s), forcing API refresh (attempt %s/%s)",
                initial_wait + (retry_interval * retries),
                self.resource_id,
                resource.attributes.state.name if resource.attributes.state else "None",
                target_state.name,
                retries + 1,
                max_retries + 1,
            )

            try:
                await self.controller._refresh(resource_id=self.resource_id)

                # Check if refresh fixed it
                resource = self.hub.api.partitions.get(self.resource_id)
                if resource and resource.attributes.state == target_state:
                    log.info("State corrected after API refresh")
                    self._pending_command_target = None
                    self.update_state(pyadc.ResourceEventMessage(
                        topic=pyadc.EventBrokerTopic.RESOURCE_UPDATED,
                        id=self.resource_id
                    ))
                    self.async_write_ha_state()
                    return
            except Exception as err:
                log.error("Failed to refresh partition state: %s", err)

            retries += 1

        # All retries exhausted - clear stuck state
        log.error("State verification failed after %s attempts", max_retries + 1)
        self._pending_command_target = None

        # Reset desired_state to match actual state to clear transitional display
        resource = self.hub.api.partitions.get(self.resource_id)
        if resource:
            resource.attributes.desired_state = resource.attributes.state
            self.update_state(pyadc.ResourceEventMessage(
                topic=pyadc.EventBrokerTopic.RESOURCE_UPDATED,
                id=self.resource_id
            ))
            self.async_write_ha_state()

    @callback
    def initiate_state(self) -> None:
        """Initiate entity state."""

        self._attr_supported_features = self.entity_description.supported_features_fn(self.controller, self.resource_id)

        super().initiate_state()

    @callback
    def update_state(self, message: pyadc.EventBrokerMessage | None = None) -> None:
        """Update entity state."""

        if isinstance(message, pyadc.ResourceEventMessage):
            old_state = self.alarm_state
            self.alarm_state = self.entity_description.state_fn(self.hub, self.resource_id)

            # Log state updates for debugging
            resource = self.hub.api.partitions.get(self.resource_id)
            if resource:
                log.info(
                    "[HA ENTITY] State update: partition=%s, ha_state: %s -> %s, adc_state=%s, adc_desired=%s, pending_target=%s",
                    self.resource_id,
                    old_state.name if old_state else "None",
                    self.alarm_state.name if self.alarm_state else "None",
                    resource.attributes.state.name if resource.attributes.state else "None",
                    resource.attributes.desired_state.name if resource.attributes.desired_state else "None",
                    self._pending_command_target.name if self._pending_command_target else "None",
                )

            # Clear pending verification if WebSocket delivered expected state
            if (
                resource
                and self._pending_command_target
                and resource.attributes.state == self._pending_command_target
            ):
                log.info("[HA ENTITY] WebSocket delivered expected state %s, clearing verification", self._pending_command_target.name)
                self._pending_command_target = None

    async def async_alarm_disarm(self, code: str | None = None) -> None:
        """Send disarm command."""

        if self._validate_code(code):
            # Cancel any pending verification task
            if self._pending_command_task and not self._pending_command_task.done():
                self._pending_command_task.cancel()

            target_state = pyadc.partition.PartitionState.DISARMED

            # Set optimistic state for immediate "Disarming..." UI feedback
            self._set_optimistic_desired_state(target_state)

            # Send the command
            await self.entity_description.control_fn(
                self.hub, self.controller, self.resource_id, DISARM, {"code": code}
            )

            # Start background verification
            self._pending_command_target = target_state
            self._pending_command_task = self.hass.async_create_task(
                self._verify_state_transition(target_state),
                name=f"alarmdotcom_verify_{self.resource_id}",
            )

    async def async_alarm_arm_home(self, code: str | None = None) -> None:
        """Send arm home command."""

        if self._validate_code(code):
            # Cancel any pending verification task
            if self._pending_command_task and not self._pending_command_task.done():
                self._pending_command_task.cancel()

            target_state = pyadc.partition.PartitionState.ARMED_STAY

            # Set optimistic state for immediate "Arming..." UI feedback
            self._set_optimistic_desired_state(target_state)

            # Send the command
            await self.entity_description.control_fn(
                self.hub, self.controller, self.resource_id, ARM_STAY, {"code": code}
            )

            # Start background verification
            self._pending_command_target = target_state
            self._pending_command_task = self.hass.async_create_task(
                self._verify_state_transition(target_state),
                name=f"alarmdotcom_verify_{self.resource_id}",
            )

    async def async_alarm_arm_away(self, code: str | None = None) -> None:
        """Send arm away command."""

        if self._validate_code(code):
            # Cancel any pending verification task
            if self._pending_command_task and not self._pending_command_task.done():
                self._pending_command_task.cancel()

            target_state = pyadc.partition.PartitionState.ARMED_AWAY

            # Set optimistic state for immediate "Arming..." UI feedback
            self._set_optimistic_desired_state(target_state)

            # Send the command
            await self.entity_description.control_fn(
                self.hub, self.controller, self.resource_id, ARM_AWAY, {"code": code}
            )

            # Start background verification
            self._pending_command_target = target_state
            self._pending_command_task = self.hass.async_create_task(
                self._verify_state_transition(target_state),
                name=f"alarmdotcom_verify_{self.resource_id}",
            )

    async def async_alarm_arm_night(self, code: str | None = None) -> None:
        """Send arm night command."""

        if self._validate_code(code):
            # Cancel any pending verification task
            if self._pending_command_task and not self._pending_command_task.done():
                self._pending_command_task.cancel()

            target_state = pyadc.partition.PartitionState.ARMED_NIGHT

            # Set optimistic state for immediate "Arming..." UI feedback
            self._set_optimistic_desired_state(target_state)

            # Send the command
            await self.entity_description.control_fn(
                self.hub, self.controller, self.resource_id, ARM_NIGHT, {"code": code}
            )

            # Start background verification
            self._pending_command_target = target_state
            self._pending_command_task = self.hass.async_create_task(
                self._verify_state_transition(target_state),
                name=f"alarmdotcom_verify_{self.resource_id}",
            )

    async def async_will_remove_from_hass(self) -> None:
        """Clean up when entity is removed."""
        if self._pending_command_task and not self._pending_command_task.done():
            self._pending_command_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pending_command_task
