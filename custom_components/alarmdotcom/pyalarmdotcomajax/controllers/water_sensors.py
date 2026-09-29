"""Alarm.com controller for water sensors."""

from .base import BaseController, device_controller
from ..models.base import ResourceType
from ..models.water_sensor import WaterSensor
from ..websocket.client import SupportedResourceEvents
from ..websocket.messages import ResourceEventType


@device_controller(ResourceType.WATER_SENSOR, WaterSensor)
class WaterSensorController(BaseController[WaterSensor]):
    """Controller for water sensors."""

    _supported_resource_events = SupportedResourceEvents(
        events=[ResourceEventType.Opened, ResourceEventType.Closed]
    )
