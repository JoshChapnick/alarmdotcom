"""Alarm.com model for water sensors."""

from dataclasses import dataclass

from .base import (
    AdcDeviceResource,
    ResourceType,
)
from .sensor import SensorAttributes


@dataclass
class WaterSensor(AdcDeviceResource[SensorAttributes]):
    """Water sensor resource."""

    # Can be active / idle / wet / dry

    resource_type = ResourceType.WATER_SENSOR
    attributes_type = SensorAttributes
