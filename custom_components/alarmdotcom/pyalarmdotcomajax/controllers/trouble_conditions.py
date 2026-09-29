"""Alarm.com controller for trouble conditions."""

from .base import BaseController
from ..models.base import ResourceType
from ..models.trouble_condition import TroubleCondition

from .base import device_controller


@device_controller(ResourceType.TROUBLE_CONDITION, TroubleCondition)
class TroubleConditionController(BaseController[TroubleCondition]):
    """Controller for trouble conditions."""
