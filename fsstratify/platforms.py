"""This module contains identifiers for the supported platforms."""
import platform
from enum import Enum

from fsstratify.errors import SimulationError


class Platform(Enum):
    """This enum represents the supported platforms."""

    WINDOWS = 1
    LINUX = 2


def get_current_platform() -> Platform:
    """Return the platform a simulation is started on."""
    system = platform.system()
    if system == "Linux":
        return Platform.LINUX
    if system == "Windows":
        return Platform.WINDOWS
    raise SimulationError
