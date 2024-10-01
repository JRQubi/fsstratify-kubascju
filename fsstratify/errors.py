"""This module contains the fsstratify errors/exceptions."""


class FsstratifyError(Exception):
    """Base class for all fsstratify errors."""


class ConfigurationError(FsstratifyError):
    """This error indicates an invalid configuration."""


class SimulationError(FsstratifyError):
    """This error indicates an error during the simulation."""


class VolumeError(FsstratifyError):
    """This error indicates that something went wrong with the volume."""


class PlaybookError(FsstratifyError):
    """This error indicates errors in a playbook."""
