import importlib
import pkgutil
from dataclasses import dataclass
from pathlib import Path
from random import choices
from typing import Optional

from strictyaml import Map, load, as_document, YAMLValidationError
from strictyaml.exceptions import YAMLSerializationError

from fsstratify.errors import ConfigurationError
from fsstratify.filesystems import SimulationVirtualFileSystem
from fsstratify.operations import get_operations_map

_model_registry = {}


def get_model_registry() -> dict:
    return _model_registry.copy()


@dataclass
class UsageModelConfiguration:
    simulation_vfs: SimulationVirtualFileSystem
    parameters: dict
    simulation_dir: Path


class UsageModel:
    """Base class for usage model implementations.

    Usage model implementations have to inherit from this class, and they have to be
    iterable. The usage model name can be customized using the model_name keyword
    argument.
    """

    def __init__(self, config: dict, simulation_dir: Path):
        self._operations = get_operations_map()
        self._config: dict = config
        self._sim_vfs: Optional[SimulationVirtualFileSystem] = None
        self._simulation_dir: Path = simulation_dir
        self._parse_config(config)
        self._validate_config()

    def steps(self) -> int:
        """Returns the total number of operations the model is going to perform."""
        raise NotImplementedError(
            f'Usage model "{self.__class__.__name__}" does not implement "steps".'
        )

    def set_simulation_parameters(self, vfs: SimulationVirtualFileSystem) -> None:
        self._sim_vfs = vfs

    def _parse_config(self, config: dict) -> None:
        """Parse the given configuration dict."""
        if schema := self._get_schema():
            schema = Map(schema)
            try:
                self._config = load(as_document(config, schema).as_yaml(), schema).data
            except YAMLSerializationError as err:
                if "non-empty list" in str(err):
                    raise ConfigurationError(
                        "Error: Expected a non-empty list, found an empty list."
                    )
                raise
            except YAMLValidationError as err:
                raise ConfigurationError(str(err))

    def _validate_config(self) -> None:
        raise NotImplementedError(
            f'Usage model "{self.__class__.__name__}" does not '
            'implement "_validate_config".'
        )

    def _get_schema(self):
        raise NotImplementedError(
            f'Usage model "{self.__class__.__name__}" does not '
            'implement "_get_schema".'
        )

    @classmethod
    def get_yaml_template(cls) -> list:
        schema = cls._get_schema()
        return [{"key": k, "value": v} for k, v in schema.items()]

    def __init_subclass__(cls, **kwargs):
        if "model_name" in kwargs:
            name = kwargs.pop("model_name")
        else:
            name = cls.__name__
        super().__init_subclass__(**kwargs)
        cls.model_name = name
        _model_registry[name] = cls


def use_existing_path(weight: float) -> bool:
    if not isinstance(weight, float):
        raise ValueError("Error: weight has to be a float between 0.0 and 1.0.")
    weight = float(weight)
    if weight < 0 or weight > 1:
        raise ValueError("Error: weight has to be a float between 0.0 and 1.0.")
    return choices((0, 1), weights=(1 - weight, weight), k=1)[0] == 1


def _discover_usage_model_plugins() -> None:
    here = str(Path(__file__).absolute().parent)
    for _, name, _ in pkgutil.iter_modules([here], "fsstratify.usagemodels."):
        if name != "base":
            importlib.import_module(name)


_discover_usage_model_plugins()
