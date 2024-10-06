from pathlib import Path
from pprint import pformat

import strictyaml
from strictyaml import Map, Int, Bool, Str, Optional, Any

from fsstratify.errors import ConfigurationError
from fsstratify.platforms import get_current_platform, Platform
from fsstratify.utils import parse_size_definition

FSSTRATIFY_BLOCK_SIZE = 512
SIMULATION_CONFIG_FILE_NAME = "simulation.yml"

SIMULATION_MOUNT_POINT = "mnt"
_SIMULATION_LOG_NAME = "simulation.log"
_STRATA_LOG_NAME = "simulation.strata"
_OUTPUT_PLAYBOOK_NAME = "simulation.playbook"


class Configuration:
    """This class represents the configuration of fsstratify."""

    def __init__(self):
        self._cfg = None
        self._schema = Map(
            {
                "seed": Int(),
                Optional("write_playbook", default=False): Bool(),
                Optional("log_level", default="warning"): Str(),
                Optional("write_timestamps", default=False): Bool(),
                "file_system": Map(
                    {
                        "type": Str(),
                        "formatting_parameters": Str(),
                        Optional("prepopulate_with"): Map(
                            {
                                "dataset": Str(),
                                Optional("mutable", default=False): Bool(),
                            }
                        ),
                    }
                ),
                "volume": Map(
                    {
                        "keep": Bool(),
                        "type": Str(),
                        "size": Str(),
                        Optional("force_overwrite", default=False): Bool(),
                        Optional("win_drive_letter", default="S"): Str(),
                        Optional("disk_num"): Int(),
                        Optional("device"): Str(),
                        Optional("use_again", default=False): Bool(),
                        Optional("volume_number", default=""): Str(),
                    }
                ),
                "usage_model": Map({"type": Str(), "parameters": Any()}),
            }
        )

    @staticmethod
    def _additional_conf_check(cfg):
        if (
            get_current_platform() == Platform.WINDOWS
            and cfg["volume"]["type"].lower() == "physical"
            and "disk_num" not in cfg["volume"]
        ):
            raise ConfigurationError(
                "Physical Windows volume requires argument disk_num in the configuration file"
            )
        if (
            get_current_platform() == Platform.LINUX
            and cfg["volume"]["type"].lower() == "physical"
            and "device" not in cfg["volume"]
        ):
            raise ConfigurationError(
                "Error: Missing device path for physical Linux volume."
            )

    def load_str(self, conf_str: str, simulation_dir: Path) -> None:
        """Load the configuration from a string.

        Args:
            conf_str: string with configuration to parse.
            simulation_dir: path to simulation directory.
        """
        cfg = strictyaml.load(conf_str, self._schema)
        cfg = dict(cfg.data)
        cfg["volume"]["directory"] = simulation_dir
        if "size" in cfg["volume"]:
            try:
                cfg["volume"]["size"] = parse_size_definition(cfg["volume"]["size"])
            except ValueError as err:
                raise ConfigurationError(err) from err
        cfg["path"] = simulation_dir
        cfg["mount_point"] = (Path(simulation_dir) / SIMULATION_MOUNT_POINT).resolve()
        cfg["strata_log"] = (Path(simulation_dir) / _STRATA_LOG_NAME).resolve()
        cfg["simulation_log"] = (Path(simulation_dir) / _SIMULATION_LOG_NAME).resolve()
        cfg["output_playbook_path"] = (
            Path(simulation_dir) / _OUTPUT_PLAYBOOK_NAME
        ).resolve()
        self._additional_conf_check(cfg)
        self._cfg = cfg

    def load_file(self, path: Path) -> None:
        """Load the configuration from a config file.

        Args:
            path: Path to the config file to parse.
        """
        with path.open("r") as f:
            self.load_str(f.read(), Path(path.parent))

    def __str__(self) -> str:
        return pformat(self._cfg)

    def __getitem__(self, item):
        return self._cfg[item]
