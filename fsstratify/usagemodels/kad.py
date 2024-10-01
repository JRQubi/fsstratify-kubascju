import math
import random
from enum import Enum
from pathlib import Path

from strictyaml import Int, Seq, Float, Map

from fsstratify.errors import ConfigurationError, SimulationError
from fsstratify.filesystems import FileType, FileFilter
from fsstratify.usagemodels.base import UsageModel


class _ModelState(Enum):
    PREVENT_EMPTY_DISK = 0
    REGULAR = 1
    PREVENT_FILLED_DISK = 2


class KADModel(UsageModel, model_name="KAD"):
    def __init__(self, config: dict, simulation_dir: Path):
        super().__init__(config, simulation_dir)
        self._steps: int = self._config["steps"]
        self._current_step: int = 0

        self._write_chunked: bool = True
        self._chunk_size: int = self._config["chunk_size"]
        self._size_factors: list[dict[str, int]] = self._config["size_factors"]
        self._random_range: dict[str, int] = self._config["random_range"]
        self._operation_biases = self._compute_op_biases()

        self._regular_ops = {"names": ("write", "rm", "extend", "shrink")}
        self._regular_ops["weights"] = tuple(
            self._operation_biases[k] for k in self._regular_ops["names"]
        )

        self._mode: _ModelState = _ModelState.REGULAR

    def steps(self) -> int:
        """Returns the total number of operations the model is going to perform."""
        return self._steps

    def _validate_config(self) -> None:
        if self._config["steps"] < 1:
            raise ConfigurationError("Error: Number of steps must be > 0.")
        if self._config["chunk_size"] < 1:
            raise ConfigurationError("Error: chunk_size must be > 0.")
        for weight in (
            "write",
            "delete",
            "increase",
            "decrease",
        ):
            if self._config["operation_factors"][weight] < 0:
                raise ConfigurationError(f"Error: {weight} must be >= 0.")
        if all(x == 0 for x in self._config["operation_factors"].values()):
            raise ConfigurationError(
                "Error: At least one operation factor has to be > 0."
            )
        if len(self._config["size_factors"]) == 0:
            raise ConfigurationError("Error: No size factors provided.")
        for factor in self._config["size_factors"]:
            if factor["size"] <= 0:
                raise ConfigurationError("Error: size of a size factor must be > 0.")
            if factor["weight"] < 0:
                raise ConfigurationError("Error: weight of a size factor must be >= 0.")
        if self._config["random_range"]["min"] < 0:
            raise ConfigurationError("Error: random_range min must be >= 0.")
        if self._config["random_range"]["max"] < 1:
            raise ConfigurationError("Error: random_range max must be > 0.")

        self._assert_valid_write_delete_limits()

    def _assert_valid_write_delete_limits(self) -> None:
        self._assert_valid_write_limits()
        self._assert_valid_delete_limits()
        if self._config["delete_limit"]["stop"] <= self._config["write_limit"]["stop"]:
            raise ConfigurationError(
                "Error: delete_limit start must be > write_limit stop."
            )
        if (
            self._config["write_limit"]["start"]
            >= self._config["delete_limit"]["start"]
        ):
            raise ConfigurationError(
                "Error: write_limit start must be <= delete_limit start."
            )

    def _assert_valid_write_limits(self) -> None:
        if (
            self._config["write_limit"]["start"] < 0
            or self._config["write_limit"]["start"] > 1
        ):
            raise ConfigurationError(
                "Error: write_limit start has to be between 0 and 1."
            )
        if (
            self._config["write_limit"]["stop"] < 0
            or self._config["write_limit"]["stop"] > 1
        ):
            raise ConfigurationError(
                "Error: write_limit stop has to be between 0 and 1."
            )
        if self._config["write_limit"]["start"] > self._config["write_limit"]["stop"]:
            raise ConfigurationError(
                "Error: start value of write_limit has to be <= stop value."
            )

    def _assert_valid_delete_limits(self) -> None:
        if (
            self._config["delete_limit"]["start"] < 0
            or self._config["delete_limit"]["start"] > 1
        ):
            raise ConfigurationError(
                "Error: delete_limit start has to be between 0 and 1."
            )
        if (
            self._config["delete_limit"]["stop"] < 0
            or self._config["delete_limit"]["stop"] > 1
        ):
            raise ConfigurationError(
                "Error: delete_limit stop has to be between 0 and 1."
            )
        if self._config["delete_limit"]["start"] < self._config["delete_limit"]["stop"]:
            raise ConfigurationError(
                "Error: start value of write_limit has to be >= stop value."
            )

    def _get_schema(self) -> dict:
        schema = {
            "steps": Int(),
            "operation_factors": Map(
                {
                    "write": Int(),
                    "delete": Int(),
                    "increase": Int(),
                    "decrease": Int(),
                }
            ),
            "size_factors": Seq(Map({"size": Int(), "weight": Int()})),
            "random_range": Map({"min": Int(), "max": Int()}),
            "chunk_size": Int(),
            "write_limit": Map({"start": Float(), "stop": Float()}),
            "delete_limit": Map({"start": Float(), "stop": Float()}),
        }
        return schema

    def _compute_op_biases(self) -> dict[str, float]:
        factors = self._config["operation_factors"]
        sum_factors = sum(factors.values())
        return {
            "write": factors["write"] / sum_factors,
            "rm": factors["delete"] / sum_factors,
            "extend": factors["increase"] / sum_factors,
            "shrink": factors["decrease"] / sum_factors,
        }

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_step >= self._steps:
            raise StopIteration
        self._current_step += 1
        self._update_model_state()

        if self._mode == _ModelState.PREVENT_EMPTY_DISK:
            return self._generate_write()
        elif self._mode == _ModelState.PREVENT_FILLED_DISK:
            return self._generate_remove()
        else:  # regular mode
            if self._sim_vfs.get_free_space() < 512:  # special case: disk is full
                valid_ops = {"names": ("rm", "shrink")}
                valid_ops["weights"] = tuple(
                    self._operation_biases[k] for k in valid_ops["names"]
                )
            elif self._sim_vfs.empty():  # special case: no files stored on file system
                valid_ops = {"names": ("write",), "weights": (1.0,)}
            else:  # normal case
                valid_ops = self._regular_ops
            op_name = random.choices(valid_ops["names"], weights=valid_ops["weights"])[
                0
            ]
            match op_name:
                case "write":
                    return self._generate_write()
                case "rm":
                    return self._generate_remove()
                case "extend":
                    return self._generate_extend()
                case "shrink":
                    return self._generate_shrink()
                case _:  # pragma: nocover
                    raise SimulationError(
                        f"Unsupported operation for KAD model: {op_name}. "
                        "This should not happen and is a bug."
                    )

    def _generate_write(self):
        op_name = "write"
        path = self._sim_vfs.get_nonexistent_path()
        file_size = self._generate_operation_size(
            min_size=1, max_size=self._sim_vfs.get_free_space()
        )
        playbook_line = (
            f"{op_name} {path} size={file_size} "
            f"chunked={self._write_chunked} chunk_size={self._chunk_size}"
        )
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _generate_remove(self):
        op_name = "rm"
        file = self._sim_vfs.get_random_file().path
        playbook_line = f"{op_name} {file}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _generate_extend(self):
        op_name = "extend"
        path = self._sim_vfs.get_random_file(file_type=FileType.REGULAR).path
        file_size = self._generate_operation_size(
            # min_size => extend at least one _MODEL_BLOCK_SIZE byte block
            # max_size => extend not beyond the remaining disk space
            min_size=1,
            max_size=self._sim_vfs.get_free_space(),
        )
        playbook_line = (
            f"{op_name} {path} extend_size={file_size} "
            f"chunked={self._write_chunked} chunk_size={self._chunk_size}"
        )
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _generate_shrink(self):
        op_name = "shrink"
        file_filter = FileFilter(
            file_type=FileType.REGULAR, min_size=2 * self._chunk_size
        )
        path = self._sim_vfs.get_random_file(file_filter=file_filter).path
        shrink_size = self._generate_operation_size(
            # min_size => shrink at least one chunk_size byte block
            # max_size => keep at least one chunk of the file
            min_size=1,
            max_size=self._sim_vfs.get_size_of(path) - 1,
        )
        playbook_line = f"{op_name} {path} shrink_size={shrink_size}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _generate_operation_size(self, min_size=0, max_size=math.inf):
        size_factors = tuple(x["size"] for x in self._size_factors)
        size_weights = tuple(x["weight"] for x in self._size_factors)
        factor = random.choices(population=size_factors, weights=size_weights)[0]
        random_number = random.randint(
            self._random_range["min"], self._random_range["max"]
        )
        size = self._chunk_size * factor * random_number
        if size > max_size:
            return max_size // self._chunk_size * self._chunk_size
        if size < min_size:
            return (min_size // self._chunk_size + 1) * self._chunk_size
        return size

    def _update_model_state(self):
        disk_usage = self._used_space()
        if disk_usage < self._config["write_limit"]["start"]:
            self._mode = _ModelState.PREVENT_EMPTY_DISK
        elif disk_usage > self._config["delete_limit"]["start"]:
            self._mode = _ModelState.PREVENT_FILLED_DISK
        if (
            self._config["write_limit"]["stop"]
            <= disk_usage
            <= self._config["delete_limit"]["stop"]
        ):
            self._mode = _ModelState.REGULAR

    def _used_space(self):
        total, used, _ = self._sim_vfs.get_usage()
        return used / total
