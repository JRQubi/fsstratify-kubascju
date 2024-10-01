from pathlib import Path
from random import choice, randint, random
from typing import Tuple

from strictyaml import Str, Int

from fsstratify.errors import ConfigurationError, SimulationError
from fsstratify.filesystems import (
    FileType,
    set_simulation_mount_point,
)
from fsstratify.operations import get_operations_map, Operation
from fsstratify.usagemodels.base import (
    UsageModel,
    use_existing_path,
)
from fsstratify.utils import parse_size_definition

set_simulation_mount_point(Path())


class ProbabilisticModel(UsageModel, model_name="Probabilistic"):
    def __init__(self, config: dict, simulation_dir: Path):
        super().__init__(config, simulation_dir)
        self._steps: int = self._config["steps"]
        self._new_file_size_range = self._get_new_file_size_range()
        self._overwrite_probability = 0.5
        self._operations = get_operations_map()
        self._current_step: int = 0

    def steps(self) -> int:
        """Returns the total number of operations the model is going to perform."""
        return self._steps

    def _validate_config(self) -> None:
        if self._config["steps"] < 1:
            raise ConfigurationError("Error: Number of steps must be > 0.")

    def _get_schema(self) -> dict:
        schema = {
            "steps": Int(),
            "file_size_min": Str(),
            "file_size_max": Str(),
        }
        return schema

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_step >= self._steps:
            raise StopIteration

        self._current_step += 1
        if self._sim_vfs.empty():
            op_names = ["write", "mkdir"]
        else:
            if self._sim_vfs.get_free_space() <= 0:
                op_names = ("cp", "mv", "rm", "write")
            else:
                op_names = ["write", "mkdir", "cp", "mv", "rm"]
                if self._sim_vfs.get_count_of(FileType.REGULAR) > 0:
                    op_names.append("extend")
        op_name = choice(op_names)
        match op_name:
            case "cp" | "mv":
                return self._random_copy_or_move(op_name)
            case "extend":
                return self._random_extend()
            case "mkdir":
                return self._random_mkdir()
            case "rm":
                return self._random_remove()
            case "write":
                return self._random_write()
            case _:  # pragma: no cover
                raise SimulationError(
                    "Error: Unsupported operation. This should not happen and is a bug."
                )

    def _random_remove(self) -> Operation:
        op_name = "rm"
        file = self._sim_vfs.get_random_file()
        playbook_line = f"{op_name} {file.path}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _random_mkdir(self, op_name="mkdir") -> Operation:
        path = self._sim_vfs.get_nonexistent_path()
        playbook_line = f"{op_name} {path}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _random_write(self, op_name="write") -> Operation:
        min_write_size, max_write_size = self._new_file_size_range
        max_write_size = min(self._sim_vfs.get_free_space(), max_write_size)
        free_space = self._sim_vfs.get_free_space()

        if free_space < min_write_size:
            # Special case: When there is not enough free space to write a new file with at
            # least min_write_size, we always overwrite an existing file.
            path = self._sim_vfs.get_random_file(file_type=FileType.REGULAR).path
        elif len(self._sim_vfs.get_files(file_type=FileType.REGULAR)) == 0:
            # Special case: When there are no regular files to overwrite, we always create a new one.
            path = self._sim_vfs.get_nonexistent_path()
        else:
            # Normal case: Overwrite an existing file based on the overwrite probability.
            if random() >= self._overwrite_probability:
                path = self._sim_vfs.get_random_file(file_type=FileType.REGULAR).path
            else:
                path = self._sim_vfs.get_nonexistent_path()

        max_write_size = max(1, max_write_size)
        filesize = randint(*sorted((min_write_size, max_write_size)))
        playbook_line = f"{op_name} {path} size={filesize}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _random_extend(self, op_name="extend") -> Operation:
        file = self._sim_vfs.get_random_file(FileType.REGULAR)
        min_extend_size, max_extend_size = self._get_new_file_size_range()
        filesize = self._sim_vfs.get_size_of(file.path)
        max_extend_size -= filesize
        max_extend_size = min(max_extend_size, self._sim_vfs.get_free_space())
        extend_size = randint(1, max_extend_size)
        playbook_line = f"{op_name} {file.path} extend_size={extend_size}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _random_copy_or_move(self, op_name) -> Operation:
        src = self._sim_vfs.get_random_file()
        if op_name == "cp":
            match src.type:
                case FileType.REGULAR:
                    filesize = self._sim_vfs.get_size_of(src.path)
                    if self._sim_vfs.get_free_space() < filesize:
                        return self._random_remove()
                case FileType.DIRECTORY:
                    dir_size = 0
                    for file_path in self._sim_vfs.get_files_below(src.path):
                        dir_size += self._sim_vfs.get_size_of(file_path)
                    if self._sim_vfs.get_free_space() < dir_size:
                        return self._random_remove()
                case _:  # pragma: no cover
                    raise SimulationError(
                        "Error: Unsupported file type. This should not happen and is a bug."
                    )
        if use_existing_path(0.5):
            if src.type == FileType.REGULAR:
                dst = self._sim_vfs.get_random_file(
                    FileType.REGULAR, files_to_filter_out={src}
                )
                if dst:
                    dst = dst.path
                else:
                    dst = self._sim_vfs.get_nonexistent_path()
            else:
                dst = self._sim_vfs.get_random_file(
                    FileType.DIRECTORY, files_to_filter_out={src}
                )
                if dst:
                    dst = dst.path
                else:
                    dst = self._sim_vfs.get_nonexistent_path(skip_dir=src)
        else:
            if src.type == FileType.DIRECTORY:
                dst = self._sim_vfs.get_nonexistent_path(skip_dir=src)
            else:
                dst = self._sim_vfs.get_nonexistent_path()

        playbook_line = f"{op_name} {src.path} {dst}"
        return self._operations[op_name].from_playbook_line(playbook_line)

    def _get_new_file_size_range(self) -> Tuple[int, int]:
        return (
            parse_size_definition(self._config["file_size_min"]),
            parse_size_definition(self._config["file_size_max"]),
        )
