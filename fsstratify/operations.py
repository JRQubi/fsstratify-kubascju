"""This module contains the operations to be carried out on the mounted file systems."""

import shutil
from abc import ABC
from pathlib import Path
from random import randbytes
from typing import Optional

from tqdm import tqdm

from fsstratify.errors import SimulationError, PlaybookError
from fsstratify.filesystems import get_path_in_mount_point
from fsstratify.utils import parse_size_definition, parse_boolean_string

_operations_registry: dict = {}

_MAX_CHUNK_SIZE = 2**28 - 1
"""This is the maximal size that random.randbytes accepts."""


def single_step_progress_bar(method):
    def inner(ref):
        pbar = tqdm(
            total=1, desc=f"  current operation - {ref.playbook_command}", leave=False
        )
        method(ref)
        pbar.update(1)

    return inner


def get_operations_map() -> dict:
    """Return a mapping of available operations and their implementations."""
    ops = _operations_registry.copy()
    # Remove (base) classes that are not intended to be used directly.
    ops.pop("filewriteoperation")
    return ops


class Operation:
    """The base class for all file system operations.

    All actual implementations have to be derived from this class so that they are
    automatically added to the list of supported operations.
    """

    def __init_subclass__(cls, **kwargs):
        if "playbook_command" in kwargs:
            pb_cmd = kwargs.pop("playbook_command")
        else:
            pb_cmd = cls.__name__.lower()
        super().__init_subclass__(**kwargs)
        cls.playbook_command = pb_cmd
        _operations_registry[pb_cmd] = cls

    def execute(self):  # pragma: no cover
        """This method implemented the actual functionality of the operation."""
        raise NotImplementedError

    def as_dict(self):  # pragma: no cover
        """Return the current operation as dict."""
        raise NotImplementedError

    def as_playbook_line(self):  # pragma: no cover
        """Return the current operation as playbook line."""
        raise NotImplementedError

    @property
    def target(self):  # pragma: no cover
        raise NotImplementedError

    @classmethod
    def from_playbook_line(cls, line: str):  # pragma: no cover
        """Create an operation instance based on a given playbook line."""
        raise NotImplementedError

    @staticmethod
    def _normalize_simulation_path(path: Path) -> Path:
        if path.is_absolute():
            return path
        return Path("/") / path


class FileWriteOperation(Operation, ABC):
    def __init__(self, path: Path, write_size: int):
        if write_size <= 0:
            raise SimulationError(
                f"{self.__class__.__name__} "
                "operation cannot be performed with write size < 0"
            )
        self._path = self._normalize_simulation_path(path)
        self._real_path: Optional[Path] = None
        self._write_size = write_size

    def _write(self, chunk_size: int) -> None:
        self._write_to_file(chunk_size, "w")

    def _append(self, chunk_size: int) -> None:
        self._write_to_file(chunk_size, "a")

    def _write_to_file(self, chunk_size: int, mode: str) -> None:
        self._assert_path_is_valid()
        byte_num_to_add = self._write_size
        with (
            self._real_path.open(f"{mode}b") as f,
            tqdm(
                total=self._write_size,
                desc=f"  current operation - {self.playbook_command}",
                leave=False,
            ) as pbar,
        ):
            while byte_num_to_add != 0:
                byte_num_for_step = (
                    chunk_size if (chunk_size <= byte_num_to_add) else byte_num_to_add
                )
                f.write(randbytes(byte_num_for_step))
                byte_num_to_add -= byte_num_for_step
                pbar.update(byte_num_for_step)

    def _assert_path_is_valid(self) -> None:
        if self._real_path.is_dir():
            raise SimulationError(
                f'"{self.playbook_command}" \
                "is an unsupported operation for directories (path: "{self._path}").'
            )


class Copy(Operation, playbook_command="cp"):
    """Implements a file copy operation.

    Both paths (``src`` and ``dst``) are relative to the simulation mount point. Copy
    can not be used to copy files from the corpus directory to the simulation file
    system; use Add for that.

    Args:
        src: source path
        dst: destination path
    """

    def __init__(self, src: Path, dst: Path):
        self._src = self._normalize_simulation_path(src)
        self._dst = self._normalize_simulation_path(dst)

    @single_step_progress_bar
    def execute(self) -> None:
        src = get_path_in_mount_point(self._src)
        dst = get_path_in_mount_point(self._dst)
        if not src.exists():
            raise SimulationError(f'Source file "{self._src}" does not exist.')
        if src.is_file():
            shutil.copy(src, dst)
        elif src.is_dir():
            if not dst.exists():
                shutil.copytree(src, dst)
            elif dst.is_dir():
                shutil.copytree(src, (dst / src.name))
            elif dst.is_file():
                raise SimulationError(
                    f'Can\'t copy directory "{self._src}" '
                    f'to existing file "{self._dst}".'
                )
        else:
            raise SimulationError(
                f"Unsupported file type for Copy (path: {self._src})."
            )

    @property
    def target(self) -> Path:
        return self._dst

    def as_dict(self) -> dict:
        return {"command": self.playbook_command, "src": self._src, "dst": self._dst}

    def as_playbook_line(self):
        return f"{self.playbook_command} {self._src} {self._dst}"

    @classmethod
    def from_playbook_line(cls, line: str):
        args = tuple(Path(p) for p in line.split()[1:])
        if len(args) != 2:
            raise PlaybookError(f'Invalid playbook line: "{line}".')
        return cls(*args)


class Mkdir(Operation):
    """Creates a new empty directory."""

    def __init__(self, path: Path):
        self._path = self._normalize_simulation_path(path)

    @single_step_progress_bar
    def execute(self):
        real_path = get_path_in_mount_point(self._path)
        try:
            real_path.mkdir(parents=True, exist_ok=False)
        except Exception as err:
            raise SimulationError(err)

    @property
    def target(self):
        return self._path

    def as_dict(self):
        return {"command": self.playbook_command, "path": self._path}

    def as_playbook_line(self):
        return f"{self.playbook_command} {self._path}"

    @classmethod
    def from_playbook_line(cls, line: str):
        args = tuple(Path(p) for p in line.split()[1:])
        if len(args) != 1:
            raise PlaybookError(f'Invalid playbook line: "{line}".')
        return cls(args[0])


class Move(Operation, playbook_command="mv"):
    """Move a file or directory."""

    def __init__(self, src: Path, dst: Path):
        self._src = self._normalize_simulation_path(src)
        self._dst = self._normalize_simulation_path(dst)

    @single_step_progress_bar
    def execute(self):
        src = get_path_in_mount_point(self._src)
        dst = get_path_in_mount_point(self._dst)
        if not src.exists():
            raise SimulationError(f'Source file "{self._src}" does not exist.')
        if src.is_dir() and dst.is_file():
            raise SimulationError(
                f'Can\'t move directory "{self._src}" to existing file "{self._dst}".'
            )
        shutil.move(src, dst)

    @property
    def target(self):
        return self._dst

    def as_dict(self):
        return {"command": "mv", "src": self._src, "dst": self._dst}

    def as_playbook_line(self):
        return f"{self.playbook_command} {self._src} {self._dst}"

    @classmethod
    def from_playbook_line(cls, line: str):
        args = tuple(Path(p) for p in line.split()[1:])
        if len(args) != 2:
            raise PlaybookError(f'Invalid playbook line: "{line}".')
        return cls(*args)


class Remove(Operation, playbook_command="rm"):
    """Delete a file or directory."""

    def __init__(self, path: Path):
        self._path = self._normalize_simulation_path(path)

    @single_step_progress_bar
    def execute(self):
        real_path = get_path_in_mount_point(self._path)
        if not real_path.exists():
            raise SimulationError(f'File "{self._path}" does not exist.')
        if real_path.is_file():
            real_path.unlink()
        elif real_path.is_dir():
            if len(tuple(real_path.iterdir())) == 0:
                real_path.rmdir()
            else:
                shutil.rmtree(real_path)
        else:
            raise SimulationError(
                f"Unsupported file type for Remove (path: {self._path})."
            )

    @property
    def target(self):
        return self._path

    def as_dict(self):
        return {"command": self.playbook_command, "path": self._path}

    def as_playbook_line(self):
        return f"{self.playbook_command} {self._path}"

    @classmethod
    def from_playbook_line(cls, line: str):
        args = tuple(Path(p) for p in line.split()[1:])
        if len(args) != 1:
            raise PlaybookError(f'Invalid playbook line: "{line}".')
        return cls(args[0])


class Shrink(Operation):
    """Shrink an existing file."""

    def __init__(self, path: Path, shrink_size: int):
        self._path = self._normalize_simulation_path(path)
        self._shrink_size = int(shrink_size)
        if self._shrink_size < 1:
            raise ValueError("shrink_size has to be >= 1.")

    @single_step_progress_bar
    def execute(self):
        real_path = get_path_in_mount_point(self._path)
        self._assert_path_is_valid(real_path)
        file_size = real_path.stat().st_size
        new_file_size = file_size - self._shrink_size
        if new_file_size < 0:
            raise SimulationError(
                "shrink_size is greater than original file size "
                f"(path={self._path}, file size: {file_size}, shrink_size: {self._shrink_size})."
            )
        with real_path.open("a") as fd:
            fd.truncate(new_file_size)

    @property
    def target(self):
        return self._path

    def as_dict(self):
        return {
            "command": self.playbook_command,
            "path": self._path,
            "shrink_size": self._shrink_size,
        }

    def as_playbook_line(self):
        return f"{self.playbook_command} {self._path} shrink_size={self._shrink_size}"

    @classmethod
    def from_playbook_line(cls, line: str):
        args = {}
        parameters = line.split()[1:]
        if len(parameters) < 2:
            raise PlaybookError(
                f'Invalid playbook line: "{line}". Not enough arguments.'
            )
        args["path"] = Path(parameters[0])
        for param in parameters[1:]:
            if param.startswith("shrink_size="):
                try:
                    args["shrink_size"] = parse_size_definition(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
                if args["shrink_size"] <= 0:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". shrink_size has to be > 0.'
                    )
            else:
                raise PlaybookError(
                    f'Invalid playbook line: "{line}". Unknown parameter "{param}.'
                )
        return cls(**args)

    def _assert_path_is_valid(self, real_path: Path):
        if not real_path.exists():
            raise SimulationError(f'File "{self._path}" does not exist.')
        if real_path.is_dir():
            raise SimulationError(
                f'"{self.playbook_command}" \
                "is an unsupported operation for directories (path: "{self._path}").'
            )


class Extend(FileWriteOperation):
    """Extend an existing file."""

    def __init__(
        self, path: Path, extend_size: int, chunked: bool = False, chunk_size: int = 512
    ):
        super().__init__(path, extend_size)
        if chunk_size <= 0:
            raise SimulationError(
                "Extend operation cannot be performed with chunk_size <= 0."
            )
        self._chunked = chunked
        self._chunk_size = chunk_size

    def execute(self):
        self._real_path = get_path_in_mount_point(self._path)
        if not self._real_path.exists():
            raise SimulationError(f'File "{self._path}" does not exist.')
        chunk_size = _MAX_CHUNK_SIZE
        if self._chunked:
            chunk_size = self._chunk_size
        self._append(chunk_size)

    def as_dict(self):
        return {
            "command": self.playbook_command,
            "path": self._path,
            "extend_size": self._write_size,
            "chunked": self._chunked,
            "chunk_size": self._chunk_size,
        }

    def as_playbook_line(self):
        return (
            f"{self.playbook_command} {self._path} extend_size={self._write_size} "
            f"chunked={self._chunked} chunk_size={self._chunk_size}"
        )

    @property
    def target(self):
        return self._path

    @classmethod
    def from_playbook_line(cls, line: str):
        args = {}
        parameters = line.split()[1:]
        if len(parameters) < 2:
            raise PlaybookError(
                f'Invalid playbook line: "{line}". Not enough arguments.'
            )
        args["path"] = Path(parameters[0])
        for param in parameters[1:]:
            if param.startswith("chunked="):
                try:
                    args["chunked"] = parse_boolean_string(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
            elif param.startswith("chunk_size="):
                try:
                    args["chunk_size"] = parse_size_definition(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
                if args["chunk_size"] <= 0:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". chunk_size has to be > 0.'
                    )
            elif param.startswith("extend_size="):
                try:
                    args["extend_size"] = parse_size_definition(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
                if args["extend_size"] <= 0:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". extend_size has to be > 0.'
                    )
            else:
                raise PlaybookError(
                    f'Invalid playbook line: "{line}". Unknown parameter "{param}.'
                )
        return cls(**args)


class Write(FileWriteOperation):
    """(Re-)Write a file."""

    def __init__(
        self, path: Path, size: int, chunked: bool = False, chunk_size: int = 512
    ):
        super().__init__(path, size)
        if chunk_size <= 0:
            raise SimulationError(
                "Write operation cannot be performed with chunk_size <= 0."
            )
        self._chunked = chunked
        self._chunk_size = chunk_size

    def execute(self):
        self._real_path = get_path_in_mount_point(self._path)
        chunk_size = _MAX_CHUNK_SIZE
        if self._chunked:
            chunk_size = self._chunk_size
        self._write(chunk_size)

    def as_dict(self):
        return {
            "command": self.playbook_command,
            "path": self._path,
            "size": self._write_size,
            "chunked": self._chunked,
            "chunk_size": self._chunk_size,
        }

    def as_playbook_line(self):
        return (
            f"{self.playbook_command} {self._path} size={self._write_size} "
            f"chunked={self._chunked} chunk_size={self._chunk_size}"
        )

    @property
    def target(self):
        return self._path

    @classmethod
    def from_playbook_line(cls, line: str):
        args = {}
        parameters = line.split()[1:]
        if len(parameters) < 2:
            raise PlaybookError(
                f'Invalid playbook line: "{line}". Not enough arguments.'
            )
        args["path"] = Path(parameters[0])
        for param in parameters[1:]:
            if param.startswith("chunked="):
                try:
                    args["chunked"] = parse_boolean_string(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
            elif param.startswith("chunk_size="):
                try:
                    args["chunk_size"] = parse_size_definition(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
                if args["chunk_size"] <= 0:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". chunk_size has to be > 0.'
                    )
            elif param.startswith("size="):
                try:
                    args["size"] = parse_size_definition(param.split("=")[1])
                except ValueError:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". Invalid parameter "{param}.'
                    )
                if args["size"] <= 0:
                    raise PlaybookError(
                        f'Invalid playbook line: "{line}". size has to be > 0.'
                    )
            else:
                raise PlaybookError(
                    f'Invalid playbook line: "{line}". Unknown parameter "{param}.'
                )
        return cls(**args)
