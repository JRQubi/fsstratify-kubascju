from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, List, Tuple

import pytest

from fsstratify.errors import SimulationError
from fsstratify.filesystems import (
    set_simulation_mount_point,
    File,
    FileType,
    SimulationVirtualFileSystem,
    FileSystemParser,
)

_FSSTRATIFY_SRC_FILE_CONTENT = "fsstratify unit test data"


@dataclass
class MountedTestVfs:
    path: Path
    file_contents: dict


@pytest.fixture(scope="function")
def mounted_test_vfs(tmpdir) -> MountedTestVfs:
    mnt = Path(tmpdir)
    for folder in ("emptydir", "srcdir", "dstdir", "dir/subdir"):
        (mnt / folder).mkdir(parents=True)
    file_contents = {}
    for file in ("sfile", "srcdir/sfile", "dir/subdir/sfile"):
        with (mnt / file).open("w") as f:
            content = ": ".join((_FSSTRATIFY_SRC_FILE_CONTENT, file))
            f.write(content)
            file_contents[file] = content
    set_simulation_mount_point(mnt)
    return MountedTestVfs(path=mnt, file_contents=file_contents)


class WriteCountSpy:
    def __init__(self):
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1
        if len(args) > 0:
            return len(args[0])


class FakeTestFileSystemParser(FileSystemParser):
    def __init__(self, contents: List[Tuple[File, int]]):
        self._contents = []
        self._sizes = {}
        for entry in contents:
            self._contents.append(entry[0])
            self._sizes[str(entry[0].path)] = entry[1]

    def get_allocated_fragments_for_file(self, path: Path) -> List[Tuple[int, int]]:
        return []

    def get_files(self) -> List[File]:
        return self._get_files()

    def get_size_of(self, path: Path) -> int:
        try:
            return self._sizes[str(path)]
        except KeyError:
            raise SimulationError

    def get_files_below(self, path: Path) -> List[Path]:
        files = []
        for f in self._contents:
            if f.type == FileType.REGULAR and str(f.path).startswith(str(path)):
                files.append(f.path)
        return files

    def _get_files(self) -> List[File]:
        return self._contents.copy()


class FakeSimulationVirtualFileSystem(SimulationVirtualFileSystem):
    _ntuple_diskusage = namedtuple("usage", "total used free")

    def __init__(self, size: int, used: int, contents: List[Tuple[File, int]]):
        self._usage = self._ntuple_diskusage(total=size, used=used, free=size - used)
        self._file_system = FakeTestFileSystemParser(contents)
        self.call_counts = {"get_nonexistent_path": 0, "get_random_file": 0}

    def get_usage(self) -> namedtuple:
        return self._usage

    def set_usage(self, usage: int) -> None:
        self._usage = self._ntuple_diskusage(
            self._usage.total, usage, self._usage.total - usage
        )

    def get_nonexistent_path(
        self, length: int = 8, skip_dir: File | None = None
    ) -> Path:
        self.call_counts["get_nonexistent_path"] += 1
        return super().get_nonexistent_path(length)

    def get_random_file(
        self,
        file_type: Optional[FileType] = None,
        files_to_filter_out: Set[File] | None = None,
        file_filter=None,
    ) -> File:
        self.call_counts["get_random_file"] += 1
        return super().get_random_file(file_type, files_to_filter_out, file_filter)
