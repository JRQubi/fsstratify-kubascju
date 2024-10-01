from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import List, Tuple
from unittest.mock import patch

import pytest

from fsstratify.errors import SimulationError
from fsstratify.filesystems import (
    FileSystemParser,
    File,
    SimulationVirtualFileSystem,
    FileType,
)
from fsstratify.volumes import Volume

_ntuple_diskusage = namedtuple("usage", "total used free")


class FakeVolume(Volume):
    pass


class FakeFileSystemParserWithEmptyFileSystem(FileSystemParser):
    def get_allocated_fragments_for_file(self, path: Path) -> List[Tuple[int, int]]:
        raise SimulationError()

    def get_size_of(self, path: Path) -> int:
        raise SimulationError(f'Error: file "{path}" does not exist.')

    def _get_files(self) -> List[File]:
        return []


class FakeTestFileSystemParser(FileSystemParser):
    def __init__(self, volume: Volume):
        super().__init__(volume)
        self._contents = []
        self._sizes = {}
        self._fragments = {}

    def set_contents(self, contents: List[Tuple[File, int, List[Tuple[int, int]]]]):
        for entry in contents:
            self._contents.append(entry[0])
            self._sizes[str(entry[0].path)] = entry[1]
            self._fragments[str(entry[0].path)] = entry[2]

    def get_allocated_fragments_for_file(self, path: Path) -> List[Tuple[int, int]]:
        try:
            return self._fragments[str(path)]
        except KeyError:
            raise SimulationError

    def get_size_of(self, path: Path) -> int:
        try:
            return self._sizes[str(path)]
        except KeyError:
            raise SimulationError

    def _get_files(self) -> List[File]:
        return self._contents.copy()


FAKE_FILE_SYSTEM_CONTENTS = {
    "fakefs_with_single_file": [
        (File(type=FileType.REGULAR, path=Path("/file1")), 10, [(42, 42)]),
    ],
    "fakefs_with_single_dir": [
        (File(type=FileType.DIRECTORY, path=Path("/dir1")), 10, [(42, 42)]),
    ],
    "fakefs_with_files_and_folders": [
        (File(type=FileType.DIRECTORY, path=Path("/some/folder")), 0, []),
        (File(type=FileType.DIRECTORY, path=Path("/other/folder")), 0, []),
        (File(type=FileType.DIRECTORY, path=Path("/folder")), 0, []),
        (File(type=FileType.REGULAR, path=Path("/file1")), 10, [(11, 11)]),
        (
            File(type=FileType.REGULAR, path=Path("/other/folder/file2")),
            4 * 1024,
            [(42, 43)],
        ),
        (
            File(type=FileType.REGULAR, path=Path("/some/folder/file3")),
            128 * 1024,
            [(100, 111), (200, 222)],
        ),
        (
            File(type=FileType.REGULAR, path=Path("/file4")),
            5 * 1024**2,
            [(1000, 1100), (2000, 2200), (3000, 3300)],
        ),
    ],
}


def fake_get_file_system_parser(fs_type: str, volume: Volume) -> FileSystemParser:
    if fs_type == "fakefs_empty":
        return FakeFileSystemParserWithEmptyFileSystem(volume)
    else:
        parser = FakeTestFileSystemParser(volume)
        parser.set_contents(FAKE_FILE_SYSTEM_CONTENTS[fs_type])
        return parser


@pytest.fixture
def empty_vfs():
    with patch(
        "fsstratify.filesystems.get_file_system_parser",
        side_effect=fake_get_file_system_parser,
    ):
        yield SimulationVirtualFileSystem(FakeVolume({}), Path(), "fakefs_empty")


@contextmanager
def fake_vfs(fake_fs_type: str):
    with patch(
        "fsstratify.filesystems.get_file_system_parser",
        side_effect=fake_get_file_system_parser,
    ):
        yield SimulationVirtualFileSystem(FakeVolume({}), Path(), fake_fs_type)


class TestGetFiles:
    def test_that_an_empty_file_system_returns_an_empty_list(self, empty_vfs):
        assert empty_vfs.get_files() == []
        assert empty_vfs.get_files(FileType.REGULAR) == []
        assert empty_vfs.get_files(FileType.DIRECTORY) == []

    def test_that_an_fs_with_a_single_file_returns_a_list_with_this_file(self):
        with fake_vfs("fakefs_with_single_file") as vfs:
            assert vfs.get_files() == [File(path=Path("/file1"), type=FileType.REGULAR)]

    def test_that_an_fs_with_a_single_folder_returns_a_list_with_this_file(self):
        with fake_vfs("fakefs_with_single_dir") as vfs:
            assert sorted(vfs.get_files(), key=lambda x: x.path) == sorted(
                [
                    File(path=Path("/dir1"), type=FileType.DIRECTORY),
                ],
                key=lambda x: x.path,
            )
            assert sorted(
                vfs.get_files(FileType.DIRECTORY), key=lambda x: x.path
            ) == sorted(
                [
                    File(path=Path("/dir1"), type=FileType.DIRECTORY),
                ],
                key=lambda x: x.path,
            )
            assert vfs.get_files(FileType.REGULAR) == []

    def test_that_a_populated_fs_returns_the_correct_files(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            assert sorted(vfs.get_files(), key=lambda x: x.path) == sorted(
                [
                    File(FileType.DIRECTORY, Path("/some/folder")),
                    File(FileType.DIRECTORY, Path("/other/folder")),
                    File(FileType.DIRECTORY, Path("/folder")),
                    File(type=FileType.REGULAR, path=Path("/file1")),
                    File(FileType.REGULAR, Path("/other/folder/file2")),
                    File(FileType.REGULAR, Path("/some/folder/file3")),
                    File(FileType.REGULAR, Path("/file4")),
                ],
                key=lambda x: x.path,
            )


class TestEmpty:
    def test_that_an_empty_file_system_returns_true(self, empty_vfs):
        assert empty_vfs.empty()

    def test_that_a_file_system_with_a_single_file_returns_false(self):
        with fake_vfs("fakefs_with_single_file") as vfs:
            assert not vfs.empty()

    def test_that_a_file_system_with_a_single_folder_returns_false(self):
        with fake_vfs("fakefs_with_single_dir") as vfs:
            assert not vfs.empty()

    def test_that_a_populated_fs_returns_false(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            assert not vfs.empty()


class TestGetUsage:
    def test_that_correct_values_are_returned(self, empty_vfs):
        with patch(
            "shutil.disk_usage",
            return_value=_ntuple_diskusage(total=100, used=0, free=100),
        ):
            usage = empty_vfs.get_usage()
            assert usage.total == 100
            assert usage.used == 0
            assert usage.free == 100


class TestGetFreeSpace:
    @pytest.mark.parametrize(
        "total,used,free,expected",
        ((100, 0, 100, 100), (100, 50, 50, 50), (100, 10, 90, 90)),
    )
    def test_that_a_correct_value_is_returned(
        self, total: int, used: int, free: int, expected: int, empty_vfs
    ):
        with patch(
            "shutil.disk_usage",
            return_value=_ntuple_diskusage(total=total, used=used, free=free),
        ):
            assert empty_vfs.get_free_space() == expected


class TestGetRandomFile:
    def test_that_an_empty_file_system_raises_an_error(self, empty_vfs):
        with pytest.raises(SimulationError):
            assert empty_vfs.get_random_file()

    def test_that_an_empty_file_system_raises_an_error_for_directories(self, empty_vfs):
        with pytest.raises(SimulationError):
            assert empty_vfs.get_random_file(FileType.DIRECTORY)

    def test_that_an_empty_file_system_raises_an_error_for_regular_files(
        self, empty_vfs
    ):
        with pytest.raises(SimulationError):
            assert empty_vfs.get_random_file(FileType.REGULAR)

    def test_that_an_fs_with_a_single_file_returns_that_file(self):
        with fake_vfs("fakefs_with_single_file") as vfs:
            expected = File(FileType.REGULAR, Path("/file1"))
            assert vfs.get_random_file() == expected
            assert vfs.get_random_file(FileType.REGULAR) == expected

    def test_that_an_fs_with_a_single_file_raises_when_dirs_are_requested(self):
        with fake_vfs("fakefs_with_single_file") as vfs, pytest.raises(SimulationError):
            assert vfs.get_random_file(FileType.DIRECTORY)


class TestGetNonexistentPath:
    def test_that_an_empty_file_system_returns_a_path_in_the_root_directory(
        self, empty_vfs
    ):
        with patch("fsstratify.filesystems.get_path_in_mount_point") as m:
            m.return_value.exists.return_value = False
            assert empty_vfs.get_nonexistent_path().parent == Path("/")

    @pytest.mark.parametrize("length", (1, 2, 3, 5, 8, 13, 21, 34, 128, 250, 255))
    def test_that_the_file_name_has_the_correct_length(self, length: int, empty_vfs):
        with patch("fsstratify.filesystems.get_path_in_mount_point") as m:
            m.return_value.exists.return_value = False
            assert len(empty_vfs.get_nonexistent_path(length).name) == length

    @pytest.mark.parametrize("length", (256, 257, 1024, 4096))
    def test_that_exceeding_the_max_file_name_length_raises_an_error(
        self, length: int, empty_vfs
    ):
        with pytest.raises(SimulationError):
            empty_vfs.get_nonexistent_path(length)

    def test_that_no_existing_path_is_generated(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs, patch(
            "fsstratify.filesystems.get_path_in_mount_point"
        ) as m:
            m.return_value.exists.return_value = False
            for _ in range(10000):
                assert vfs.get_nonexistent_path() not in vfs.get_files()

    def test_that_an_error_is_raised_when_no_path_can_be_generated(self):
        with patch("fsstratify.filesystems.get_path_in_mount_point"), fake_vfs(
            "fakefs_empty"
        ) as vfs, pytest.raises(SimulationError):
            assert vfs.get_nonexistent_path(1)


class TestGetCountOf:
    def test_that_an_empty_file_system_returns_zero_for_regular_files(self, empty_vfs):
        assert empty_vfs.get_count_of(FileType.REGULAR) == 0

    def test_that_an_empty_file_system_returns_zero_for_regular_directories(
        self, empty_vfs
    ):
        assert empty_vfs.get_count_of(FileType.DIRECTORY) == 0

    def test_that_an_fs_with_a_single_file_returns_correct_values(self):
        with fake_vfs("fakefs_with_single_file") as vfs:
            assert vfs.get_count_of(FileType.REGULAR) == 1
            assert vfs.get_count_of(FileType.DIRECTORY) == 0

    def test_that_an_fs_with_a_single_folder_returns_correct_values(self):
        with fake_vfs("fakefs_with_single_dir") as vfs:
            assert vfs.get_count_of(FileType.REGULAR) == 0
            assert vfs.get_count_of(FileType.DIRECTORY) == 1

    def test_that_a_populated_fs_returns_correct_values(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            assert vfs.get_count_of(FileType.REGULAR) == 4
            assert vfs.get_count_of(FileType.DIRECTORY) == 3


class TestGetSizeOf:
    @pytest.mark.parametrize(
        "path",
        (
            Path(),
            Path("/some/file"),
            Path("some/file"),
            Path("file"),
            Path("/file"),
        ),
    )
    def test_that_an_empty_file_system_raises_an_error(self, empty_vfs, path: Path):
        with pytest.raises(SimulationError):
            assert empty_vfs.get_size_of(path)

    def test_that_the_correct_sizes_are_returned(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            assert vfs.get_size_of(Path("/file1")) == 10
            assert vfs.get_size_of(Path("/other/folder/file2")) == 4 * 1024
            assert vfs.get_size_of(Path("/some/folder/file3")) == 128 * 1024
            assert vfs.get_size_of(Path("/file4")) == 5 * 1024**2


class TestGetAllocatedFragmentsForFile:
    def test_that_an_empty_file_system_raises_an_error(self, empty_vfs):
        with pytest.raises(SimulationError):
            assert empty_vfs.get_allocated_fragments_for_file(Path())

    def test_that_the_correct_fragments_are_returned(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            assert vfs.get_allocated_fragments_for_file(Path("/some/folder")) == []
            assert vfs.get_allocated_fragments_for_file(Path("/other/folder")) == []
            assert vfs.get_allocated_fragments_for_file(Path("/folder")) == []
            assert vfs.get_allocated_fragments_for_file(Path("/file1")) == [(11, 11)]
            assert vfs.get_allocated_fragments_for_file(
                Path("/other/folder/file2")
            ) == [(42, 43)]
            assert vfs.get_allocated_fragments_for_file(Path("/some/folder/file3")) == [
                (100, 111),
                (200, 222),
            ]
            assert vfs.get_allocated_fragments_for_file(Path("/file4")) == [
                (1000, 1100),
                (2000, 2200),
                (3000, 3300),
            ]


class TestCollectImmutableFiles:
    def test_that_an_empty_file_system_creates_an_empty_list(self, empty_vfs):
        empty_vfs.collect_immutable_files()
        assert empty_vfs.get_files() == []

    def test_that_a_single_file_is_collected_correctly(self):
        with fake_vfs("fakefs_with_single_file") as vfs:
            vfs.collect_immutable_files()
            assert vfs.get_files() == []
            assert vfs.get_files(FileType.REGULAR) == []
            assert vfs.get_files(FileType.DIRECTORY) == []

    def test_that_a_single_folder_is_collected_correctly(self):
        with fake_vfs("fakefs_with_single_dir") as vfs:
            vfs.collect_immutable_files()
            assert vfs.get_files() == []
            assert vfs.get_files(FileType.REGULAR) == []
            assert vfs.get_files(FileType.DIRECTORY) == []

    def test_that_files_of_a_populated_are_collected_correctly(self):
        with fake_vfs("fakefs_with_files_and_folders") as vfs:
            vfs.collect_immutable_files()
            assert vfs.get_files() == []
            assert vfs.get_files(FileType.REGULAR) == []
            assert vfs.get_files(FileType.DIRECTORY) == []


class _TestGetFileSystemAreas:
    def test_that_an_empty_file_system_returns_an_empty_list(self, empty_vfs):
        assert empty_vfs.get_file_system_areas == []
