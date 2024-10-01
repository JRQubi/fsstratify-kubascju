import os
from pathlib import Path

import pytest

from fsstratify.errors import PlaybookError, SimulationError
from fsstratify.operations import Shrink


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Shrink(Path("sfile"), shrink_size=1024),
            {
                "command": "shrink",
                "path": Path("/sfile"),
                "shrink_size": 1024,
            },
        ),
        (
            Shrink(Path("/sfile"), shrink_size=1024),
            {
                "command": "shrink",
                "path": Path("/sfile"),
                "shrink_size": 1024,
            },
        ),
    ),
)
def test_that_as_dict_works(op, expected):
    assert op.as_dict() == expected


@pytest.mark.parametrize(
    "line,expected",
    (
        (
            "extend sfile shrink_size=1024",
            {
                "command": "shrink",
                "path": Path("/sfile"),
                "shrink_size": 1024,
            },
        ),
        (
            "extend /sfile shrink_size=1024",
            {
                "command": "shrink",
                "path": Path("/sfile"),
                "shrink_size": 1024,
            },
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Shrink.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Shrink(path=Path("sfile"), shrink_size=1024),
            "shrink /sfile shrink_size=1024",
        ),
        (
            Shrink(path=Path("/sfile"), shrink_size=1024),
            "shrink /sfile shrink_size=1024",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Shrink, expected: str):
    assert op.as_playbook_line() == expected


@pytest.mark.parametrize(
    "line",
    (
        "shrink",
        "shrink a",
        "shrink a b",
        "shrink a b c",
        "shrink a shrink_size=0",
        "shrink a shrink_size=-1",
        "shrink a shrink_size=-1KiB",
        "shrink a shrink_size=512X",
        "shrink a shrinksize=512",
        "shrink a shrink_size=512 this=that",
        "shrink shrink_size=512",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Shrink.from_playbook_line(line)


@pytest.mark.parametrize("size", (0, -1, -2, -3, -100))
def test_that_invalid_shrink_sizes_raise_an_error(size):
    with pytest.raises(ValueError):
        Shrink(Path("sfile"), shrink_size=size)


def test_that_a_non_existing_file_raises(mounted_test_vfs):
    with pytest.raises(SimulationError):
        Shrink(path=Path("/notthere"), shrink_size=512).execute()
        Shrink(path=Path("notthere"), shrink_size=512).execute()


def test_that_an_error_is_raised_when_a_directory_is_used(mounted_test_vfs):
    with pytest.raises(SimulationError):
        Shrink(path=Path("/emptydir"), shrink_size=512).execute()


@pytest.mark.parametrize("shrink_size", (1, 2, 3, 5, 10))
def test_that_a_file_is_shrunk_correctly(shrink_size: int, mounted_test_vfs):
    real_path = mounted_test_vfs.path / "sfile"
    old_size = os.stat(real_path).st_size
    Shrink(Path("sfile"), shrink_size=shrink_size).execute()
    assert os.stat(real_path).st_size == old_size - shrink_size


def test_that_a_shrunk_file_has_the_correct_contents(mounted_test_vfs):
    real_path = mounted_test_vfs.path / "sfile"
    Shrink(Path("sfile"), shrink_size=12).execute()
    assert real_path.open().read() == "fsstratify unit test"


@pytest.mark.parametrize("size", (100, 200, 300))
def test_that_a_shrink_size_greater_than_the_file_size_raises_an_error(
    size: int, mounted_test_vfs
):
    with pytest.raises(SimulationError):
        Shrink(Path("sfile"), shrink_size=size).execute()


def test_that_target_returns_the_correct_path():
    assert Shrink(Path("/a/b/c"), shrink_size=1024).target == Path("/a/b/c")
    assert Shrink(Path("a/b/c"), shrink_size=1024).target == Path("/a/b/c")
    assert Shrink.from_playbook_line("extend /a/b/c shrink_size=1024").target == Path(
        "/a/b/c"
    )
    assert Shrink.from_playbook_line("extend a/b/c shrink_size=1024").target == Path(
        "/a/b/c"
    )
