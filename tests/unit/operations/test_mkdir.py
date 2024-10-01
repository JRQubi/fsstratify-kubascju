from pathlib import Path

import pytest

from fsstratify.errors import SimulationError, PlaybookError
from fsstratify.filesystems import set_simulation_mount_point
from fsstratify.operations import Mkdir


@pytest.mark.parametrize(
    "line,expected",
    (
        ("mkdir some-dir", {"command": "mkdir", "path": Path("/some-dir")}),
        ("mkdir /some-dir", {"command": "mkdir", "path": Path("/some-dir")}),
        (
            "mkdir some-dir/new-dir",
            {"command": "mkdir", "path": Path("/some-dir/new-dir")},
        ),
        (
            "mkdir /some-dir/new-dir",
            {"command": "mkdir", "path": Path("/some-dir/new-dir")},
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Mkdir.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Mkdir(path=Path("some-dir")),
            {"command": "mkdir", "path": Path("/some-dir")},
        ),
        (
            Mkdir(path=Path("/some-dir")),
            {"command": "mkdir", "path": Path("/some-dir")},
        ),
    ),
)
def test_that_as_dict_works(op, expected):
    assert op.as_dict() == expected


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Mkdir(path=Path("a")),
            "mkdir /a",
        ),
        (
            Mkdir(path=Path("/a")),
            "mkdir /a",
        ),
        (
            Mkdir(path=Path("a/b/c")),
            "mkdir /a/b/c",
        ),
        (
            Mkdir(path=Path("/a/b/c")),
            "mkdir /a/b/c",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Mkdir, expected: str):
    assert op.as_playbook_line() == expected


def test_that_target_returns_the_correct_path():
    assert Mkdir(Path("/a/b/c")).target == Path("/a/b/c")
    assert Mkdir(Path("a/b/c")).target == Path("/a/b/c")
    assert Mkdir.from_playbook_line("mkdir /a/b/c").target == Path("/a/b/c")
    assert Mkdir.from_playbook_line("mkdir a/b/c").target == Path("/a/b/c")


def test_that_a_directory_is_created(tmpdir):
    mnt = Path(tmpdir)
    set_simulation_mount_point(mnt)
    op = Mkdir(Path("/some-dir"))
    op.execute()
    assert (mnt / "some-dir").is_dir()


def test_that_a_directory_with_missing_parents_is_created(mounted_test_vfs):
    op = Mkdir(Path("/a/b/c"))
    op.execute()
    assert (mounted_test_vfs.path / "a" / "b" / "c").is_dir()


def test_that_creating_an_existing_folder_raises(mounted_test_vfs):
    op = Mkdir(Path("srcdir"))
    with pytest.raises(SimulationError):
        op.execute()


def test_that_an_error_is_raised_when_path_is_an_existing_file(mounted_test_vfs):
    op = Mkdir(Path("sfile"))
    with pytest.raises(SimulationError):
        op.execute()


@pytest.mark.parametrize(
    "line",
    (
        "mkdir d this",
        "mkdir d this=that",
    ),
)
def test_that_invalid_parameters_raise(line: str):
    with pytest.raises(PlaybookError):
        Mkdir.from_playbook_line(line).as_dict()
