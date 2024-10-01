from pathlib import Path

import pytest

from fsstratify.errors import PlaybookError, SimulationError
from fsstratify.operations import Remove


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Remove(Path("sfile")),
            {"command": "rm", "path": Path("/sfile")},
        ),
        (
            Remove(Path("/sfile")),
            {"command": "rm", "path": Path("/sfile")},
        ),
    ),
)
def test_that_as_dict_works(op, expected):
    assert op.as_dict() == expected


@pytest.mark.parametrize(
    "line,expected",
    (
        ("rm sfile", {"command": "rm", "path": Path("/sfile")}),
        ("rm /sfile", {"command": "rm", "path": Path("/sfile")}),
        (
            "rm sdir/sfile",
            {"command": "rm", "path": Path("/sdir/sfile")},
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Remove.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "line",
    (
        "rm",
        "rm a b",
        "mv a b c",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Remove.from_playbook_line(line)


@pytest.mark.parametrize(
    "op,expected",
    (
        (Remove(path=Path("sfile")), "rm /sfile"),
        (Remove(path=Path("/sfile")), "rm /sfile"),
    ),
)
def test_that_as_playbook_line_works(op: Remove, expected: str):
    assert op.as_playbook_line() == expected


def test_that_target_returns_the_correct_path():
    assert Remove(Path("/a/b/c")).target == Path("/a/b/c")
    assert Remove(Path("a/b/c")).target == Path("/a/b/c")
    assert Remove.from_playbook_line("mkdir /a/b/c").target == Path("/a/b/c")
    assert Remove.from_playbook_line("mkdir a/b/c").target == Path("/a/b/c")


def test_that_a_non_existing_file_raises(mounted_test_vfs):
    op = Remove(path=Path("/notthere"))
    with pytest.raises(SimulationError):
        op.execute()


def test_that_a_file_is_removed_correctly(mounted_test_vfs):
    Remove(Path("sfile")).execute()
    assert not (mounted_test_vfs.path / "sfile").exists()


def test_that_an_empty_directory_is_removed_correctly(mounted_test_vfs):
    Remove(Path("emptydir")).execute()
    assert not (mounted_test_vfs.path / "emptydir").exists()


def test_that_a_directory_with_content_is_removed_correctly(mounted_test_vfs):
    Remove(Path("dir")).execute()
    assert not (mounted_test_vfs.path / "dir").exists()
