from pathlib import Path

import pytest

from fsstratify.errors import SimulationError, PlaybookError
from fsstratify.operations import Move


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Move(src=Path("sfile"), dst=Path("dfile")),
            {
                "command": "mv",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            Move(src=Path("/sfile"), dst=Path("/dfile")),
            {
                "command": "mv",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
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
            "mv sfile dfile",
            {
                "command": "mv",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            "mv /sfile /dfile",
            {
                "command": "mv",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            "mv sdir/sfile /ddir/dfile",
            {
                "command": "mv",
                "src": Path("/sdir/sfile"),
                "dst": Path("/ddir/dfile"),
            },
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Move.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "line",
    (
        "mv",
        "mv a",
        "mv a b c",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Move.from_playbook_line(line)


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Move(src=Path("sfile"), dst=Path("dfile")),
            "mv /sfile /dfile",
        ),
        (
            Move(src=Path("/sfile"), dst=Path("/dfile")),
            "mv /sfile /dfile",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Move, expected: str):
    assert op.as_playbook_line() == expected


def test_that_target_returns_the_correct_path():
    assert Move(Path("a"), Path("/b")).target == Path("/b")
    assert Move(Path("a"), Path("b")).target == Path("/b")
    assert Move.from_playbook_line("cp a /b").target == Path("/b")
    assert Move.from_playbook_line("cp a b").target == Path("/b")


def test_that_a_non_existing_src_file_raises(mounted_test_vfs):
    op = Move(src=Path("/notthere"), dst=Path("/dfile"))
    with pytest.raises(SimulationError):
        op.execute()


def test_that_a_file_is_moved_correctly(mounted_test_vfs):
    op = Move(Path("sfile"), Path("dfile"))
    op.execute()
    assert not (mounted_test_vfs.path / "sfile").exists()
    assert (mounted_test_vfs.path / "dfile").is_file()
    assert (
        mounted_test_vfs.path / "dfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_a_directory_is_moved_correctly_when_target_does_not_exist(
    mounted_test_vfs,
):
    op = Move(Path("srcdir"), Path("newdir"))
    op.execute()
    assert not (mounted_test_vfs.path / "srcdir" / "sfile").exists()
    assert not (mounted_test_vfs.path / "srcdir").exists()
    assert (mounted_test_vfs.path / "newdir").is_dir()
    assert (mounted_test_vfs.path / "newdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "newdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["srcdir/sfile"]


def test_that_a_directory_is_moved_correctly_when_target_is_an_existing_directory(
    mounted_test_vfs,
):
    op = Move(Path("srcdir"), Path("dstdir"))
    op.execute()
    assert not (mounted_test_vfs.path / "srcdir" / "sfile").exists()
    assert not (mounted_test_vfs.path / "srcdir").exists()
    assert (mounted_test_vfs.path / "dstdir").is_dir()
    assert (mounted_test_vfs.path / "dstdir" / "srcdir").is_dir()
    assert (mounted_test_vfs.path / "dstdir" / "srcdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dstdir" / "srcdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["srcdir/sfile"]


def test_that_a_file_is_moved_correctly_if_dst_is_a_directory(mounted_test_vfs):
    op = Move(Path("sfile"), Path("dstdir"))
    op.execute()
    assert not (mounted_test_vfs.path / "sfile").is_file()
    assert (mounted_test_vfs.path / "dstdir").is_dir()
    assert (mounted_test_vfs.path / "dstdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dstdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_a_file_is_moved_correctly_if_dst_file_already_exists(mounted_test_vfs):
    op = Move(Path("sfile"), Path("dir/subdir/sfile"))
    op.execute()
    assert not (mounted_test_vfs.path / "sfile").is_file()
    assert (mounted_test_vfs.path / "dir" / "subdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dir" / "subdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_moving_a_directory_to_an_existing_file_raises(mounted_test_vfs):
    op = Move(Path("srcdir"), Path("/sfile"))
    with pytest.raises(SimulationError):
        op.execute()
