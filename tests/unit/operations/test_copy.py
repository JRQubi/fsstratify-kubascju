from pathlib import Path

import pytest

from fsstratify.errors import SimulationError, PlaybookError
from fsstratify.operations import Copy


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Copy(src=Path("sfile"), dst=Path("dfile")),
            {
                "command": "cp",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            Copy(src=Path("/sfile"), dst=Path("/dfile")),
            {
                "command": "cp",
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
            "cp sfile dfile",
            {
                "command": "cp",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            "cp /sfile /dfile",
            {
                "command": "cp",
                "src": Path("/sfile"),
                "dst": Path("/dfile"),
            },
        ),
        (
            "cp sdir/sfile /ddir/dfile",
            {
                "command": "cp",
                "src": Path("/sdir/sfile"),
                "dst": Path("/ddir/dfile"),
            },
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Copy.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "line",
    (
        "cp",
        "cp a",
        "cp a b c",
        "cp a b this=that",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Copy.from_playbook_line(line)


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Copy(src=Path("sfile"), dst=Path("dfile")),
            "cp /sfile /dfile",
        ),
        (
            Copy(src=Path("/sfile"), dst=Path("/dfile")),
            "cp /sfile /dfile",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Copy, expected: str):
    assert op.as_playbook_line() == expected


def test_that_target_returns_the_correct_path():
    assert Copy(Path("a"), Path("/b")).target == Path("/b")
    assert Copy(Path("a"), Path("b")).target == Path("/b")
    assert Copy.from_playbook_line("cp a /b").target == Path("/b")
    assert Copy.from_playbook_line("cp a b").target == Path("/b")


def test_that_a_non_existing_src_file_raises(mounted_test_vfs):
    op = Copy(src=Path("/notthere"), dst=Path("/dfile"))
    with pytest.raises(SimulationError):
        op.execute()


def test_that_a_file_is_copied_correctly(mounted_test_vfs):
    op = Copy(Path("sfile"), Path("dfile"))
    op.execute()
    assert (mounted_test_vfs.path / "sfile").is_file()
    assert (mounted_test_vfs.path / "dfile").is_file()
    assert (
        mounted_test_vfs.path / "dfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_a_directory_is_copied_correctly(mounted_test_vfs):
    op = Copy(Path("srcdir"), Path("newdir"))
    op.execute()
    assert (mounted_test_vfs.path / "srcdir" / "sfile").is_file()
    assert (mounted_test_vfs.path / "newdir").is_dir()
    assert (mounted_test_vfs.path / "newdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "newdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["srcdir/sfile"]


def test_that_a_directory_is_copied_correctly_when_target_is_an_existing_directory(
    mounted_test_vfs,
):
    op = Copy(Path("srcdir"), Path("dstdir"))
    op.execute()
    assert (mounted_test_vfs.path / "srcdir" / "sfile").is_file()
    assert (mounted_test_vfs.path / "dstdir" / "srcdir").is_dir()
    assert (mounted_test_vfs.path / "dstdir" / "srcdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dstdir" / "srcdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["srcdir/sfile"]


def test_that_a_file_is_copied_correctly_if_dst_is_a_folder(mounted_test_vfs):
    op = Copy(Path("sfile"), Path("dstdir"))
    op.execute()
    assert (mounted_test_vfs.path / "sfile").is_file()
    assert (mounted_test_vfs.path / "dstdir").is_dir()
    assert (mounted_test_vfs.path / "dstdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dstdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_a_file_is_copied_correctly_if_dst_file_already_exists(mounted_test_vfs):
    op = Copy(Path("sfile"), Path("dir/subdir/sfile"))
    op.execute()
    assert (mounted_test_vfs.path / "sfile").is_file()
    assert (mounted_test_vfs.path / "dir" / "subdir" / "sfile").is_file()
    assert (
        mounted_test_vfs.path / "dir" / "subdir" / "sfile"
    ).open().read() == mounted_test_vfs.file_contents["sfile"]


def test_that_copying_a_directory_to_an_existing_file_raises(mounted_test_vfs):
    op = Copy(Path("srcdir"), Path("/sfile"))
    with pytest.raises(SimulationError):
        op.execute()
