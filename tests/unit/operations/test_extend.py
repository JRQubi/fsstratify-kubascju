import os
from pathlib import Path
from unittest.mock import patch, mock_open

import pytest

from fsstratify.errors import PlaybookError, SimulationError
from fsstratify.operations import Extend
from tests.unit.operations.conftest import WriteCountSpy


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Extend(Path("sfile"), extend_size=1024),
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            Extend(Path("/sfile"), extend_size=1024),
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            Extend(Path("sfile"), extend_size=1024, chunked=True),
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": True,
                "chunk_size": 512,
            },
        ),
        (
            Extend(Path("sfile"), extend_size=2048, chunk_size=1024),
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 2048,
                "chunked": False,
                "chunk_size": 1024,
            },
        ),
        (
            Extend(Path("sfile"), extend_size=2048, chunked=True, chunk_size=1024),
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 2048,
                "chunked": True,
                "chunk_size": 1024,
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
            "extend sfile extend_size=1024",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            "extend /sfile extend_size=1024",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            "extend sfile extend_size=1024 chunked=yes",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": True,
                "chunk_size": 512,
            },
        ),
        (
            "extend sfile extend_size=1024 chunk_size=1000",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": False,
                "chunk_size": 1000,
            },
        ),
        (
            "extend sfile extend_size=1024 chunked=True chunk_size=1000",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "extend sfile extend_size=1MiB chunked=True chunk_size=1000",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024**2,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "extend sfile extend_size=1024 chunked=True chunk_size=1kB",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1024,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "extend sfile extend_size=1GB chunked=True chunk_size=1MiB",
            {
                "command": "extend",
                "path": Path("/sfile"),
                "extend_size": 1000**3,
                "chunked": True,
                "chunk_size": 1024**2,
            },
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Extend.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "line",
    (
        "extend",
        "extend a",
        "extend a b",
        "extend a b c",
        "extend a chunk=yes",
        "extend a extend_size=yes",
        "extend a extend_size=0",
        "extend a extend_size=-1",
        "extend a extend_size=512 chunk=yes",
        "extend a extend_size=512 chunk_size=-1",
        "extend a extend_size=512 chunk_size=0",
        "extend a extendsize=512",
        "extend a extend_size=512 chunksize=512",
        "extend a extend_size=512 this=that",
        "extend sfile extend_size=1024 chunked=xyz",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Extend.from_playbook_line(line)


def test_that_chunk_sizes_smaller_than_zero_raise_an_error():
    with pytest.raises(SimulationError):
        Extend(path=Path("path"), extend_size=1024, chunk_size=0)
        Extend(path=Path("path"), extend_size=1024, chunk_size=-1)
        Extend.from_playbook_line("extend a extend_size=512 chunk_size=0")
        Extend.from_playbook_line("extend a extend_size=512 chunk_size=-1")


def test_that_target_returns_the_correct_path():
    assert Extend(Path("/a/b/c"), extend_size=1024).target == Path("/a/b/c")
    assert Extend(Path("a/b/c"), extend_size=1024).target == Path("/a/b/c")
    assert Extend.from_playbook_line("extend /a/b/c extend_size=1024").target == Path(
        "/a/b/c"
    )
    assert Extend.from_playbook_line("extend a/b/c extend_size=1024").target == Path(
        "/a/b/c"
    )


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Extend(path=Path("sfile"), extend_size=1024),
            "extend /sfile extend_size=1024 chunked=False chunk_size=512",
        ),
        (
            Extend(path=Path("/sfile"), extend_size=1024),
            "extend /sfile extend_size=1024 chunked=False chunk_size=512",
        ),
        (
            Extend(path=Path("/sfile"), extend_size=1024, chunked=True),
            "extend /sfile extend_size=1024 chunked=True chunk_size=512",
        ),
        (
            Extend(path=Path("/sfile"), extend_size=1024, chunked=False),
            "extend /sfile extend_size=1024 chunked=False chunk_size=512",
        ),
        (
            Extend(path=Path("/sfile"), extend_size=1024, chunk_size=768),
            "extend /sfile extend_size=1024 chunked=False chunk_size=768",
        ),
        (
            Extend(path=Path("/sfile"), extend_size=1024, chunked=True, chunk_size=768),
            "extend /sfile extend_size=1024 chunked=True chunk_size=768",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Extend, expected: str):
    assert op.as_playbook_line() == expected


def test_that_a_non_existing_file_raises(mounted_test_vfs):
    with pytest.raises(SimulationError):
        Extend(path=Path("/notthere"), extend_size=512).execute()


def test_that_an_error_is_raised_when_a_directory_is_used(mounted_test_vfs):
    with pytest.raises(SimulationError):
        Extend(path=Path("/emptydir"), extend_size=512).execute()


@pytest.mark.parametrize("extend_size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_a_file_is_extended_correctly(extend_size: int, mounted_test_vfs):
    real_path = mounted_test_vfs.path / "sfile"
    old_size = os.stat(real_path).st_size
    Extend(Path("sfile"), extend_size=extend_size).execute()
    assert os.stat(real_path).st_size == old_size + extend_size


@pytest.mark.parametrize("extend_size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_a_file_is_extended_correctly_when_chunkwise_extending_is_turned_on(
    extend_size: int, mounted_test_vfs
):
    real_path = mounted_test_vfs.path / "sfile"
    old_size = os.stat(real_path).st_size
    Extend(Path("sfile"), extend_size=extend_size, chunked=True).execute()
    assert os.stat(real_path).st_size == old_size + extend_size


@pytest.mark.parametrize("chunk_size", (1, 2, 3, 100, 512, 1024, 4096))
@pytest.mark.parametrize("extend_size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_different_chunk_sizes_work(
    chunk_size: int, extend_size: int, mounted_test_vfs
):
    real_path = mounted_test_vfs.path / "sfile"
    old_size = os.stat(real_path).st_size
    Extend(
        Path("sfile"), extend_size=extend_size, chunked=True, chunk_size=chunk_size
    ).execute()
    assert os.stat(real_path).st_size == old_size + extend_size


@pytest.mark.parametrize(
    "extend_size,chunk_size,expected_call_count",
    (
        (1, 1, 1),
        (1, 2, 1),
        (2, 1, 2),
        (2, 2, 1),
        (3, 2, 2),
        (512, 1024, 1),
        (4096, 512, 8),
        (4097, 512, 9),
    ),
)
def test_that_a_file_is_extended_chunkwise_if_chunked_is_active(
    extend_size: int,
    chunk_size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        Extend(
            Path("sfile"),
            extend_size=extend_size,
            chunked=True,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count


def test_that_a_file_is_extended_in_one_chunk_if_chunked_is_not_active(
    mounted_test_vfs,
):
    extend_size = 4096
    chunk_size = 1024
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write.return_value = extend_size
        Extend(
            Path("sfile"),
            extend_size=extend_size,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == 1


@pytest.mark.slow
@pytest.mark.parametrize(
    "extend_size,chunk_size,expected_call_count",
    (
        (1024**2, 1024, 1024),
        (1024**3, 1024, 1024**2),
        (100 * 1024**3, 1024**2, 100 * 1024),
    ),
)
def test_that_large_extend_sizes_work_when_extending_in_chunks(
    extend_size: int,
    chunk_size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write = WriteCountSpy()
        Extend(
            Path("sfile"),
            extend_size=extend_size,
            chunked=True,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count


@pytest.mark.slow
@pytest.mark.parametrize(
    "extend_size,expected_call_count",
    (
        (1024**2, 1),
        (200 * 1024**2, 1),
        (500 * 1024**2, 2),
        (1024**3, 5),
        (100 * 1024**3, 401),
    ),
)
def test_that_large_extend_sizes_work_when_extending_not_in_chunks(
    extend_size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write = WriteCountSpy()
        Extend(
            Path("sfile"),
            extend_size=extend_size,
            chunked=False,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count
