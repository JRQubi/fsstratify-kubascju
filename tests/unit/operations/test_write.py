import os
from pathlib import Path
from unittest.mock import patch, mock_open

import pytest

from fsstratify.errors import PlaybookError, SimulationError
from fsstratify.operations import Write
from tests.unit.operations.conftest import WriteCountSpy


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Write(Path("sfile"), size=1024),
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            Write(Path("/sfile"), size=1024),
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            Write(Path("sfile"), size=1024, chunked=True),
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": True,
                "chunk_size": 512,
            },
        ),
        (
            Write(Path("sfile"), size=2048, chunk_size=1024),
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 2048,
                "chunked": False,
                "chunk_size": 1024,
            },
        ),
        (
            Write(Path("sfile"), size=2048, chunked=True, chunk_size=1024),
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 2048,
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
            "write sfile size=1024",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            "write /sfile size=1024",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": False,
                "chunk_size": 512,
            },
        ),
        (
            "write sfile size=1024 chunked=yes",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": True,
                "chunk_size": 512,
            },
        ),
        (
            "write sfile size=1024 chunk_size=1000",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": False,
                "chunk_size": 1000,
            },
        ),
        (
            "write sfile size=1024 chunked=True chunk_size=1000",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "write sfile size=1MiB chunked=True chunk_size=1000",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024**2,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "write sfile size=1024 chunked=True chunk_size=1kB",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1024,
                "chunked": True,
                "chunk_size": 1000,
            },
        ),
        (
            "write sfile size=1GB chunked=True chunk_size=1MiB",
            {
                "command": "write",
                "path": Path("/sfile"),
                "size": 1000**3,
                "chunked": True,
                "chunk_size": 1024**2,
            },
        ),
    ),
)
def test_that_a_valid_playbook_line_is_parsed_correctly(line, expected):
    assert Write.from_playbook_line(line).as_dict() == expected


@pytest.mark.parametrize(
    "line",
    (
        "write",
        "write a",
        "write a b",
        "write a b c",
        "write a chunk=yes",
        "write a size=yes",
        "write a size=0",
        "write a size=-1",
        "write a size=512 chunk=yes",
        "write a size=512 chunk=xyz",
        "write a size=512 chunked=y chunk_size=-1",
        "write a size=512 chunked=y chunk_size=0",
        "write a size=1MiB chunked=xyz chunk_size=1000",
        "write a write_size=512",
        "write a size=512 chunksize=512",
        "write a size=512 this=that",
    ),
)
def test_that_invalid_playbook_lines_raise(line):
    with pytest.raises(PlaybookError):
        Write.from_playbook_line(line)


@pytest.mark.parametrize("size", (0, -1, -2, -3, -100))
def test_that_invalid_write_sizes_raise_an_error(size):
    with pytest.raises(SimulationError):
        Write(Path("sfile"), size=size)


def test_that_chunk_sizes_smaller_than_zero_raise_an_error():
    with pytest.raises(SimulationError):
        Write(path=Path("path"), size=1024, chunk_size=0)
        Write(path=Path("path"), size=1024, chunk_size=-1)
        Write.from_playbook_line("write a size=512 chunk_size=0")
        Write.from_playbook_line("write a size=512 chunk_size=-1")


@pytest.mark.parametrize(
    "op,expected",
    (
        (
            Write(path=Path("sfile"), size=1024),
            "write /sfile size=1024 chunked=False chunk_size=512",
        ),
        (
            Write(path=Path("/sfile"), size=1024),
            "write /sfile size=1024 chunked=False chunk_size=512",
        ),
        (
            Write(path=Path("/sfile"), size=1024, chunked=True),
            "write /sfile size=1024 chunked=True chunk_size=512",
        ),
        (
            Write(path=Path("/sfile"), size=1024, chunked=False),
            "write /sfile size=1024 chunked=False chunk_size=512",
        ),
        (
            Write(path=Path("/sfile"), size=1024, chunk_size=768),
            "write /sfile size=1024 chunked=False chunk_size=768",
        ),
        (
            Write(path=Path("/sfile"), size=1024, chunked=True, chunk_size=768),
            "write /sfile size=1024 chunked=True chunk_size=768",
        ),
    ),
)
def test_that_as_playbook_line_works(op: Write, expected: str):
    assert op.as_playbook_line() == expected


def test_that_target_returns_the_correct_path():
    assert Write(Path("/a/b/c"), size=1024).target == Path("/a/b/c")
    assert Write(Path("a/b/c"), size=1024).target == Path("/a/b/c")
    assert Write.from_playbook_line("write /a/b/c size=1024").target == Path("/a/b/c")
    assert Write.from_playbook_line("write a/b/c size=1024").target == Path("/a/b/c")


def test_that_an_error_is_raised_when_a_directory_is_used(mounted_test_vfs):
    with pytest.raises(SimulationError):
        Write(path=Path("/emptydir"), size=512).execute()


@pytest.mark.parametrize("size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_a_file_is_written_correctly(size: int, mounted_test_vfs):
    real_path = mounted_test_vfs.path / "newfile"
    Write(Path("newfile"), size=size).execute()
    assert os.stat(real_path).st_size == size


@pytest.mark.parametrize("size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_a_file_is_overwritten_correctly(size: int, mounted_test_vfs):
    real_path = mounted_test_vfs.path / "sfile"
    Write(Path("sfile"), size=size, chunked=True).execute()
    assert os.stat(real_path).st_size == size


@pytest.mark.parametrize("size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_a_file_is_written_correctly_when_chunkwise_extending_is_turned_on(
    size: int, mounted_test_vfs
):
    real_path = mounted_test_vfs.path / "newfile"
    Write(Path("newfile"), size=size, chunked=True).execute()
    assert os.stat(real_path).st_size == size


@pytest.mark.parametrize("chunk_size", (1, 2, 3, 100, 512, 1024, 4096))
@pytest.mark.parametrize("size", (1, 2, 3, 100, 512, 1024, 4096))
def test_that_different_chunk_sizes_work(size: int, chunk_size: int, mounted_test_vfs):
    real_path = mounted_test_vfs.path / "newfile"
    Write(Path("newfile"), size=size, chunked=True, chunk_size=size).execute()
    assert os.stat(real_path).st_size == size


@pytest.mark.parametrize(
    "size,chunk_size,expected_call_count",
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
def test_that_a_file_is_written_chunkwise_if_chunked_is_active(
    size: int,
    chunk_size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        Write(
            Path("newfile"),
            size=size,
            chunked=True,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count


def test_that_a_file_is_written_in_one_chunk_if_chunked_is_not_active(mounted_test_vfs):
    size = 4096
    chunk_size = 1024
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write.return_value = size
        Write(
            Path("newfile"),
            size=size,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == 1


@pytest.mark.slow
@pytest.mark.parametrize(
    "size,chunk_size,expected_call_count",
    (
        (1024**2, 1024, 1024),
        (1024**3, 1024, 1024**2),
        (100 * 1024**3, 1024**2, 100 * 1024),
    ),
)
def test_that_large_write_sizes_work_when_extending_in_chunks(
    size: int,
    chunk_size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write = WriteCountSpy()
        Write(
            Path("newfile"),
            size=size,
            chunked=True,
            chunk_size=chunk_size,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count


@pytest.mark.slow
@pytest.mark.parametrize(
    "size,expected_call_count",
    (
        (1024**2, 1),
        (200 * 1024**2, 1),
        (500 * 1024**2, 2),
        (1024**3, 5),
        (100 * 1024**3, 401),
    ),
)
def test_that_large_write_sizes_work_when_extending_not_in_chunks(
    size: int,
    expected_call_count: int,
    mounted_test_vfs,
):
    with patch("fsstratify.operations.Path.open", mock_open()) as spy:
        spy.return_value.write = WriteCountSpy()
        Write(
            Path("newfile"),
            size=size,
            chunked=False,
        ).execute()
    assert spy.return_value.write.call_count == expected_call_count
