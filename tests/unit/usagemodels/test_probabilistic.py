from collections import namedtuple, defaultdict
from pathlib import Path
from typing import Tuple, List, Optional, Set

import pytest

from fsstratify.errors import ConfigurationError, SimulationError
from fsstratify.filesystems import (
    SimulationVirtualFileSystem,
    FileSystemParser,
    File,
    FileType,
)
from fsstratify.operations import Write, Mkdir, Extend, Remove, Copy, Move
from fsstratify.usagemodels.probabilistic import ProbabilisticModel
from tests.unit.operations.conftest import FakeSimulationVirtualFileSystem


@pytest.mark.parametrize("steps", (1, 2, 3, 5, 8, 13, 21, 34, 10946, 4807526976))
def test_that_steps_returns_the_correct_number_of_steps(steps: int):
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, [])
    )
    assert model.steps() == steps


@pytest.mark.parametrize(
    "steps", (0, -1, -2, -3, -5, -8, -13, -21, -34, -10946, -4807526976)
)
def test_that_invalid_step_numbers_raise_an_error(steps: int):
    with pytest.raises(ConfigurationError):
        ProbabilisticModel(
            {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}, Path()
        )


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_that_the_correct_number_of_steps_is_generated(steps: int):
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, []),
    )
    step_count = 0
    for _ in model:
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_that_only_valid_ops_for_an_empty_vfs_are_generated(steps: int):
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, []),
    )
    for op in model:
        assert isinstance(op, (Mkdir, Write))


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_that_some_random_samples_for_an_empty_vfs_work(steps: int):
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, [])
    )
    for op in model:
        assert isinstance(op, (Mkdir, Write))
        if isinstance(op, Write):
            assert 1024 <= op.as_dict()["size"] <= 500 * 1000**2


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597, 5000, 10000),
)
def test_that_only_valid_ops_for_a_vfs_with_a_single_file_are_generated(steps: int):
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=15 * 1024**2,
        contents=[(File(type=FileType.REGULAR, path=Path("/some/file")), 15 * 1024**2)],
    )
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, (Copy, Extend, Mkdir, Move, Remove, Write))
        if isinstance(op, Extend):
            assert op.as_dict()["path"] == Path("/some/file")
            assert 1 <= op.as_dict()["extend_size"] <= 508559360
        elif isinstance(op, Remove):
            assert op.as_dict()["path"] == Path("/some/file")
        elif isinstance(op, (Copy, Move)):
            assert op.as_dict()["src"] == Path("/some/file")
            assert op.as_dict()["dst"] != Path("/some/file")
        elif isinstance(op, Mkdir):
            assert op.as_dict()["path"] != Path("/some/file")
        elif isinstance(op, Write):
            assert 1024 <= op.as_dict()["size"] <= 500 * 1024**2
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597, 5000, 10000),
)
def test_that_only_valid_ops_for_a_vfs_with_a_single_folder_are_generated(steps: int):
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=15 * 1024**2,
        contents=[(File(type=FileType.DIRECTORY, path=Path("/some/folder")), 0)],
    )
    folder = Path("/some/folder")
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, (Copy, Mkdir, Move, Remove, Write))
        if isinstance(op, Remove):
            assert op.as_dict()["path"] == folder
        elif isinstance(op, (Copy, Move)):
            assert op.as_dict()["src"] == folder
            assert op.as_dict()["dst"] != folder
        elif isinstance(op, Mkdir):
            assert op.as_dict()["path"] != folder
        elif isinstance(op, Write):
            assert 1024 <= op.as_dict()["size"] <= 500 * 1024**2
            path = op.as_dict()["path"]
            assert path.is_relative_to(folder) or path.parent == Path("/")
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597, 5000, 10000),
)
def test_that_only_valid_ops_for_a_full_vfs_are_generated(steps: int):
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), 16 * 1024**3),
        (File(type=FileType.REGULAR, path=Path("/other/file")), 48 * 1024**3),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=64 * 1024**3,
        contents=contents,
    )
    config = {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"}
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, (Copy, Move, Remove, Write))
        if isinstance(op, Remove):
            assert op.as_dict()["path"] in existing_paths
        elif isinstance(op, Copy):
            assert op.as_dict()["src"] == Path("/some/file")
            assert op.as_dict()["dst"] == Path("/other/file")
        elif isinstance(op, Move):
            assert op.as_dict()["src"] in existing_paths
            assert op.as_dict()["src"] != op.as_dict()["dst"]
        elif isinstance(op, Write):
            assert op.as_dict()["path"] in existing_paths
            assert op.as_dict()["size"] <= fake_vfs.get_size_of(op.as_dict()["path"])
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_correct_behavior_when_extend_is_generated(steps: int, fake_vfs_contents):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        step_count += 1
        if isinstance(op, Extend):
            assert op.as_dict()["path"] in tuple(
                x[0].path for x in fake_vfs_contents if x[0].type == FileType.REGULAR
            )
            assert (
                1024
                <= op.as_dict()["extend_size"]
                <= 500 * 1000**2
                - tuple(
                    x[1] for x in fake_vfs_contents if x[0].path == op.as_dict()["path"]
                )[0]
            )
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_correct_behavior_when_mkdir_is_generated(steps: int, fake_vfs_contents):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        step_count += 1
        if isinstance(op, Mkdir):
            assert op.as_dict()["path"] not in tuple(
                x[0].path for x in fake_vfs_contents
            )
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (100, 200, 300, 500, 800, 1300, 2100, 3400, 5500, 8900, 14400, 159700),
)
def test_correct_behavior_when_mv_is_generated(
    steps: int,
    fake_vfs_contents,
):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        step_count += 1
        if isinstance(op, Move):
            src = op.as_dict()["src"]
            src_type = next(f[0].type for f in fake_vfs_contents if f[0].path == src)
            dst = op.as_dict()["dst"]
            dst_exists = dst in tuple(x[0].path for x in fake_vfs_contents)

            assert src in tuple(x[0].path for x in fake_vfs_contents)
            assert src != dst

            if dst_exists:
                dst_type = next(
                    f[0].type for f in fake_vfs_contents if f[0].path == dst
                )
                if src_type == FileType.DIRECTORY:
                    assert dst_type != FileType.REGULAR
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (250000, 500000, 1000000),
)
def test_that_overwriting_probabilities_are_correct(steps, fake_vfs_contents):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    op_counts = defaultdict(float)
    overwrite_counts = defaultdict(float)
    for op in model:
        step_count += 1
        if isinstance(op, (Copy, Move)):
            src = op.as_dict()["src"]
            dst = op.as_dict()["dst"]
            assert src in tuple(x[0].path for x in fake_vfs_contents)
            assert src != dst
            dst_exists = dst in tuple(x[0].path for x in fake_vfs_contents)
            op_counts[op.as_dict()["command"]] += 1
            if dst_exists:
                overwrite_counts[op.as_dict()["command"]] += 1
        if isinstance(op, Write):
            dst_exists = op.as_dict()["path"] in tuple(
                x[0].path for x in fake_vfs_contents
            )
            op_counts[op.as_dict()["command"]] += 1
            if dst_exists:
                overwrite_counts[op.as_dict()["command"]] += 1

    assert step_count == steps
    assert overwrite_counts.keys() == op_counts.keys()
    for key in op_counts.keys():
        assert overwrite_counts[key] / op_counts[key] == pytest.approx(0.5, abs=0.005)


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_correct_behavior_when_remove_is_generated(steps: int, fake_vfs_contents):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        step_count += 1
        if isinstance(op, Remove):
            assert op.as_dict()["path"] in tuple(x[0].path for x in fake_vfs_contents)
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (50000, 100000, 500000, 1000000),
)
def test_that_only_valid_ops_for_a_vfs_with_files_and_folders_are_generated(
    steps: int, fake_vfs_contents
):
    fake_vfs, config = get_module_test_config_with_data(fake_vfs_contents, steps)
    model = ProbabilisticModel(config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    op_counts = defaultdict(float)
    for op in model:
        step_count += 1

        op_counts[op.as_dict()["command"]] += 1

        assert isinstance(op, (Copy, Extend, Mkdir, Move, Remove, Write))

        if isinstance(op, Remove):
            assert op.as_dict()["path"] in tuple(x[0].path for x in fake_vfs_contents)
        elif isinstance(op, (Copy, Move)):
            assert op.as_dict()["src"] in tuple(x[0].path for x in fake_vfs_contents)
            assert op.as_dict()["src"] != op.as_dict()["dst"]
        elif isinstance(op, Mkdir):
            assert op.as_dict()["path"] != tuple(x[0].path for x in fake_vfs_contents)
        elif isinstance(op, Write):
            assert 1024 <= op.as_dict()["size"] <= 500 * 1024**2
        elif isinstance(op, Extend):
            path = op.as_dict()["path"]
            assert path in tuple(
                x[0].path for x in fake_vfs_contents if x[0].type == FileType.REGULAR
            )
            orig_file_size = next(x[1] for x in fake_vfs_contents if x[0].path == path)
            assert 1 <= op.as_dict()["extend_size"] <= 500 * 1024**2 - orig_file_size
    assert step_count == steps
    expected = 1.0 / len(op_counts.keys())
    for count in op_counts.values():
        assert count / steps == pytest.approx(expected, abs=0.005)


@pytest.fixture
def fake_vfs_contents() -> List[Tuple[File, int]]:
    return [
        (File(type=FileType.DIRECTORY, path=Path("/some/folder")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other/folder")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/folder")), 0),
        (File(type=FileType.REGULAR, path=Path("/file1")), 10),
        (File(type=FileType.REGULAR, path=Path("/other/folder/file2")), 4 * 1024),
        (File(type=FileType.REGULAR, path=Path("/some/folder/file3")), 128 * 1024),
        (File(type=FileType.REGULAR, path=Path("/file4")), 5 * 1024**2),
    ]


def get_module_test_config_with_data(
    vfs_contents: List[Tuple[File, int]], steps: int
) -> Tuple[SimulationVirtualFileSystem, dict]:
    return (
        FakeSimulationVirtualFileSystem(
            size=64 * 1024**3, used=15 * 1024**2, contents=vfs_contents
        ),
        {"steps": steps, "file_size_min": "1KiB", "file_size_max": "500M"},
    )
