import random
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple

import pytest

from fsstratify.errors import ConfigurationError
from fsstratify.filesystems import (
    File,
    FileType,
)
from fsstratify.operations import Remove, Write, Extend, Shrink
from fsstratify.usagemodels.kad import KADModel
from tests.unit.operations.conftest import FakeSimulationVirtualFileSystem


@pytest.mark.parametrize("steps", (1, 2, 3, 5, 8, 13, 21, 34, 10946, 4807526976))
def test_that_steps_returns_the_correct_number_of_steps(steps: int, model_config):
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, [])
    )
    assert model.steps() == steps


@pytest.mark.parametrize("steps", (-1, -2, -3, -5, -8, 0))
def test_that_steps_smaller_than_one_raise_an_error(steps, model_config):
    model_config["steps"] = steps
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("chunk_size", (-1, -2, -3, -5, -8, 0))
def test_that_chunk_sizes_smaller_than_one_raise_an_error(chunk_size, model_config):
    model_config["chunk_size"] = chunk_size
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("parameter", ("write", "increase", "delete", "decrease"))
@pytest.mark.parametrize("weight", (-1, -2, -3, -5, -8))
def test_that_invalid_op_factors_raise_an_error(parameter, weight, model_config):
    model_config["operation_factors"][parameter] = weight
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


def test_that_at_least_one_operation_factor_is_not_zero(model_config):
    model_config["operation_factors"]["write"] = 0
    model_config["operation_factors"]["delete"] = 0
    model_config["operation_factors"]["increase"] = 0
    model_config["operation_factors"]["decrease"] = 0
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("start,stop", ((0.00001, 0), (0.2, 0.1), (1, 0.99999)))
def test_that_write_limit_start_larger_than_stop_raises_an_error(
    start, stop, model_config
):
    model_config["write_limit"]["start"] = start
    model_config["write_limit"]["stop"] = stop
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("start,stop", ((0, 0.00001), (0.1, 0.2), (0.99, 1)))
def test_that_delete_limit_start_smaller_than_stop_raises_an_error(
    start, stop, model_config
):
    model_config["delete_limit"]["start"] = start
    model_config["delete_limit"]["stop"] = stop
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "start,stop",
    (
        (-0.1, 0.1),
        (-0.9999, 0.9999),
        (-0.5, -0.4),
        (-0.00001, 0),
        (1, 1.000001),
        (1, 2),
        (2, 3),
    ),
)
def test_that_write_limits_are_between_0_and_1(start, stop, model_config):
    model_config["write_limit"]["start"] = start
    model_config["write_limit"]["stop"] = stop
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "start,stop",
    (
        (-0.1, 0.1),
        (0.1, -0.1),
        (-0.9999, 0.9999),
        (0.9999, -0.9999),
        (-0.5, -0.4),
        (-0.000001, 0),
        (1.11111, 1),
        (2, 1),
        (1, 2),
        (3, 2),
    ),
)
def test_that_delete_limits_are_between_0_and_1(start, stop, model_config):
    model_config["delete_limit"]["start"] = start
    model_config["delete_limit"]["stop"] = stop
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "factors",
    (
        (
            [],
            [{"size": -1, "weight": 1}],
            [{"size": -13, "weight": 1}],
            [{"size": 1, "weight": -1}],
            [{"size": 1, "weight": -13}],
        )
    ),
)
def test_that_invalid_size_factors_raise_an_error(factors, model_config):
    model_config["size_factors"] = factors
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


def test_that_no_size_factors_raise_an_error(model_config):
    model_config["size_factors"] = []
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())
    del model_config["size_factors"]
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("invalid", (-1, -2, -3, -5, -8))
def test_that_an_invalid_random_range_min_raises_an_error(invalid, model_config):
    model_config["random_range"]["min"] = invalid
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize("invalid", (0, -1, -2, -3, -5, -8))
def test_that_an_invalid_random_range_max_raises_an_error(invalid, model_config):
    model_config["random_range"]["max"] = invalid
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "write_limit,delete_limit",
    (
        ({"start": 0, "stop": 0.3}, {"start": 0.2, "stop": 0.1}),
        ({"start": 0.1, "stop": 0.1}, {"start": 0.1, "stop": 0}),
    ),
)
def test_that_overlapping_write_and_delete_limit_raise_an_error(
    write_limit, delete_limit, model_config
):
    model_config["write_limit"] = write_limit
    model_config["delete_limit"] = delete_limit
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "write_limit,delete_limit",
    (
        ({"start": 0.6, "stop": 0.7}, {"start": 0.4, "stop": 0.2}),
        ({"start": 1, "stop": 1}, {"start": 0, "stop": 0}),
    ),
)
def test_that_write_limit_after_delete_limit_raises_an_error(
    write_limit, delete_limit, model_config
):
    model_config["write_limit"] = write_limit
    model_config["delete_limit"] = delete_limit
    with pytest.raises(ConfigurationError):
        KADModel(model_config, Path())


@pytest.mark.parametrize(
    "steps",
    (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 1597),
)
def test_that_the_correct_number_of_steps_is_generated(steps: int, model_config: dict):
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
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
def test_that_only_valid_ops_are_generated(steps: int, model_config: dict):
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, []),
    )
    step_count = 0
    for op in model:
        step_count += 1
        assert isinstance(op, (Extend, Remove, Shrink, Write))
    assert step_count == steps


@pytest.mark.parametrize(
    "file_size",
    (
        48 * 1024**3,
        47 * 1024**3,
        46 * 1024**3,
        45 * 1024**3,
        # the next one is one byte more than (disk_size - file_size_1) * 0.95
        48103633716,
    ),
)
def test_that_only_remove_ops_are_generated_when_delete_limit_is_hit(
    model_config: dict, file_size: int
):
    steps = 10000
    file_size_1 = 16 * 1024**3
    file_size_2 = file_size
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), file_size_1),
        (File(type=FileType.REGULAR, path=Path("/other/file")), file_size_2),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=file_size_1 + file_size_2,
        contents=contents,
    )
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, Remove)
        assert op.as_dict()["path"] in existing_paths
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "file_size",
    (1, 512, 1024, 3435973835),
)
def test_that_only_write_ops_are_generated_when_write_limit_is_hit(
    model_config: dict, file_size: int
):
    steps = 10000
    file_size_1 = 1
    file_size_2 = file_size
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), file_size_1),
        (File(type=FileType.REGULAR, path=Path("/other/file")), file_size_2),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=file_size_1 + file_size_2,
        contents=contents,
    )
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, Write)
        assert op.as_dict()["path"] not in existing_paths
        step_count += 1
    assert step_count == steps


def test_that_write_mode_is_left_when_write_stop_is_reached(model_config: dict):
    steps = 400000
    file_size_1 = 1024
    file_size_2 = 1024
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), file_size_1),
        (File(type=FileType.REGULAR, path=Path("/other/file")), file_size_2),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fs_size = 64 * 1024**3
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=fs_size,
        used=file_size_1 + file_size_2,
        contents=contents,
    )
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    regular_steps_count = 0
    step_when_write_stop_limit_is_hit = 10000
    write_limit_threshold = int(fs_size * model_config["write_limit"]["stop"]) + 1
    op_counts = defaultdict(int)

    for op in model:
        if step_count < step_when_write_stop_limit_is_hit - 5000:
            assert isinstance(op, Write)
            assert op.as_dict()["path"] not in existing_paths
            fs_usage = random.randint(1, write_limit_threshold)
            fake_vfs.set_usage(fs_usage)
        if (
            step_when_write_stop_limit_is_hit - 5000
            >= step_count
            > step_when_write_stop_limit_is_hit
        ):
            fs_usage = random.randint(
                write_limit_threshold - 1, write_limit_threshold - 1
            )
            fake_vfs.set_usage(fs_usage)
            assert isinstance(op, Write)
            assert op.as_dict()["path"] not in existing_paths
        if step_count >= step_when_write_stop_limit_is_hit:
            op_counts[op.as_dict()["command"]] += 1
            regular_steps_count += 1
            assert isinstance(op, (Write, Remove, Extend, Shrink))
        step_count += 1
        if step_count == step_when_write_stop_limit_is_hit:
            fake_vfs.set_usage(write_limit_threshold)
            model.set_simulation_parameters(fake_vfs)
            total, used, _ = fake_vfs.get_usage()
        model.set_simulation_parameters(fake_vfs)

    assert step_count == steps
    tolerance = 0.004
    assert op_counts["extend"] / regular_steps_count == pytest.approx(
        0.275, abs=tolerance
    )
    assert op_counts["rm"] / regular_steps_count == pytest.approx(0.225, abs=tolerance)
    assert op_counts["shrink"] / regular_steps_count == pytest.approx(
        0.25, abs=tolerance
    )
    assert op_counts["write"] / regular_steps_count == pytest.approx(
        0.25, abs=tolerance
    )


def test_that_delete_mode_is_left_when_delete_stop_is_reached(model_config: dict):
    steps = 400000
    file_size_1 = 66000000000
    file_size_2 = 1024
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), file_size_1),
        (File(type=FileType.REGULAR, path=Path("/other/file")), file_size_2),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fs_size = 64 * 1024**3
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=fs_size,
        used=file_size_1 + file_size_2,
        contents=contents,
    )
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    regular_steps_count = 0
    step_when_delete_stop_limit_is_hit = 10000
    delete_limit_threshold = int(fs_size * model_config["delete_limit"]["stop"]) + 1
    op_counts = defaultdict(int)

    # ensure model is in delete mode
    fake_vfs.set_usage(0.99 * fs_size)

    for op in model:
        if step_count < step_when_delete_stop_limit_is_hit - 5000:
            assert isinstance(op, Remove)
            assert op.as_dict()["path"] in existing_paths
            fs_usage = random.randint(delete_limit_threshold, fs_size)
            fake_vfs.set_usage(fs_usage)
        if (
            step_when_delete_stop_limit_is_hit - 5000
            >= step_count
            > step_when_delete_stop_limit_is_hit
        ):
            assert isinstance(op, Remove)
            assert op.as_dict()["path"] in existing_paths
            fs_usage = random.randint(delete_limit_threshold, delete_limit_threshold)
            fake_vfs.set_usage(fs_usage)
        if step_count >= step_when_delete_stop_limit_is_hit:
            op_counts[op.as_dict()["command"]] += 1
            regular_steps_count += 1
            assert isinstance(op, (Write, Remove, Extend, Shrink))
        step_count += 1
        if step_count == step_when_delete_stop_limit_is_hit:
            fake_vfs.set_usage(delete_limit_threshold - 1)
            model.set_simulation_parameters(fake_vfs)
            total, used, _ = fake_vfs.get_usage()
        model.set_simulation_parameters(fake_vfs)

    assert step_count == steps
    tolerance = 0.004
    assert op_counts["extend"] / regular_steps_count == pytest.approx(
        0.275, abs=tolerance
    )
    assert op_counts["rm"] / regular_steps_count == pytest.approx(0.225, abs=tolerance)
    assert op_counts["shrink"] / regular_steps_count == pytest.approx(
        0.25, abs=tolerance
    )
    assert op_counts["write"] / regular_steps_count == pytest.approx(
        0.25, abs=tolerance
    )


@pytest.mark.parametrize("steps", (250000, 500000, 1000000))
def test_that_ops_are_generated_according_to_weights_when_in_regular_mode(
    model_config: dict, steps: int
):
    file_size_1 = 16 * 1024**3
    file_size_2 = 16 * 1024**3
    contents = [
        (File(type=FileType.DIRECTORY, path=Path("/some")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other")), 0),
        (File(type=FileType.REGULAR, path=Path("/some/file")), file_size_1),
        (File(type=FileType.REGULAR, path=Path("/other/file")), file_size_2),
    ]
    existing_paths = tuple(p[0].path for p in contents)
    fake_vfs = FakeSimulationVirtualFileSystem(
        size=64 * 1024**3,
        used=file_size_1 + file_size_2,
        contents=contents,
    )
    model_config["steps"] = steps
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    op_counts = defaultdict(float)
    for op in model:
        op_counts[op.as_dict()["command"]] += 1
        step_count += 1

    tolerance = 0.004
    assert step_count == steps
    assert op_counts["extend"] / steps == pytest.approx(0.275, abs=tolerance)
    assert op_counts["rm"] / steps == pytest.approx(0.225, abs=tolerance)
    assert op_counts["shrink"] / steps == pytest.approx(0.25, abs=tolerance)
    assert op_counts["write"] / steps == pytest.approx(0.25, abs=tolerance)


def test_correct_behavior_when_write_is_generated(
    model_config: dict, fake_vfs_contents
):
    fake_vfs = _create_fake_vfs(64 * 1024**3, fake_vfs_contents)
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    op_count = 0
    for op in model:
        if isinstance(op, Write):
            assert op.as_dict()["path"] not in tuple(
                x[0].path for x in fake_vfs_contents
            )
            assert (
                512 * 1 * 8  # model block size * min. random_range * min. size_factor
                <= op.as_dict()["size"]
                <= 512
                * 1024
                * 2048  # model block size * min. random_range * min. size_factor
            )
            op_count += 1
    assert op_count > 100


def test_correct_behavior_when_remove_is_generated(
    model_config: dict, fake_vfs_contents
):
    fake_vfs = _create_fake_vfs(64 * 1024**3, fake_vfs_contents)
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    op_count = 0
    for op in model:
        if isinstance(op, Remove):
            assert op.as_dict()["path"] in tuple(x[0].path for x in fake_vfs_contents)
            op_count += 1
    assert op_count > 100


def test_correct_behavior_when_extend_is_generated(
    model_config: dict, fake_vfs_contents
):
    fake_vfs = _create_fake_vfs(64 * 1024**3, fake_vfs_contents)
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    op_count = 0
    for op in model:
        if isinstance(op, Extend):
            op_count += 1
            assert op.as_dict()["path"] in tuple(
                x[0].path for x in fake_vfs_contents if x[0].type == FileType.REGULAR
            )
            assert (
                512 * 1 * 8  # model block size * min. random_range * min. size_factor
                <= op.as_dict()["extend_size"]
                <= 512
                * 1024
                * 2048  # model block size * min. random_range * min. size_factor
            )
    assert op_count > 100


def test_correct_behavior_when_shrink_is_generated(
    model_config: dict, fake_vfs_contents
):
    fake_vfs = _create_fake_vfs(64 * 1024**3, fake_vfs_contents)
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    op_count = 0
    for op in model:
        if isinstance(op, Shrink):
            op_count += 1
            assert op.as_dict()["path"] in tuple(
                x[0].path for x in fake_vfs_contents if x[0].type == FileType.REGULAR
            )
            assert (
                512  # shrink at least one chunk
                <= op.as_dict()["shrink_size"]
                <= 512
                * 1024
                * 2048  # model block size * min. random_range * min. size_factor
            )
    assert op_count > 100


@pytest.mark.parametrize(
    "steps",
    (5000, 10000, 15000, 20000),
)
def test_that_only_valid_ops_for_a_full_vfs_are_generated(
    steps: int, model_config: dict
):
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
    model_config["steps"] = steps
    model_config["write_limit"]["start"] = 0.0
    model_config["delete_limit"]["start"] = 1.0
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(fake_vfs)
    step_count = 0
    for op in model:
        assert isinstance(op, (Extend, Remove, Shrink, Write))
        if isinstance(op, (Remove, Shrink)):
            assert op.as_dict()["path"] in existing_paths
        elif isinstance(op, Extend):
            assert op.as_dict()["path"] in existing_paths
            new_file_size = (
                fake_vfs.get_size_of(op.as_dict()["path"]) + op.as_dict()["extend_size"]
            )
            assert new_file_size <= fake_vfs.get_size_of(op.as_dict()["path"])
        elif isinstance(op, Write):
            assert op.as_dict()["path"] in existing_paths
            assert op.as_dict()["size"] <= fake_vfs.get_size_of(op.as_dict()["path"])
        step_count += 1
    assert step_count == steps


@pytest.mark.parametrize(
    "steps",
    (5000, 10000, 15000, 20000),
)
def test_that_only_valid_ops_for_an_empty_vfs_are_generated(
    steps: int, model_config: dict
):
    model_config["steps"] = steps
    model_config["write_limit"]["start"] = 0.0
    model_config["delete_limit"]["start"] = 1.0
    model = KADModel(model_config, Path())
    model.set_simulation_parameters(
        FakeSimulationVirtualFileSystem(64 * 1024**3, 0, []),
    )
    step_count = 0
    for op in model:
        step_count += 1
        assert isinstance(op, Write)
    assert step_count == steps


@pytest.fixture
def model_config() -> dict:
    return {
        "steps": 10000,
        "operation_factors": {"write": 10, "delete": 9, "increase": 11, "decrease": 10},
        "size_factors": [{"size": 8, "weight": 1}, {"size": 2048, "weight": 1}],
        "random_range": {"min": 1, "max": 1024},
        "chunk_size": 512,
        "write_limit": {"start": 0.05, "stop": 0.3},
        "delete_limit": {"start": 0.95, "stop": 0.7},
    }


@pytest.fixture
def fake_vfs_contents() -> list[tuple[File, int]]:
    return [
        (File(type=FileType.DIRECTORY, path=Path("/some/folder")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/other/folder")), 0),
        (File(type=FileType.DIRECTORY, path=Path("/folder")), 0),
        (File(type=FileType.REGULAR, path=Path("/file1")), 512),
        (File(type=FileType.REGULAR, path=Path("/other/folder/file2")), 4 * 1024),
        (File(type=FileType.REGULAR, path=Path("/some/folder/file3")), 128 * 1024),
        (File(type=FileType.REGULAR, path=Path("/file4")), 5 * 1024**2),
        (File(type=FileType.REGULAR, path=Path("/file5")), 10 * 1024**3),
    ]


def _create_fake_vfs(
    size: int, fake_vfs_contents: list[tuple[File, int]]
) -> FakeSimulationVirtualFileSystem:
    used = sum(x[1] for x in fake_vfs_contents)
    return FakeSimulationVirtualFileSystem(
        size=size, used=used, contents=fake_vfs_contents
    )
