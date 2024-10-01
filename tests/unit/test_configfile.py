from pathlib import Path

import pytest
import yaml

from fsstratify.errors import ConfigurationError
from fsstratify.configuration import Configuration


class TestMinimalConfigurationFile:
    def test_that_a_minimal_configuration_file_works(self, minimal_configuration):
        config = Configuration()
        config.load_str(minimal_configuration, Path())

    def test_that_all_values_are_handled_correctly(self, minimal_configuration):
        config = Configuration()
        config.load_str(minimal_configuration, Path())
        assert config["seed"] == 12345
        assert config["file_system"] == {"type": "ntfs", "formatting_parameters": ""}
        assert config["volume"] == {
            "type": "file",
            "directory": Path(),
            "size": 10000000,
            "keep": False,
            "force_overwrite": False,
            "win_drive_letter": "S",
        }
        assert config["usage_model"]["type"] == "ProbabilisticModel"
        assert config["usage_model"]["parameters"] == {
            "steps": "100",
            "file_size_min": "100Ki",
            "file_size_max": "1M",
        }

    def test_that_the_config_is_loaded_correctly_from_disk(
        self, minimal_configuration: str, tmp_path: Path
    ):
        expected_config = Configuration()
        expected_config.load_str(minimal_configuration, tmp_path)

        config = Configuration()
        config_file = tmp_path / "config.yml"

        with config_file.open("w") as cfg:
            cfg.write(minimal_configuration)
        config.load_file(config_file)
        assert str(config) == str(expected_config)


class TestThePrepopulateWithParameter:
    def test_that_it_is_not_required(self, minimal_configuration):
        Configuration().load_str(minimal_configuration, Path())

    def test_that_the_dataset_key_is_handled_correctly(self, minimal_configuration):
        config = yaml.load(minimal_configuration, yaml.Loader)
        config["file_system"]["prepopulate_with"] = {"dataset": "Win10.parquet"}
        conf_str = yaml.dump(config)
        config = Configuration()
        config.load_str(conf_str, Path())
        assert "prepopulate_with" in config["file_system"]
        assert config["file_system"]["prepopulate_with"] == {
            "dataset": "Win10.parquet",
            "mutable": False,
        }

    def test_that_the_default_value_for_mutable_is_set_correctly(
        self, minimal_configuration
    ):
        config = yaml.load(minimal_configuration, yaml.Loader)
        config["file_system"]["prepopulate_with"] = {"dataset": "Win10.parquet"}
        conf_str = yaml.dump(config)
        config = Configuration()
        config.load_str(conf_str, Path())
        assert "prepopulate_with" in config["file_system"]
        assert config["file_system"]["prepopulate_with"] == {
            "dataset": "Win10.parquet",
            "mutable": False,
        }

    @pytest.mark.parametrize("value", (True, False))
    def test_that_the_mutable_key_is_handled_correctly(
        self, value, minimal_configuration
    ):
        config = yaml.load(minimal_configuration, yaml.Loader)
        config["file_system"]["prepopulate_with"] = {
            "dataset": "Win10.parquet",
            "mutable": value,
        }
        conf_str = yaml.dump(config)
        config = Configuration()
        config.load_str(conf_str, Path())
        assert "prepopulate_with" in config["file_system"]
        assert config["file_system"]["prepopulate_with"] == {
            "dataset": "Win10.parquet",
            "mutable": value,
        }


class TestInvalidConfigurations:

    @pytest.mark.parametrize("size", ("1x", "x", "-5", "-5x"))
    def test_that_a_config_with_an_invalid_volume_size_raises(self, size: str):
        config_str = yaml.dump(
            {
                "seed": 12345,
                "file_system": {"type": "ntfs", "formatting_parameters": ""},
                "volume": {"type": "file", "keep": "no", "size": size},
                "usage_model": {
                    "type": "ProbabilisticModel",
                    "parameters": {
                        "steps": 100,
                        "file_size_min": "100Ki",
                        "file_size_max": "1M",
                    },
                },
            }
        )
        config = Configuration()
        with pytest.raises(ConfigurationError):
            config.load_str(config_str, Path())

    def test_that_a_physical_volume_without_a_path_raises_on_linux(self):
        config_str = yaml.dump(
            {
                "seed": 12345,
                "file_system": {"type": "ntfs", "formatting_parameters": ""},
                "volume": {"type": "physical", "keep": "no", "size": "10G"},
                "usage_model": {
                    "type": "ProbabilisticModel",
                    "parameters": {
                        "steps": 100,
                        "file_size_min": "100Ki",
                        "file_size_max": "1M",
                    },
                },
            }
        )
        config = Configuration()
        with pytest.raises(ConfigurationError):
            config.load_str(config_str, Path())


@pytest.fixture
def minimal_configuration():
    config = {
        "seed": 12345,
        "file_system": {"type": "ntfs", "formatting_parameters": ""},
        "volume": {"type": "file", "size": "10M", "keep": "no"},
        "usage_model": {
            "type": "ProbabilisticModel",
            "parameters": {
                "steps": 100,
                "file_size_min": "100Ki",
                "file_size_max": "1M",
            },
        },
    }
    return yaml.dump(config)
