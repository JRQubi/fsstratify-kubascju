import pathlib
import sys
from typing import Optional

import pytest
from dataclasses import dataclass

TEST_SIMULATION_ROOT = pathlib.Path("tests/system/data")
LOG_FILE = "simulation.log"
PLAYBOOK_FILE = "simulation.playbook"
STRATA_FILE = "simulation.strata"
IMAGE_FILE = "fs.vhd" if sys.platform == "win32" else "fs.img"


@dataclass
class SimulationFiles:
    simulation_dir: pathlib.Path
    log_file: pathlib.Path
    strata_file: pathlib.Path
    playbook_file: pathlib.Path
    image_file: Optional[pathlib.Path] = None

    def remove_simulation_artefacts(self):
        self.playbook_file.unlink(missing_ok=True)
        self.strata_file.unlink(missing_ok=True)
        self.log_file.unlink(missing_ok=True)
        if self.image_file:
            self.image_file.unlink(missing_ok=True)


@pytest.fixture
def minimal_simulation():
    supported_platforms = {"linux", "win32"}
    platform = sys.platform
    if platform not in supported_platforms:
        raise RuntimeError(f"Unsupported platform {platform}")
    simulation_dir = TEST_SIMULATION_ROOT / "minimal-simulation"
    log_file = simulation_dir / LOG_FILE
    strata_file = simulation_dir / STRATA_FILE
    playbook = simulation_dir / PLAYBOOK_FILE
    image_file = simulation_dir / IMAGE_FILE
    simulation_files = SimulationFiles(simulation_dir, log_file, strata_file, playbook, image_file)
    yield simulation_files
    simulation_files.remove_simulation_artefacts()
