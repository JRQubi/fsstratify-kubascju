import pytest
import subprocess

from fsstratify.simulation import is_user_admin

CLI = "fsstratify"


@pytest.mark.skipif(
    not is_user_admin(), reason="Admin privileges are required to run system tests."
)
def test_minimal_simulation(minimal_simulation):
    completed_process = subprocess.run(
        ("python", CLI, "run", minimal_simulation.simulation_dir)
    )
    assert completed_process.returncode == 0
    assert minimal_simulation.log_file.exists()
    assert minimal_simulation.strata_file.exists()
    assert minimal_simulation.playbook_file.exists()
