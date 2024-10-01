"""This module contains functionality to prepopulate a file system."""
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from fsstratify.errors import ConfigurationError


def preserve_files(directory: Path) -> pd.DataFrame:
    """Preserve the files and directories existing under DIRECTORY."""
    preserved_files = []
    for root, dirs, files in tqdm(
        os.walk(directory), desc="[*] Collecting file information", unit=" paths"
    ):
        for d in dirs:
            preserved_path = (Path(root) / d).relative_to(directory)
            preserved_files.append({"type": "d", "path": preserved_path, "size": 0})
        for f in files:
            path = Path(root) / f
            if not path.exists():
                continue
            preserved_path = path.relative_to(directory)
            preserved_files.append(
                {"type": "f", "path": preserved_path, "size": path.stat().st_size}
            )

    return pd.DataFrame(preserved_files).astype(
        {"type": "category", "path": str, "size": int}
    )


def write_preserved_files(preserved: pd.DataFrame, outfile: Path) -> None:
    """Serialize the PRESERVED files to OUTFILE."""
    preserved.to_parquet(outfile, index=False)


def prepopulate_with(target: Path, dataset: Path) -> None:
    """Prepopulate the TARGET file system with the given DATASET."""
    df = pd.read_parquet(dataset)
    for _, row in df.iterrows():
        if row["type"] == "d":
            (target / row["path"]).mkdir()
        if row["type"] == "f":
            content = "X" * row["size"]
            with (target / row["path"]).open("w") as f:
                f.write(content)


def get_prepopulation_dataset_path(name: str, simulation_dir: Path) -> Path:
    """Returns the path to the dataset with the given name.

    First, the folder "prepopulation_datasets" in the package directory is checked. If the dataset is not found there,
    then the current simulation directory is checked. If the dataset is neither in the package directory nor in the
    simulation directory, an exception is raised.
    """
    subdir = "prepopulation_datasets"
    paths = (Path(__file__).parent / subdir / name, simulation_dir / subdir / name)
    for path in paths:
        if path.is_file():
            return path
    raise ConfigurationError(f'Prepopulation dataset "{name}" not found!')
