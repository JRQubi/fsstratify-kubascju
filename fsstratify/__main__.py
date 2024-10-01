import sys
from pathlib import Path
from typing import Optional

import click
from click_option_group import optgroup, MutuallyExclusiveOptionGroup

import fsstratify.simulation
from fsstratify import prepopulation, __versions__
from fsstratify.platforms import Platform, get_current_platform
from fsstratify.simulation import Simulation, Configuration
from fsstratify.usagemodels import get_model_registry

CONTEXT_SETTINGS = {"help_option_names": ("-h", "--help")}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__versions__.__version__)
def cli():
    pass


replications = [
    "KAD19_1",
    "KAD19_2",
    "CaseyNikonD70",
    "CaseyPowershotELPH100HS",
    "CaseyPowershotSD1200IS",
    "CaseyNikonCoolpixS600",
]


@cli.command(name="init")
@click.argument("directory", type=click.Path())
@optgroup.group("Usage model initialization options", cls=MutuallyExclusiveOptionGroup)
@optgroup.option(
    "--model",
    type=click.Choice(tuple(get_model_registry().keys())),
    help="Create a template for the given usage model.",
)
@optgroup.option(
    "--replicate",
    type=click.Choice(replications),
    help="Create a usage model configuration to replicate the given experiment.",
)
def initialize_simulation_directory(directory, model, replicate):
    """Generate and initialize a new simulation directory.

    \b
    DIRECTORY is the target directory. This must not exist yet."""
    status = fsstratify.simulation.initialize_directory(
        directory=directory, replicate=replicate, model=model
    )
    if status == 0:
        print(f'[+] Initialized simulation directory "{directory}".')


@cli.command(name="run")
@click.argument(
    "directory",
    type=click.Path(exists=True, resolve_path=True, file_okay=False, path_type=Path),
)
def run_simulation(directory):
    """Run the simulation given by DIRECTORY.

    \b
    DIRECTORY is an initialized simulation directory."""
    try:
        config_file = (directory / "simulation.yml").resolve()
        config = Configuration()
        config.load_file(config_file)

        usage_model_name = config["usage_model"]["type"]
        usage_model_params = config["usage_model"]["parameters"]
        usage_model = get_model_registry()[usage_model_name](
            usage_model_params, directory
        )

        simulation = Simulation(directory, config, usage_model)
        simulation.run()
    except Exception as err:
        print(f"Error: {err}", file=sys.stderr)
        raise


# clean: remove everything except for the config
# ???  : remove everything but the config and the results
@cli.command(name="clean")
@click.argument(
    "directory",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
)
@click.option(
    "--all",
    "delete_results",
    is_flag=True,
    help="also clean file system strata, playbook and logs",
    default=False,
)
def clean_sim_dir(directory: Path, delete_results: bool = False):
    """Clean simulation directory given by DIRECTORY.

    \b
    DIRECTORY is an initialized simulation directory."""
    keep = ["simulation.yml", "prepopulation_datasets"]
    if not delete_results:
        keep.extend(["simulation.strata", "simulation.playbook", "simulation.log"])
    sim_dir = Path(directory).resolve()
    try:
        if get_current_platform() == Platform.WINDOWS:
            fsstratify.simulation.clean_sim_dir_win(sim_dir, keep=keep)
        elif get_current_platform() == Platform.LINUX:
            fsstratify.simulation.clean_sim_dir_linux(sim_dir, keep=keep)
        else:
            print("command not supported on current platform")
    except Exception as err:
        print(f"Error: {err}", file=sys.stderr)
        return -1


@cli.group(name="preserve")
def preserve():
    """Preserve a model of an existing file system state."""
    pass


@preserve.command(name="files")
@click.argument(
    "directory",
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
)
@click.option(
    "-o",
    "--outfile",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
)
def preserve_files(directory: Path, outfile: Optional[Path] = None):
    """Preserve the files and directory existing under DIRECTORY."""
    if outfile is None:
        outfile = ".".join((directory.name, "parquet"))
    preserved = prepopulation.preserve_files(directory)
    click.echo(f"[*] Writing model file to {outfile}")
    prepopulation.write_preserved_files(preserved, outfile)
    click.echo("[*] Done!")


if __name__ == "__main__":
    sys.exit(cli())
