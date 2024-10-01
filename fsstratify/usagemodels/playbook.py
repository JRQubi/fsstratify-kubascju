from pathlib import Path

from fsstratify.errors import PlaybookError, ConfigurationError
from fsstratify.operations import get_operations_map, Operation
from fsstratify.usagemodels.base import UsageModel

INPUT_PLAYBOOK_NAME = "playbook"


class PlaybookModel(UsageModel, model_name="Playbook"):
    """Playbook usage model.

    The playbook usage model reads operations from a playbook file and returns them
    sequentially. Apart from skipping comment lines, the operations are executes as
    specified in the playbook file; no additional behaviour is added by the model
    implementation.
    """

    def __init__(self, config: dict, simulation_dir: Path):
        self._playbook_path: Path = simulation_dir / INPUT_PLAYBOOK_NAME
        super().__init__(config, simulation_dir)
        self._operations_map = get_operations_map()
        self._operations = []
        self._parse_playbook()

    def __iter__(self):
        return self._operations.__iter__()

    def steps(self) -> int:
        """Returns the total number of operations the model is going to perform."""
        return len(self._operations)

    def _validate_config(self) -> None:
        if not self._playbook_path.is_file():
            raise ConfigurationError(
                f'Error: Required input playbook "{INPUT_PLAYBOOK_NAME}" is missing.'
            )

    @classmethod
    def _get_schema(cls):
        schema = {}
        return schema

    def _parse_playbook(self) -> None:
        with self._playbook_path.open("r") as playbook:
            for line in playbook:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    operation = self._parse_playbook_line(line)
                    self._operations.append(operation)
                except Exception as err:
                    raise PlaybookError(err) from err
        if len(self._operations) == 0:
            raise PlaybookError(
                f'Error: No operations defined in playbook "{self._playbook_path}"'
            )

    def _parse_playbook_line(self, line) -> Operation:
        operation = line.split()[0]
        try:
            return self._operations_map[operation].from_playbook_line(line)
        except KeyError as err:
            raise PlaybookError(
                f'Error: Invalid playbook operation "{operation}"'
            ) from err
