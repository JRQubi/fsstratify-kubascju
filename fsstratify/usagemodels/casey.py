import random
from pathlib import Path
from typing import Optional

from strictyaml import Int, Seq, Str

from fsstratify.errors import ConfigurationError
from fsstratify.operations import get_operations_map
from fsstratify.usagemodels.base import UsageModelConfiguration, UsageModel


class CaseyModel(UsageModel):
    def __init__(self, config: UsageModelConfiguration):
        super().__init__(config)
        self._current_step = 0  # one step = taking a series of photos and deleting those photos afterwards
        self._steps = self._config.parameters["series_num"]
        self._max_photo_num = self._config.parameters["max_photo_num"]
        # size of pixel: number of bytes needed to store bit depth * number of channels
        self._pixel_size = (self._config.parameters["bit_depth"] // 8) + (
            (self._config.parameters["bit_depth"] % 8) > 0
        )
        self._pixel_size *= self._config.parameters["channel_num"]
        self._photo_sizes = [
            x.lower().split("x") for x in self._config.parameters["resolutions"]
        ]
        self._photo_sizes = [
            int(x[0]) * int(x[1]) * self._pixel_size for x in self._photo_sizes
        ]
        self._operations = get_operations_map()

    def steps(self) -> int:
        """Returns the total number of operations the model is going to perform."""
        return self._steps

    @classmethod
    def check_conf(cls, conf, directory: Optional[Path] = None):
        resolutions = [x.lower() for x in conf["resolutions"]]
        # check if resolution strings are provided in correct format nxm or nXm
        if not all(
            x.count("x") == 1 and sum(c.isdigit() for c in x) == len(x) - 1
            for x in resolutions
        ):
            raise ConfigurationError("Photo resolution must have format NxM or NXM")
        # check if max_photo_num,series_num  > 0
        if conf["max_photo_num"] <= 0:
            raise ConfigurationError("max_photo_num > 0 required")
        if conf["series_num"] <= 0:
            raise ConfigurationError("series_num > 0 required")
        if conf["bit_depth"] <= 0:
            raise ConfigurationError("bit_depth > 0 required")
        if conf["channel_num"] <= 0:
            raise ConfigurationError("channel_num > 0 required")

    def _op_gen(self):
        while self._current_step < self._steps:
            self._current_step += 1
            photo_num = random.randint(1, self._max_photo_num)
            photo_size = random.choice(
                self._photo_sizes
            )  # same resolution for all photos in one series
            # take series of photos
            photos_taken = []
            op_name = "create"
            for i in range(photo_num):
                path = self._sim_vfs.get_nonexistent_path()
                photos_taken.append(path)
                playbook_line = f"{op_name} {path} size={photo_size}"
                yield self._operations[op_name].from_playbook_line(playbook_line)
            # don't delete photos from last series
            if self._current_step == self._steps:
                break
            # delete series of photos taken before in random order
            op_name = "rm"
            random.shuffle(photos_taken)
            for p in photos_taken:
                playbook_line = f"{op_name} {p} size={photo_size}"
                yield self._operations[op_name].from_playbook_line(playbook_line)

    @classmethod
    def _get_schema(cls):
        schema = {
            "resolutions": Seq(Str()),
            "max_photo_num": Int(),
            "series_num": Int(),
            Optional("bit_depth", default=8): Int(),
            Optional("channel_num", default=3): Int(),
        }
        return schema

    def __iter__(self):
        return self._op_gen()
