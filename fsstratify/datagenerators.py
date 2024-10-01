from itertools import chain, repeat, cycle
from random import randbytes
from typing import Optional

from fsstratify.errors import SimulationError


class DataGenerator:
    def __init__(self, size_hint: Optional[int] = None):
        self._size_hint = size_hint

    def generate(self, size: int) -> bytes:
        if size < 0:
            raise SimulationError(
                "Error: Number of bytes to generate must not be negative."
            )
        return self._generate(size)

    def _generate(self, size: int) -> bytes:
        raise NotImplementedError


class RandomDataGenerator(DataGenerator):
    def _generate(self, size: int) -> bytes:
        return randbytes(size)


class StaticStringGenerator(DataGenerator):
    def __init__(self, s: str, size_hint: Optional[int] = None):
        super().__init__(size_hint)
        self._value = cycle(bytes(s, encoding="utf-8"))

    def _generate(self, size: int) -> bytes:
        return bytes(next(self._value) for _ in range(size))
