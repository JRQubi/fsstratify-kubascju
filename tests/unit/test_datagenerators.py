import pytest

from fsstratify.datagenerators import RandomDataGenerator, StaticStringGenerator
from fsstratify.errors import SimulationError


class TestRandomDataGenerator:
    @pytest.mark.parametrize("size", (0, 1, 2, 128, 512, 1024, 4096, 6000))
    def test_that_the_correct_number_of_bytes_are_returned(self, size: int):
        gen = RandomDataGenerator()
        assert len(gen.generate(size)) == size

    @pytest.mark.parametrize("size", (-1, -2, -3, -128, -512, -1024, -4096, -6000))
    def test_that_negative_sizes_raise_an_error(self, size: int):
        gen = RandomDataGenerator()
        with pytest.raises(SimulationError):
            gen.generate(size)


class TestStaticDataGenerator:
    @pytest.mark.parametrize("size", (0, 1, 2, 128, 512, 1024, 4096, 6000))
    def test_that_the_correct_number_of_bytes_are_returned(self, size: int):
        gen = StaticStringGenerator("A")
        assert len(gen.generate(size)) == size

    @pytest.mark.parametrize("size", (-1, -2, -3, -128, -512, -1024, -4096, -6000))
    def test_that_negative_sizes_raise_an_error(self, size: int):
        gen = StaticStringGenerator("A")
        with pytest.raises(SimulationError):
            gen.generate(size)

    @pytest.mark.parametrize("size", (0, 1, 2, 128, 512, 1024, 4096, 6000))
    @pytest.mark.parametrize(
        "value",
        (
            "A",
            "b",
            "C",
            "\xff",
            "XyZ",
            "\xab\xac\xad\xae\xaf",
            "fsstratify",
            "fünününü",
        ),
    )
    def test_that_the_correct_data_is_generated(self, size: int, value):
        gen = StaticStringGenerator(value)
        data = gen.generate(size)
        assert len(data) == size
