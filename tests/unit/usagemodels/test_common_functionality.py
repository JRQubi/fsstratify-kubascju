import pytest

from fsstratify.usagemodels.base import use_existing_path


class TestUseExistingPath:
    @pytest.mark.parametrize(
        "value",
        (
            -10,
            -10.0,
            -1,
            -1.0,
            -0.0000000000000000000000000001,
            1.1,
            1.000000000000001,
            2,
            1.2,
            10,
        ),
    )
    def test_that_invalid_weights_raise_an_error(self, value):
        with pytest.raises(ValueError):
            use_existing_path(value)

    @pytest.mark.parametrize("value", ("10", "1", "0.5", True, False, "a", "abc"))
    def test_that_weights_which_are_not_numbers_raise_an_error(self, value):
        with pytest.raises(ValueError):
            use_existing_path(value)

    def test_that_a_weight_of_one_works_as_expected(self):
        samples = (use_existing_path(1.0) for _ in range(100000))
        assert all(samples)

    def test_that_a_weight_of_zero_works_as_expected(self):
        samples = (use_existing_path(0.0) for _ in range(100000))
        assert not all(samples)

    @pytest.mark.parametrize("weight", (0.1, 0.3, 0.5, 0.7, 0.8))
    def test_that_the_weight_works_as_expected(self, weight):
        n = 100000
        samples = tuple(use_existing_path(weight) for _ in range(n))
        true_count = samples.count(True)
        assert pytest.approx(weight, 0.02) == true_count / n
