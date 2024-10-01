import pytest

from fsstratify.operations import (
    get_operations_map,
    Copy,
    Extend,
    Mkdir,
    Move,
    Remove,
    Shrink,
    Write,
)


class TestOperationsMap:

    def test_that_the_correct_operations_map_is_returned(self):
        ops = get_operations_map()
        assert ops == {
            "cp": Copy,
            "extend": Extend,
            "mkdir": Mkdir,
            "mv": Move,
            "rm": Remove,
            "shrink": Shrink,
            "write": Write,
        }
