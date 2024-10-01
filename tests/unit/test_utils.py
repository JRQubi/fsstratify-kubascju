import pytest

from fsstratify.utils import (
    parse_size_definition,
    merge_blocks_to_fragments,
    merge_overlapping_fragments,
    parse_boolean_string,
    get_random_string,
)


class TestGetRandomString:
    @pytest.mark.parametrize("length", (1, 2, 3, 10, 20, 100, 123))
    def test_that_the_generated_string_has_the_correct_length(self, length):
        assert len(get_random_string(length)) == length


class TestParseSizeDefinition:
    def test_that_an_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_size_definition("")

    @pytest.mark.parametrize("invalid", ("1 X", "2X", "3YiB", "4 YiB", "5Z", "5ZB"))
    def test_that_an_invalid_suffix_raises(self, invalid):
        with pytest.raises(ValueError):
            parse_size_definition(invalid)

    @pytest.mark.parametrize("integer", [1, 2, 3, 100, 1024, 547567832423])
    def test_that_simple_integers_work(self, integer):
        assert parse_size_definition(str(integer)) == integer

    @pytest.mark.parametrize(
        "size_str,expected",
        [
            ("123", 123),
            ("5k", 5000),
            ("74 k", 74000),
            ("13 KiB", 13312),
            ("14Ki", 14336),
        ],
    )
    def test_that_size_strings_are_parsed_correctly(self, size_str, expected):
        assert parse_size_definition(size_str) == expected


class TestParseBooleanString:
    @pytest.mark.parametrize("invalid_input", ("", "xyz", "yes no", "yes abc", 1))
    def test_that_invalid_inputs_raise(self, invalid_input):
        with pytest.raises(ValueError):
            parse_boolean_string(invalid_input)

    @pytest.mark.parametrize(
        "bool_value", ("True", "true", "TRUE", "TrUe", "yes", "YES", "yEs", "y", "Y")
    )
    def test_that_true_values_are_parsed_correctly(self, bool_value):
        assert parse_boolean_string(bool_value) is True

    @pytest.mark.parametrize(
        "bool_value", ("False", "false", "FALSE", "FaLsE", "no", "NO", "No", "n", "N")
    )
    def test_that_false_values_are_parsed_correctly(self, bool_value):
        assert parse_boolean_string(bool_value) is False


class TestMergeBlocksToFragments:
    def test_that_an_empty_list_returns_an_empty_list(self):
        assert merge_blocks_to_fragments([]) == []

    @pytest.mark.parametrize(
        "block_list,expected",
        [
            ((1, 2, 3), [(1, 3)]),
            ((1, 2, 3, 5, 6), [(1, 3), (5, 6)]),
            ((1, 3, 5), [(1, 1), (3, 3), (5, 5)]),
            (
                (1, 3, 4, 7, 9, 10, 11, 12, 17),
                [(1, 1), (3, 4), (7, 7), (9, 12), (17, 17)],
            ),
        ],
    )
    def test_that_blocks_are_merged_correctly(self, block_list, expected):
        assert merge_blocks_to_fragments(block_list) == expected


class TestMergeOverlappingFragments:
    def test_that_an_empty_list_returns_an_empty_list(self):
        assert merge_overlapping_fragments([]) == []

    @pytest.mark.parametrize(
        "fragments", ([(1, 2)], [(1, 2), (4, 6)], [(1, 2), (4, 6), (8, 13)])
    )
    def test_that_non_overlapping_lists_are_left_untouched(self, fragments):
        assert fragments == merge_overlapping_fragments(fragments)

    @pytest.mark.parametrize(
        "fragments,expected",
        (
            ([(1, 2), (2, 3)], [(1, 3)]),
            ([(1, 2), (2, 3), (3, 4)], [(1, 4)]),
            ([(1, 2), (2, 3), (2, 4)], [(1, 4)]),
            ([(1, 2), (2, 3), (5, 5)], [(1, 3), (5, 5)]),
            ([(1, 2), (2, 3), (5, 6), (1, 8)], [(1, 8)]),
        ),
    )
    def test_that_overlapping_fragments_are_merged(self, fragments, expected):
        assert merge_overlapping_fragments(fragments) == expected

    @pytest.mark.parametrize(
        "fragments,expected",
        (
            ([(1, 2), (3, 4)], [(1, 4)]),
            ([(1, 2), (3, 4), (4, 5), (6, 8)], [(1, 8)]),
            ([(1, 2), (3, 4), (5, 8), (9, 10)], [(1, 10)]),
            ([(3, 5), (1, 2)], [(1, 5)]),
            ([(3, 5), (1, 2), (6, 8)], [(1, 8)]),
        ),
    )
    def test_that_consecutive_fragments_are_merged(self, fragments, expected):
        assert merge_overlapping_fragments(fragments) == expected
