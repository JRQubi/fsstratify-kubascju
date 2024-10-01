from pathlib import Path

import pytest
from fsstratify.operations import Mkdir, Remove, Write, Copy, Move, Extend

from fsstratify.errors import ConfigurationError, PlaybookError
from fsstratify.usagemodels.playbook import PlaybookModel, INPUT_PLAYBOOK_NAME


def test_that_a_nonexistent_playbook_raises_an_error():
    with pytest.raises(ConfigurationError):
        PlaybookModel({}, Path())


def test_that_an_existing_playbook_does_not_raise_an_error(minimal_playbook):
    try:
        PlaybookModel({}, minimal_playbook)
    except ConfigurationError:
        pytest.fail("PlaybookModel did raise an error when it should not.")


def test_that_an_empty_playbook_raises_an_error(empty_playbooks):
    with pytest.raises(PlaybookError):
        PlaybookModel({}, empty_playbooks[0])
    pass


def test_that_the_correct_number_of_steps_is_returned(input_playbook):
    assert PlaybookModel({}, input_playbook).steps() == 8


def test_that_the_correct_operation_sequence_is_returned(playbooks):
    model = PlaybookModel({}, playbooks[0])
    ops = [op for op in model]
    _assert_correct_op_sequence(ops, playbooks[1])


def test_that_comments_are_ignored(playbooks_with_comments):
    playbooks = playbooks_with_comments
    model = PlaybookModel({}, playbooks[0])
    ops = [op for op in model]
    _assert_correct_op_sequence(ops, playbooks[1])


def _assert_correct_op_sequence(ops: list, expected: list):
    assert len(ops) == len(expected)
    for i in range(len(ops)):
        assert isinstance(ops[i], expected[i])


def test_that_invalid_playbook_lines_raise_an_error(invalid_playbooks):
    with pytest.raises(PlaybookError):
        PlaybookModel({}, invalid_playbooks)


@pytest.fixture
def minimal_playbook(tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write("mkdir folder\n")
    yield tmp_path


@pytest.fixture
def input_playbook(tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write("mkdir folder\n")
        playbook.write("rm folder\n")
        playbook.write("write file size=1234\n")
        playbook.write("cp file other\n")
        playbook.write("rm file\n")
        playbook.write("mv other file\n")
        playbook.write("extend file extend_size=13\n")
        playbook.write("rm file\n")
    yield tmp_path


_PLAYBOOK_1 = {
    "content": """
mkdir a
mkdir b
""",
    "ops": (Mkdir, Mkdir),
}

_PLAYBOOK_2 = {
    "content": """
mkdir folder
rm folder
write file size=123
cp file other
rm file
mv other file
extend file extend_size=13
rm file
""",
    "ops": (Mkdir, Remove, Write, Copy, Remove, Move, Extend, Remove),
}

_PLAYBOOK_3 = {
    "content": """# first comment
# second comment
mkdir folder
#third comment
rm folder
write file size=123
cp file other
## fourth comment
rm file
mv other file
#mkdir comment
extend file extend_size=13
# rm comment
rm file
""",
    "ops": (Mkdir, Remove, Write, Copy, Remove, Move, Extend, Remove),
}

_PLAYBOOK_4 = {
    "content": """# first comment
#second comment
mkdir folder
""",
    "ops": (Mkdir,),
}

_PLAYBOOK_5 = {
    "content": """# first comment
#second comment
mkdir folder
 # third comment
rm folder
""",
    "ops": (Mkdir, Remove),
}

_PLAYBOOK_6 = {"content": "", "ops": ()}
_PLAYBOOK_7 = {"content": "# comment", "ops": ()}
_PLAYBOOK_8 = {"content": "# comment\n#comment\n", "ops": ()}
_PLAYBOOK_9 = {"content": "\n\n\n\n", "ops": ()}
_PLAYBOOK_10 = {"content": "\n\n\n# comment\n", "ops": ()}


@pytest.fixture(params=(_PLAYBOOK_1, _PLAYBOOK_2, _PLAYBOOK_3))
def playbooks(request, tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write(request.param["content"])
    yield tmp_path, request.param["ops"]


@pytest.fixture(params=(_PLAYBOOK_3, _PLAYBOOK_4, _PLAYBOOK_5))
def playbooks_with_comments(request, tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write(request.param["content"])
    yield tmp_path, request.param["ops"]


@pytest.fixture(
    params=(_PLAYBOOK_6, _PLAYBOOK_7, _PLAYBOOK_8, _PLAYBOOK_9, _PLAYBOOK_10)
)
def empty_playbooks(request, tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write(request.param["content"])
    yield tmp_path, request.param["ops"]


@pytest.fixture(params=("foo\n", "mkdir folder\nrmdir folder", "write file\n"))
def invalid_playbooks(request, tmp_path):
    with (tmp_path / INPUT_PLAYBOOK_NAME).open("w") as playbook:
        playbook.write(request.param)
    yield tmp_path
