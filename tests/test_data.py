import pytest

from etlrules.data import context


def test_no_context():
    with pytest.raises(RuntimeError) as exc:
        context.KEY
    assert exc.value.args[0] == "No context set."


def test_invalid_type():
    with pytest.raises(TypeError) as exc:
        context[0]
    assert exc.value.args[0] == "Context attr name must be a string."


def test_context():
    with context.set({"STR": "key", "INT": 1, "FLOAT": 2.5, "BOOL": True}) as ctx:
        assert context.STR == "key"
        assert context.INT == 1
        assert context.FLOAT == 2.5
        assert context.BOOL == True
        assert context["STR"] == "key"
        assert context["INT"] == 1
        assert context["FLOAT"] == 2.5
        assert context["BOOL"] == 1
        assert ctx["STR"] == "key"
        assert ctx["INT"] == 1
        assert ctx["FLOAT"] == 2.5
        assert ctx["BOOL"] == 1
        with pytest.raises(KeyError) as exc:
            context.KEY2
        assert exc.value.args[0] == "No such attribute 'KEY2' found in the current context."
    with pytest.raises(RuntimeError) as exc:
        context.KEY
    assert exc.value.args[0] == "No context set."