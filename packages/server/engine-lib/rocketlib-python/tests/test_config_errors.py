"""A node that rejects its configuration with a ValueError reports InvalidParam: the customer's, not a platform failure."""

import pytest

rocketlib = pytest.importorskip('rocketlib')

from rocketlib import APERR, Ec, IGlobalBase  # noqa: E402


class _Base(IGlobalBase):
    def beginGlobal(self):
        raise ValueError('Invalid Anthropic API key format, please check your API key.')


class _Node(_Base):
    def beginGlobal(self):
        try:
            super().beginGlobal()
        except ValueError:
            self.recovered = True  # a node handling its base class's ValueError still gets one


class _Validates(IGlobalBase):
    def validateConfig(self):
        raise ValueError('missing apikey')


class _Breaks(IGlobalBase):
    def beginGlobal(self):
        raise KeyError('ours')


def test_a_rejected_configuration_is_still_a_value_error_for_python_callers():
    with pytest.raises(ValueError, match='Invalid Anthropic API key format'):
        _Base().beginGlobal()


def test_a_rejected_configuration_leaves_as_invalid_param():
    with pytest.raises(APERR) as caught:
        _Base().beginGlobal()
    assert caught.value.ec == Ec.InvalidParam
    assert 'Invalid Anthropic API key format' in str(caught.value)
    with pytest.raises(APERR) as caught:
        _Validates().validateConfig()
    assert caught.value.ec == Ec.InvalidParam


def test_a_value_error_caught_inside_the_node_stays_a_value_error():
    node = _Node()
    node.beginGlobal()
    assert node.recovered


def test_any_other_error_is_left_as_it_is():
    with pytest.raises(KeyError):
        _Breaks().beginGlobal()


class _Parses(IGlobalBase):
    def beginGlobal(self):
        import json

        json.loads('not json')  # a JSONDecodeError (a ValueError subclass): not a configuration


class _Inherits(_Base):
    pass  # no beginGlobal of its own: the base class's, already reporting


class _Grandchild(_Node):
    def beginGlobal(self):
        super().beginGlobal()


class _Static(IGlobalBase):
    @staticmethod
    def validateConfig():
        raise ValueError('a static method is left alone')


def test_a_value_error_subclass_is_ours_not_a_configuration():
    import json

    with pytest.raises(json.JSONDecodeError):
        _Parses().beginGlobal()


def test_an_inherited_start_reports_too():
    with pytest.raises(APERR) as caught:
        _Inherits().beginGlobal()
    assert caught.value.ec == Ec.InvalidParam


def test_a_grandchild_through_super_keeps_the_inner_handling():
    node = _Grandchild()
    node.beginGlobal()
    assert node.recovered


def test_static_methods_are_not_wrapped():
    with pytest.raises(ValueError):
        _Static.validateConfig()


def test_a_value_error_mapped_from_a_provider_failure_is_not_a_configuration():
    """A driver turns a 503 or a timeout at start into ValueError: a start error, not the customer's config."""
    from rocketlib.filters import _config_errors

    class Unavailable(Exception):
        status_code = 503

    class APITimeoutError(Exception):
        pass

    for failure in (Unavailable('busy'), APITimeoutError('slow'), ConnectionRefusedError('no route')):

        def begin(self, failure=failure):
            try:
                raise failure
            except Exception:
                raise ValueError('Could not validate the key')

        with pytest.raises(ValueError) as caught:
            _config_errors(begin)(object())
        assert type(caught.value) is ValueError  # left as is: not InvalidParam

    class Unauthorized(Exception):
        status_code = 401

    def bad_key(self):
        try:
            raise Unauthorized('invalid key')
        except Exception:
            raise ValueError('Invalid API key')

    with pytest.raises(ValueError) as caught:
        _config_errors(bad_key)(object())
    assert type(caught.value).__name__ == 'APERR'  # a rejected key is the configuration
