"""A node reports an invalid configuration with ConfigError (InvalidParam); any other error is left as it is."""

import pickle

import pytest

rocketlib = pytest.importorskip('rocketlib')

from rocketlib import APERR, ConfigError, Ec, IGlobalBase  # noqa: E402


def test_config_error_is_an_invalid_param_aperr_and_a_value_error():
    error = ConfigError('missing apikey')
    assert isinstance(error, APERR)
    assert isinstance(error, ValueError)
    assert error.ec == Ec.InvalidParam
    assert error.msg == 'missing apikey'
    assert str(error) == 'missing apikey'
    assert error.args == ('missing apikey',)


def test_existing_value_error_handlers_still_catch_it():
    with pytest.raises(ValueError, match='missing apikey'):
        raise ConfigError('missing apikey')


def test_config_error_survives_pickling():
    error = pickle.loads(pickle.dumps(ConfigError('bad setting')))
    assert type(error) is ConfigError
    assert error.ec == Ec.InvalidParam and str(error) == 'bad setting'


class _Rejects(IGlobalBase):
    def beginGlobal(self):
        raise ConfigError('Invalid Anthropic API key format, please check your API key.')


class _Breaks(IGlobalBase):
    def beginGlobal(self):
        int('x')  # a plain ValueError: a bug, not the customer's configuration


class _Validates(IGlobalBase):
    def validateConfig(self):
        raise ValueError('missing apikey')


def test_a_node_raising_config_error_reports_invalid_param():
    with pytest.raises(APERR) as caught:
        _Rejects().beginGlobal()
    assert caught.value.ec == Ec.InvalidParam


def test_a_plain_value_error_is_not_turned_into_invalid_param():
    for call in (_Breaks().beginGlobal, _Validates().validateConfig):
        with pytest.raises(ValueError) as caught:
            call()
        assert type(caught.value) is ValueError
        assert not isinstance(caught.value, APERR)


def test_the_start_methods_are_not_wrapped():
    assert _Breaks.__dict__['beginGlobal'].__name__ == 'beginGlobal'
    assert not hasattr(_Breaks.beginGlobal, '__wrapped__')
