# MIT License
#
# Copyright (c) 2026 Aparavi Software AG
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
ai.common.config.ConfigError / require_config: a node configuration the user must fix.

The engine's exception translator (engLib/python/call.hpp) keeps the error code
of an exception that carries __formatted/code/message/filename/function/line, so
a ConfigError reaches the run as Ec.InvalidParam: a refused pipeline.
"""

import pytest

from ai.common.config import ConfigError, UnresolvedCredentialError, check_credential_resolved, require_config
from rocketlib import Ec


def test_require_config_passes_and_refuses():
    require_config('key', 'unused')
    with pytest.raises(ConfigError, match='no API key'):
        require_config('', 'llm_openai: no API key is configured')


def test_carries_what_the_engine_reads():
    try:
        require_config(None, 'bad setting')
    except ConfigError as e:
        assert getattr(e, '__formatted') is True
        assert e.code == int(Ec.InvalidParam)
        assert e.message == 'bad setting'
        assert e.filename == __file__ and e.function == 'test_carries_what_the_engine_reads' and e.line > 0
    else:
        pytest.fail('not raised')


def test_still_a_value_error():
    # handlers written for ValueError keep catching it
    with pytest.raises(ValueError):
        require_config(False, 'x')


def test_unfilled_credentials_are_configuration_errors():
    with pytest.raises(UnresolvedCredentialError) as raised:
        check_credential_resolved('llm_openai', 'apikey', '${ROCKETRIDE_OPENAI_KEY}')
    assert isinstance(raised.value, ConfigError)
    assert raised.value.code == int(Ec.InvalidParam)
    # reported where the check raised it, not in ConfigError's constructor
    assert raised.value.function == 'check_credential_resolved'


class _Node:
    """A node that validates its configuration in its own constructor."""

    def __init__(self, key):
        require_config(key, 'no key')  # the line the error must point at


def test_reported_where_the_node_refused_even_in_a_constructor():
    with pytest.raises(ConfigError) as raised:
        _Node('')
    assert raised.value.function == '__init__'
    assert raised.value.filename == __file__
