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
ai.common.config.ConfigError: a node configuration the user must fix.

It is rocketlib's ConfigError, an APERR with Ec.InvalidParam, so the engine's
exception translator (engLib/python/call.hpp) reports it with that code: a
refused pipeline. Still a ValueError for existing handlers.
"""

import pytest

import ai.common.config as config_module
import rocketlib
from ai.common.config import ConfigError, UnresolvedCredentialError, check_credential_resolved
from rocketlib import APERR, Ec


def test_is_rocketlibs_config_error():
    assert ConfigError is rocketlib.ConfigError


def test_an_invalid_param_aperr_with_the_message_alone():
    error = ConfigError('llm_openai: no API key is configured')
    assert isinstance(error, APERR)
    assert error.ec == Ec.InvalidParam
    assert str(error) == 'llm_openai: no API key is configured'


def test_still_a_value_error():
    # handlers written for ValueError keep catching it
    with pytest.raises(ValueError):
        raise ConfigError('x')


def test_unfilled_credentials_are_configuration_errors():
    with pytest.raises(UnresolvedCredentialError) as raised:
        check_credential_resolved('llm_openai', 'apikey', '${ROCKETRIDE_OPENAI_KEY}')
    assert isinstance(raised.value, ConfigError)
    assert raised.value.ec == Ec.InvalidParam
    assert 'ROCKETRIDE_OPENAI_KEY' in str(raised.value)


def test_no_require_config_helper():
    assert not hasattr(config_module, 'require_config')
