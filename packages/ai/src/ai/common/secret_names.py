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
Names that mark a credential, shared by the access log (query parameters) and the
server's own log lines (logfmt): one list, so the two cannot drift apart.
"""

import re
from typing import FrozenSet, Tuple
from urllib.parse import unquote_plus

# Query parameter names whose values are replaced, in canonical form: lower case,
# letters and digits only, after percent-decoding, so ``api-key``, ``API_KEY`` and
# ``apikey`` are one name. Values are never decoded.
SENSITIVE_QUERY_PARAMS: FrozenSet[str] = frozenset(
    {
        # OAuth / OIDC
        'code',
        'state',
        'idtoken',
        'accesstoken',
        'refreshtoken',
        'clientsecret',
        'codeverifier',
        # Credentials this engine and its clients put in query strings
        'auth',  # AuthMiddleware header fallback; chat/dropper links; MCP upload URLs
        'token',  # /task/fetch signed JWT; task tokens on /task/*
        'tokens',  # OAuth broker result on /auth/vscode/*
        # Other usual credential carriers
        'apikey',
        'apitoken',
        'accesskey',
        'secretkey',
        'privatekey',
        'key',
        'authorization',
        'bearer',
        'jwt',
        'password',
        'passwd',
        'secret',
        'session',
        'sessionid',
        'sig',
        'signature',
    }
)

# Canonical endings that mark a credential, so ``github_token``, ``db-password`` and
# ``X-Amz-Signature`` match without being listed. ``key`` counts only as a whole
# name: as an ending it would also take ``sort_key`` or ``primary_key``.
SENSITIVE_PARAM_SUFFIXES: Tuple[str, ...] = (
    'token',
    'secret',
    'password',
    'passwd',
    'apikey',
    'accesskey',
    'secretkey',
    'privatekey',
    'sessionid',
    'signature',
)


def canonical(name: str) -> str:
    """A name in canonical form: percent-decoded, lower case, letters and digits only."""
    return re.sub(r'[^a-z0-9]', '', unquote_plus(name).lower())


def is_sensitive_param(name: str) -> bool:
    """Tell whether a query parameter name, as sent, names a credential."""
    name = canonical(name)
    return name in SENSITIVE_QUERY_PARAMS or name.endswith(SENSITIVE_PARAM_SUFFIXES)
