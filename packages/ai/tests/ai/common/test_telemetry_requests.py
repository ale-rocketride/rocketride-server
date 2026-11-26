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
Request telemetry at the WebSocket dispatch (DAPConn.on_receive) and the
export's transport check.

Every request is timed with a bounded method and error.type; a caller's trace
context is honoured only from a signed-in connection.
"""

import asyncio

import pytest

from ai.common import telemetry
from ai.common.dap import dap_conn
from ai.common.dap.dap_conn import DAPConn, request_error_type


@pytest.mark.parametrize(
    'response, expected',
    [
        ({'success': True}, None),
        ({}, None),
        (None, None),
        ({'success': False, 'code': 'OUT_OF_CREDITS'}, 'OUT_OF_CREDITS'),
        ({'success': False}, '_OTHER'),  # an exception without a code
        ({'success': False, 'code': ''}, '_OTHER'),
        ({'success': False, 'code': 42}, '_OTHER'),
    ],
)
def test_request_error_type(response, expected):
    assert request_error_type(response) == expected


class Conn(DAPConn):
    """A DAPConn without a transport: handlers and send are stubs."""

    def __init__(self, authenticated=None):
        self.sent = []
        if authenticated is not None:
            self._authenticated = authenticated

    async def on_execute(self, request):
        return {'success': False, 'code': 'OUT_OF_CREDITS'}

    async def _call_method(self, message, name, fallback):
        handler = getattr(self, name, None)
        return (True, await handler(message)) if handler else (False, None)

    def build_response(self, message):
        return {'success': True}

    def debug_message(self, *_):
        pass

    async def send(self, message):
        self.sent.append(message)


TRACEPARENT = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'


@pytest.mark.parametrize(
    'authenticated, forwarded',
    [
        (True, TRACEPARENT),  # a signed-in client's trace context is honoured
        (False, None),  # before sign-in (auth, rrext_public_*) it is not
        (None, TRACEPARENT),  # a connection without sign-in (not TaskConn) is trusted
    ],
)
def test_requests_are_timed_and_traceparent_needs_a_signed_in_caller(monkeypatch, authenticated, forwarded):
    calls = []
    monkeypatch.setattr(dap_conn.telemetry, 'request_done', lambda *args: calls.append(args))
    conn = Conn(authenticated)
    asyncio.run(conn.on_receive({'type': 'request', 'command': 'execute', 'traceparent': TRACEPARENT}))
    asyncio.run(conn.on_receive({'type': 'request', 'command': 'no_such_command'}))
    (method, error_type, duration, traceparent, span, identity), (other, other_error, _, _, _, _) = calls
    assert identity is None  # telemetry off: who asked is not worked out
    assert (method, error_type, traceparent) == ('execute', 'OUT_OF_CREDITS', forwarded)
    assert span is None  # telemetry off: no live span
    assert duration >= 0
    assert (other, other_error) == ('_OTHER', None)  # an unknown command is bounded


def test_credential_headers_over_cleartext_are_refused():
    pytest.importorskip('opentelemetry.sdk')
    from rocketride.otelbridge.config import InsecureTransportError

    from ai.common.telemetry._otel import _check_transport

    environ = {
        'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://collector.example:4318',
        'OTEL_EXPORTER_OTLP_HEADERS': 'Authorization=Bearer secret',
    }
    with pytest.raises(InsecureTransportError, match='refusing to export OTLP credentials in cleartext'):
        _check_transport(environ)
    # loopback, TLS, or the explicit opt-out are fine; the display never shows a secret
    for override in (
        {'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://localhost:4318'},
        {'OTEL_EXPORTER_OTLP_ENDPOINT': 'https://collector.example:4318'},
        {'ROCKETRIDE_OTEL_ALLOW_INSECURE': '1'},
    ):
        assert 'secret' not in _check_transport({**environ, **override})
    # a tenant header is not a credential
    assert _check_transport(
        {'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://alloy:4318', 'OTEL_EXPORTER_OTLP_HEADERS': 'X-Scope-OrgID=stg'}
    )


def test_hooks_are_noops_when_off():
    telemetry._reset_for_tests()
    telemetry.request_done('execute', None, 0.1, TRACEPARENT)  # nothing to assert but no error


def test_a_cancelled_request_is_still_ended_for_telemetry(monkeypatch):
    calls = []
    monkeypatch.setattr(dap_conn.telemetry, 'request_done', lambda *args: calls.append(args))
    conn = Conn(True)

    async def cancelled(message, *names):
        raise asyncio.CancelledError()

    monkeypatch.setattr(conn, '_call_method', cancelled)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(conn.on_receive({'type': 'request', 'command': 'execute'}))
    ((method, error_type, *_),) = calls
    assert (method, error_type) == ('execute', 'cancelled')
