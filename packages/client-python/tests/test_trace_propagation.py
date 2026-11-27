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
W3C trace context propagation from the Python SDK.

Every request built by DAPBase.build_request carries the caller's active
OpenTelemetry span as 'traceparent', so the server records it as a child span.
opentelemetry-api is optional; without it, or with no active span, or with
ROCKETRIDE_TRACE_PROPAGATION=false, nothing is added.
"""

import pytest

from rocketride.core import dap_base

trace_api = pytest.importorskip('opentelemetry.trace')
sdk_trace = pytest.importorskip('opentelemetry.sdk.trace')


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    monkeypatch.delenv('ROCKETRIDE_TRACE_PROPAGATION', raising=False)
    monkeypatch.setattr(dap_base, '_otel_trace', None)


def test_no_active_span_adds_nothing():
    assert dap_base.current_traceparent() is None


def test_active_span_is_propagated():
    tracer = sdk_trace.TracerProvider().get_tracer('test')
    with tracer.start_as_current_span('caller') as span:
        context = span.get_span_context()
        # Flags pass through as set (sampled, and the W3C level-2 random-id bit when present)
        flags = int(context.trace_flags)
        assert flags & 0x01
        assert dap_base.current_traceparent() == f'00-{context.trace_id:032x}-{context.span_id:016x}-{flags:02x}'


def test_propagation_can_be_turned_off(monkeypatch):
    monkeypatch.setenv('ROCKETRIDE_TRACE_PROPAGATION', 'false')
    tracer = sdk_trace.TracerProvider().get_tracer('test')
    with tracer.start_as_current_span('caller'):
        assert dap_base.current_traceparent() is None


def test_missing_opentelemetry_is_remembered(monkeypatch):
    monkeypatch.setattr(dap_base, '_otel_trace', False)
    assert dap_base.current_traceparent() is None
