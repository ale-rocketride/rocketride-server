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

"""DAP (WebSocket) requests: request spans, the request metrics, in-flight and overdue requests, connections.

A part of the telemetry sink: mixed into _otel.Telemetry, whose state it uses.
"""

import fnmatch
import json
import sys
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from opentelemetry import trace
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import Link, SpanContext, SpanKind, Status, StatusCode, TraceFlags


from . import _deps

from ._common import (
    _ERROR_CODE,
    MAX_VALUE_LENGTH,
    _WS_CLIENTS,
    MAX_INFLIGHT_REQUESTS,
    ORG_KEY,
    _ns,
    _preset_ids,
    _new_ids,
    _exemplar,
    _parent_context,
)


class _RequestsTelemetry:
    """DAP (WebSocket) requests (mixed into _otel.Telemetry)."""

    def request_scope(
        self, method: str, traceparent: Any = None, identity: Optional[Mapping[str, Any]] = None
    ) -> Tuple[Any, Any]:
        """
        A DAP request about to be handled: (its _deps.Request, the context to make current).

        No span is started: the request's ids are set now, so its trace is
        current while the handler runs (a log line's trace_id, the HTTP
        instrumentations' parent), and its SERVER span is built at
        request_done, only when kept.
        """
        parent = _parent_context(traceparent)
        if parent is not None:
            trace_id = trace.get_current_span(parent).get_span_context().trace_id
            span_id = self._random.getrandbits(64) or 1
        else:
            trace_id, span_id = _new_ids(self._random)
        request = _deps.Request(trace_id, span_id, parent)
        if not any(fnmatch.fnmatchcase(method, pattern) for pattern in self._slow_exclude):
            with self._lock:
                if len(self._inflight) < MAX_INFLIGHT_REQUESTS:
                    self._inflight[span_id] = [
                        method[:MAX_VALUE_LENGTH],
                        time.monotonic(),
                        dict(identity or {}),
                        request,
                        False,
                    ]
        current = SpanContext(trace_id, span_id, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
        return request, trace.set_span_in_context(trace.NonRecordingSpan(current))

    def request_done(
        self,
        method: str,
        error_type: Optional[str],
        duration_s: float,
        traceparent: Any = None,
        span: Any = None,
        identity: Optional[Mapping[str, Any]] = None,
    ) -> None:
        attributes = {'rpc.system.name': 'rocketride_dap', 'rpc.method': method[:MAX_VALUE_LENGTH]}
        if error_type:
            # A response code comes from any exception's .code: bounded like node codes
            code = error_type[:MAX_VALUE_LENGTH]
            attributes['error.type'] = self._bounded_error_type(
                code if _ERROR_CODE.match(code) else '_OTHER', 'request'
            )
        identity = identity or {}
        client = identity.get('client') if identity.get('client') in _WS_CLIENTS else None
        if client:
            attributes['rocketride.ws.client'] = client  # how the caller signed in: three values
        duration = max(0.0, duration_s)
        request = span if isinstance(span, _deps.Request) else None
        if request is not None:
            with self._lock:
                self._inflight.pop(request.span_id, None)

        # Kept: failed, named by an ERROR log, slow (not a long-lived method),
        # or, for a caller that traces, at the ratio (the caller's sampling
        # decision is respected: only a sampled parent counts)
        parent = request.parent if request is not None else _parent_context(traceparent)
        slow = (
            self._slow_request
            and duration >= self._slow_request
            and not any(fnmatch.fnmatchcase(method, pattern) for pattern in self._slow_exclude)
        )
        keep = 'error' if error_type else ''
        if not keep and request is not None and request.error_logged:
            keep = 'error_log'
        if not keep and slow and (request is not None or parent is not None) and self._slow_allowed('', ''):
            keep = 'slow'
        if not keep and parent is not None and self._keep_success():
            if self._caller_limit.allow('', str(identity.get('org_id') or '')[:MAX_VALUE_LENGTH]):
                keep = 'ratio'
            else:
                self.count_dropped('caller_rate_limited')

        # Measured whatever the sampling; the exemplar names the request's span when it is kept
        exemplar = _exemplar(request.trace_id, request.span_id, bool(keep)) if request is not None else None
        self._request.record(duration, attributes, context=exemplar)
        # Per customer: whether their requests fail, and how (no method: bounded series;
        # which method is on the exemplar's trace)
        org = identity.get('org_id')
        if org:
            counted = {'rocketride.ws.client': client or '_unknown'}
            if error_type:
                counted['error.type'] = attributes['error.type']
            self._requests.add(
                1, self._bounded('rocketride.ws.requests', self._with_org(counted, org)), context=exemplar
            )

        span_attributes = dict(attributes)
        if org:
            span_attributes[ORG_KEY] = self._span_org(org)
        if identity.get('connection_id'):
            span_attributes['rocketride.connection.id'] = str(identity['connection_id'])[:MAX_VALUE_LENGTH]
        links = []
        task_id = identity.get('task_id')
        if task_id:
            span_attributes['rocketride.task.id'] = str(task_id)[:MAX_VALUE_LENGTH]
            run_link = self._run_link(str(task_id), bool(keep))
            if run_link is not None:
                links.append(run_link)

        if request is not None:
            # Its HTTP/AWS spans wait for this decision; its database and Redis calls too
            if keep:
                self._export_request(
                    request, dict(span_attributes, **{'rocketride.sampling.reason': keep}), error_type, links
                )
            self._spans.decide(request.span_id, bool(keep))
            return
        if parent is not None and keep:
            attributes = dict(span_attributes, **{'rocketride.sampling.reason': keep})
            end = time.time()
            # semconv rpc-spans: the span is named after the method
            span = self._tracer.start_span(
                attributes['rpc.method'],
                context=parent,
                kind=SpanKind.SERVER,
                start_time=_ns(end - duration),
                attributes=attributes,
                links=links,
            )
            if error_type:
                span.set_status(Status(StatusCode.ERROR, attributes['error.type']))
            span.end(end_time=_ns(end))

    def _run_link(self, task_id: str, kept: bool) -> Optional[Link]:
        """A link to a live run's trace; a kept request keeps the run's trace too (the link must lead somewhere)."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None or not run.trace_id:
                return None
            if kept and not run.keep_reason:
                run.keep_reason = 'request_kept'
            ids = (run.trace_id, run.span_id)
        return Link(SpanContext(ids[0], ids[1], is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED)))

    def _export_request(
        self, request: Any, attributes: Dict[str, Any], error_type: Optional[str], links: Optional[List[Any]] = None
    ) -> None:
        """The kept request's SERVER span (with its preset ids) and a CLIENT span per database/Redis call."""
        with _preset_ids(request.trace_id, request.span_id):
            span = self._tracer.start_span(
                attributes['rpc.method'],
                context=request.parent,
                kind=SpanKind.SERVER,
                start_time=request.start_ns,
                attributes=dict(request.attributes, **attributes),
                links=links or None,
            )
        if error_type:
            span.set_status(Status(StatusCode.ERROR, attributes['error.type']))
        if request.dropped:
            span.set_attribute('rocketride.calls.dropped', request.dropped)
        span.end()
        parent = trace.set_span_in_context(trace.NonRecordingSpan(span.get_span_context()))
        for call in request.calls:
            self._export_call(call, parent)

    def error_logged(self) -> None:
        """An ERROR log line carries the current trace id: that request's trace must be kept."""
        request = _deps.current_request()
        if request is not None:
            request.error_logged = True

    def observe_connections(self, count: Callable[[], int]) -> None:
        self._connections = count

    def _observe_inflight(self, options: CallbackOptions):
        counts: Dict[str, int] = {}
        with self._lock:
            for method, *_rest in self._inflight.values():
                counts[method] = counts.get(method, 0) + 1
        return [Observation(count, {'rpc.method': method}) for method, count in counts.items()]

    def _observe_oldest(self, options: CallbackOptions):
        """The oldest open request per method; one error line for each request past the overdue age."""
        now = time.monotonic()
        oldest: Dict[str, float] = {}
        overdue = []
        with self._lock:
            for entry in self._inflight.values():
                method, started, identity, request, logged = entry
                age = now - started
                oldest[method] = max(oldest.get(method, 0.0), age)
                if self._overdue and age >= self._overdue and not logged:
                    entry[4] = True
                    if request is not None:
                        # before the lock is released: a request ending now keeps its trace
                        request.error_logged = True
                    overdue.append((method, age, identity, request))
        for method, age, identity, request in overdue:
            self._overdue_line(method, age, identity, request)
        return [Observation(round(age), {'rpc.method': method}) for method, age in oldest.items()]

    def _overdue_line(self, method: str, age: float, identity: Mapping[str, Any], request: Any) -> None:
        """One error line for a request open past RR_OTEL_OVERDUE_REQUEST_SECONDS; its trace is kept when it ends."""
        try:
            request.error_logged = True
            line = {
                'level': 'error',
                'event': 'rocketride.request.overdue',
                'method': method,
                'age_s': round(age),
                'trace_id': f'{request.trace_id:032x}',
            }
            for key in ('task_id', 'connection_id'):
                if identity.get(key):
                    line[key] = str(identity[key])[:MAX_VALUE_LENGTH]
            org = self._span_org(identity.get('org_id'))
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('request_overdue_log')

    def _observe_connections(self, options: CallbackOptions):
        count = self._connections
        if count is None:
            return []
        try:
            return [Observation(int(count()))]
        except Exception:
            return []
