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

"""Dependency calls, EaaS's own and the ones nodes relay: client spans, model calls, node error reports, the dependency metrics.

A part of the telemetry sink: mixed into _otel.Telemetry, whose state it uses.
"""

import contextvars
import os
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

from opentelemetry import context as otel_context
from opentelemetry.trace import SpanKind, Status, StatusCode


from . import _deps

from ._common import (
    MAX_COMPONENT_TYPES,
    MAX_MODELS_PER_PROVIDER,
    MAX_CAUSE_LENGTH,
    MAX_BUFFERED_CALLS,
    MAX_VALUE_LENGTH,
    _RELAY_ATTRIBUTES,
    _GENAI_ATTRIBUTES,
    MAX_RELAYED_DROPPED,
    ORIGIN_KEY,
    PROJECT_KEY,
    MAX_PIPELINE_SERVERS,
    MAX_MARKS_PER_PASS,
    MAX_SERVERS_PER_ORG,
    MAX_RELAYED_PER_PASS,
    MAX_PENDING_MEASUREMENTS,
    MEASURE_INTERVAL,
    ORG_KEY,
    _relay_record_valid,
    _new_ids,
    _TRACE_ID,
    _SPAN_ID,
)


class _DependenciesTelemetry:
    """Dependency calls, EaaS's own and the ones nodes relay (mixed into _otel.Telemetry)."""

    def _export_call(self, call: Any, parent: Any) -> None:
        span = self._tracer.start_span(
            call.name, context=parent, kind=SpanKind.CLIENT, start_time=call.start_ns, attributes=call.attributes
        )
        if call.error_type:
            span.set_attribute('error.type', call.error_type)
            span.set_status(Status(StatusCode.ERROR, call.error_type))
        span.end(end_time=max(call.end_ns, call.start_ns))

    def dependency_call(self, call: Any) -> None:
        """A database or Redis call ended (_deps recorders): measured, then held by its request or decided."""
        seconds = (call.end_ns - call.start_ns) / 1e9
        self._queue_measurement(
            call.metric, seconds, dict(call.attributes, **{'error.type': call.error_type, ORIGIN_KEY: 'eaas'})
        )
        request = _deps.current_request()
        if request is not None:
            if len(request.calls) < _deps.MAX_CALLS_PER_REQUEST:
                request.calls.append(call)
            else:
                request.dropped += 1
                self.count_dropped('dependency_span_cap')
            return
        # Outside any request: kept when it failed or was slow
        if call.error_type or (self._slow_request and seconds >= self._slow_request and self._slow_allowed('', '')):
            self._export_call(call, None)

    def _model_call(self, task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
        """
        A node's logical model call (relay.model_call): held by its pass (a span under
        its component, measured when the pass ends with an exemplar of that span),
        else measured now.
        """
        raw = body.get('a') if isinstance(body.get('a'), dict) else {}
        attributes = {
            key: str(value)[:MAX_VALUE_LENGTH]
            for key, value in raw.items()
            if key in _GENAI_ATTRIBUTES and value not in (None, '')
        }
        start, end = int(body.get('s') or 0), int(body.get('e') or 0)
        if not (end >= start > 0):
            return
        error_type = str(body['x'])[:MAX_VALUE_LENGTH] if body.get('x') else None
        message = str(body.get('g') or '')[:MAX_CAUSE_LENGTH]
        retries = [r for r in body.get('r') or [] if isinstance(r, list) and len(r) == 3][:16]
        raw_usage = body.get('u') if isinstance(body.get('u'), dict) else {}
        usage = {k: v for k, v in raw_usage.items() if k in ('input', 'output', 'cache_read') and isinstance(v, int)}
        answered_by = str(body.get('rm') or '')[:MAX_VALUE_LENGTH]
        pipe, component = body.get('p'), body.get('c')
        held = False
        with self._lock:
            run = self._runs.get(task_id)
            invocation = run.invocations.get(pipe) if run is not None and pipe is not None else None
            if invocation is not None and component and len(invocation.models) < MAX_RELAYED_PER_PASS:
                component_type = self._component_type(run, str(component), pipeline)
                span_id = _new_ids(self._random)[1]
                invocation.models.append(
                    (component_type, start, end, attributes, error_type, message, retries, usage, answered_by, span_id)
                )
                invocation.call_error = invocation.call_error or bool(error_type)
                held = True
        if not held:
            self._measure_model_call(attributes, (end - start) / 1e9, error_type, usage, None)

    def _measure_model_call(
        self,
        attributes: Mapping[str, str],
        seconds: float,
        error_type: Optional[str],
        usage: Mapping[str, int],
        exemplar: Optional[Any],
    ) -> None:
        """gen_ai.client.operation.duration and token usage, model bounded per provider."""
        provider = attributes.get('gen_ai.provider.name') or '_unknown'
        labels = {
            'gen_ai.operation.name': attributes.get('gen_ai.operation.name') or 'chat',
            'gen_ai.provider.name': provider,
            'gen_ai.request.model': self._bounded_model(provider, attributes.get('gen_ai.request.model') or ''),
        }
        timed = dict(labels)
        if error_type:
            timed['error.type'] = self._bounded_error_type(error_type, 'model')
        self._genai_duration.record(
            max(0.0, seconds), self._bounded('gen_ai.client.operation.duration', timed), context=exemplar
        )
        for token_type, key in (('input', 'input'), ('output', 'output')):
            if usage.get(key):
                self._genai_tokens.record(
                    usage[key],
                    self._bounded('gen_ai.client.token.usage', dict(labels, **{'gen_ai.token.type': token_type})),
                    context=exemplar,
                )

    def _bounded_model(self, provider: str, model: str) -> str:
        """A model name as metrics keep it: MAX_MODELS_PER_PROVIDER per provider, then '_other'."""
        if not model:
            return '_unknown'
        with self._lock:
            known = self._models.get(provider)
            if known is None:
                if len(self._models) >= MAX_COMPONENT_TYPES:
                    return '_other'
                known = self._models[provider] = set()
            if model in known:
                return model
            if len(known) < MAX_MODELS_PER_PROVIDER:
                known.add(model)
                return model
        self.count_dropped('model_overflow')
        return '_other'

    def dependency_relayed(self, task_id: str, body: Any, pipeline: Optional[Dict[str, Any]]) -> None:
        """Dependency calls a node made, relayed by the subprocess (relay.py, a batch): measured, then held by their pass."""
        records = body.get('calls') if isinstance(body, dict) else body
        for record in records if isinstance(records, list) else []:
            if isinstance(record, dict):
                # One record that cannot be read costs that record only, not the batch
                try:
                    self._relayed(task_id, record, pipeline)
                except Exception:
                    self.count_hook_error('relay_record')

    def _relayed(self, task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
        if 'dropped' in body:
            dropped = body['dropped']
            if isinstance(dropped, int) and not isinstance(dropped, bool) and dropped > 0:
                reason = body.get('reason') if body.get('reason') in ('relay_overflow',) else 'dependency_span_cap'
                self.count_dropped(reason, min(dropped, MAX_RELAYED_DROPPED))
            return
        kind = body.get('k')
        if kind is not None:
            if not _relay_record_valid(body):
                return
            if kind == 'err':
                self._node_error_mark(task_id, body, pipeline)
            elif kind == 'gen':
                self._model_call(task_id, body, pipeline)
            else:
                self._node_raised(task_id, body, pipeline)
            return
        metric, start, end = body.get('m'), int(body.get('s') or 0), int(body.get('e') or 0)
        # Only the attributes the relay writes, short: a node could write a line itself
        raw = body.get('a') if isinstance(body.get('a'), dict) else {}
        attributes = {
            key: (value if isinstance(value, (int, float, bool)) else str(value)[:MAX_VALUE_LENGTH])
            for key, value in raw.items()
            if key in _RELAY_ATTRIBUTES and value not in (None, '')
        }
        error_type = str(body['x'])[:MAX_VALUE_LENGTH] if body.get('x') else None
        if metric not in self._dependency or not end >= start > 0:
            return
        known_run = self._runs.get(task_id)
        measured = dict(attributes, **{'error.type': error_type, ORIGIN_KEY: 'pipeline'})
        measured['_org'] = known_run.org if known_run is not None else ''
        measured['_project'] = known_run.project if known_run is not None else ''
        if error_type:
            component = body.get('c')
            with self._lock:
                if known_run is not None and component:
                    measured['_component'] = self._component_type(known_run, str(component), pipeline)
                elif body.get('ct'):
                    measured['_component'] = self._bounded_type_locked(str(body['ct'])[:MAX_VALUE_LENGTH])
                else:
                    measured['_component'] = '_unknown'
        try:
            # No exemplar on the dependency metrics: whether the pass is exported is
            # decided when it ends (a flood guard can still drop it); the model call's
            # metrics, measured then, carry one
            self._place_relayed(task_id, body, pipeline, metric, start, end, attributes, error_type)
        finally:
            # measured whether placed or not (a call is measured exactly once)
            self._queue_measurement(metric, (end - start) / 1e9, measured)

    def _place_relayed(
        self,
        task_id: str,
        body: Mapping[str, Any],
        pipeline: Optional[Dict[str, Any]],
        metric: str,
        start: int,
        end: int,
        attributes: Dict[str, Any],
        error_type: Optional[str],
    ) -> Optional[Tuple[int, int]]:
        """
        Hold a relayed call on its pass (a span in the pass's trace, with its span id
        set now); returns the (trace id, span id) of a placed failed call, else None.
        """
        pipe, component = body.get('p'), body.get('c')
        if body.get('o'):
            self.count_dropped('relay_scope_cap')
            return None
        if pipe is None or not component:
            self.count_dropped('relay_global' if body.get('ct') else 'relay_unattributed')
            return None
        placed = None
        with self._lock:
            run = self._runs.get(task_id)
            invocation = run.invocations.get(pipe) if run is not None else None
            tid = body.get('t')
            if invocation is None or (
                # Made for another pass than the one open on the pipe (a thread a
                # node started, finishing late): measured, not placed
                invocation.engine_tid
                and isinstance(tid, str)
                and _TRACE_ID.match(tid) is not None
                and int(tid, 16) != invocation.trace_id
            ):
                reason = 'relay_late'
            elif len(invocation.calls) >= MAX_RELAYED_PER_PASS or self._buffered >= MAX_BUFFERED_CALLS:
                reason = 'dependency_span_cap'
            else:
                reason = ''
                component_type = self._component_type(run, str(component), pipeline)
                name = str(body.get('n') or metric)[:MAX_VALUE_LENGTH]
                # A call that sent a traceparent (the model server) keeps the span id it sent;
                # the others get theirs now, for the exemplar
                span_id = int(body['i'], 16) if isinstance(body.get('i'), str) and _SPAN_ID.match(body['i']) else 0
                span_id = span_id or _new_ids(self._random)[1]
                invocation.calls.append((component_type, name, start, max(end, start), attributes, error_type, span_id))
                invocation.call_error = invocation.call_error or bool(error_type)
                self._buffered += 1
                if error_type:
                    placed = (invocation.trace_id, span_id)
        if reason:
            self.count_dropped(reason)
        return placed

    def _node_raised(self, task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
        """A node's handler raised: names the node for the engine's error report, and its span shows it."""
        component = body.get('c')
        if not component:
            return
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            component_type = self._component_type(run, str(component), pipeline)
            raised = [
                component_type,
                str(body.get('x') or 'Exception')[:MAX_VALUE_LENGTH],
                str(body.get('g') or '')[:MAX_CAUSE_LENGTH],
                str(body.get('l') or '')[:MAX_CAUSE_LENGTH],
                time.monotonic(),
                None,
                int(body.get('w') or 0) / 1e9 or time.time(),
                str(component),
            ]
            run.raised = raised
            invocation = run.invocations.get(body.get('p'))
            if invocation is not None and len(invocation.raised) < MAX_MARKS_PER_PASS:
                invocation.raised.append(raised)

    def _node_error_mark(self, task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
        """A node's report that its call failed: kept on its pass, to mark the component span and the pass."""
        error_type = str(body.get('x') or 'error')[:64]
        message = str(body.get('g') or '')[:256]
        with self._lock:
            run = self._runs.get(task_id)
            invocation = run.invocations.get(body.get('p')) if run is not None else None
            tid = body.get('t')
            if (
                invocation is None
                or not body.get('c')
                or (isinstance(tid, str) and _TRACE_ID.match(tid) and int(tid, 16) != invocation.trace_id)
            ):
                reason = 'relay_late'
            elif len(invocation.marks) >= MAX_MARKS_PER_PASS:
                reason = 'dependency_span_cap'
            else:
                reason = ''
                component_type = self._component_type(run, str(body['c']), pipeline)
                wall = int(body.get('w') or 0) / 1e9 or time.time()
                invocation.marks.append((component_type, wall, error_type, message, str(body['c'])))
                invocation.call_error = True  # the pass is kept, as for a failed call
        if reason:
            self.count_dropped(reason)

    def detached_context(self) -> contextvars.Context:
        """A copy of the current context with no current trace or request, for a long-lived task."""
        context = contextvars.copy_context()
        context.run(otel_context.attach, otel_context.Context())
        context.run(_deps.set_request, None)
        return context

    def instrument_dependencies(self, environ: Optional[Mapping[str, str]] = None) -> Tuple[List[str], List[str]]:
        """Record the dependency calls (RR_OTEL_INSTRUMENT); (recorded libraries, skipped with reasons)."""
        environ = os.environ if environ is None else environ
        flags, unknown = _deps.parse_flags(environ.get('RR_OTEL_INSTRUMENT'))
        done, skipped, recorders = _deps.instrument(self._tracer_provider, flags, self._dependency_call_hook)
        self._recorders.extend(recorders)
        return done, skipped + [f'{name}: unknown RR_OTEL_INSTRUMENT name' for name in unknown]

    def _dependency_call_hook(self, call: Any) -> None:
        self.guard('dependency_call', self.dependency_call, call)

    def _keep_dependency_root(self, span: Any) -> str:
        """A dependency call outside any request: kept when it failed or was slow."""
        if span.status.status_code == StatusCode.ERROR:
            return 'error'
        duration = (span.end_time - span.start_time) / 1e9 if span.end_time and span.start_time else 0.0
        if self._slow_request and duration >= self._slow_request and self._slow_allowed('', ''):
            return 'slow'
        return ''

    def _queue_measurement(self, name: str, seconds: float, attributes: Dict[str, Any]) -> None:
        if len(self._measurements) >= MAX_PENDING_MEASUREMENTS:
            self.count_dropped('dependency_metric_overflow')
            return
        self._measurements.append((name, seconds, attributes))

    def _measure_worker(self) -> None:
        while not self._measure_stop.wait(MEASURE_INTERVAL):
            self.flush_measurements()

    def flush_measurements(self) -> None:
        """Record the dependency calls measured so far (the otel-deps thread; tests; shutdown)."""
        while True:
            try:
                name, seconds, attributes = self._measurements.popleft()
            except IndexError:
                return
            try:
                self._measure_dependency(name, seconds, attributes)
            except Exception:
                self.count_dropped('dependency_metric_error')

    def _bounded_pipeline_server(self, org: str, server: str) -> str:
        """
        A pipeline's server.address as the metrics keep it: a pipeline's servers
        are its customer's choice (their database, their model endpoint), so at
        most MAX_SERVERS_PER_ORG per customer and MAX_PIPELINE_SERVERS in all,
        then '_other'. Shared by the dependency metrics and the turns.
        """
        with self._lock:
            known = self._org_servers.setdefault(org, set())
            if server in known:
                return server
            if len(known) < MAX_SERVERS_PER_ORG and (
                server in self._pipeline_servers or len(self._pipeline_servers) < MAX_PIPELINE_SERVERS
            ):
                known.add(server)
                self._pipeline_servers.add(server)
                return server
        self.count_dropped('server_overflow')
        return '_other'

    def _measure_dependency(self, name: str, seconds: float, attributes: Dict[str, Any]) -> None:
        histogram = self._dependency.get(name)
        if histogram is None:
            return
        org = self._capped_org(attributes.get('_org')) or ''  # who made the call (relayed, bounded)
        component_type = attributes.get('_component')
        exemplar = None  # dependency calls: no exemplar (see _relayed)
        # url.template stays on the call's span: as a label it would multiply each
        # server's series (the lifetime bound on attribute sets)
        values = {
            key: str(value)[:MAX_VALUE_LENGTH]
            for key, value in attributes.items()
            if key not in ('_org', '_project', '_component', '_exemplar', 'url.template') and value not in (None, '')
        }
        # A pipeline's servers are its customer's choice (their database, their
        # model endpoint): at most MAX_SERVERS_PER_ORG per customer and
        # MAX_PIPELINE_SERVERS in all, then '_other'
        server = values.get('server.address')
        if server and values.get(ORIGIN_KEY) == 'pipeline':
            values['server.address'] = self._bounded_pipeline_server(org, server)
        histogram.record(seconds, self._bounded(name, values), context=exemplar)
        if component_type and values.get('error.type'):
            failed = {
                'rocketride.component.type': component_type,
                'error.type': self._bounded_error_type(values['error.type'], 'dependency'),
                'server.address': values.get('server.address') or '_unknown',
            }
            if org and self._org_attribute:
                failed[ORG_KEY] = org
            if attributes.get('_project'):
                failed[PROJECT_KEY] = self._bounded_project(org, str(attributes['_project']))
            self._dependency_errors.add(1, self._bounded('rocketride.component.dependency_errors', failed))
