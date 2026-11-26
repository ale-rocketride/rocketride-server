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
Unit tests for the in-process OpenTelemetry sink (ai.common.telemetry._otel).

Telemetry is a function of the existing task events: these tests replay the
events EaaS already forwards (task begin, status updates, component
enter/leave, >MET* metrics, node errors/warnings) plus the run outcome, and
check the metrics and spans produced — including the edge cases that must
never grow memory, double count, or leak an identifying value into a label.
"""

import json
import threading
import time

import pytest

pytest.importorskip('opentelemetry.sdk')

from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import StatusCode  # noqa: E402

from ai.common.telemetry import _common, _dependencies, _otel, _runs  # noqa: E402
from ai.common.telemetry._otel import OUTCOMES, RUN_KINDS, Telemetry  # noqa: E402

PIPELINE = {
    'components': [
        {'id': 'my_secret_llm_name', 'provider': 'llm_openai'},
        {'id': 'parse_1', 'provider': 'parse'},
    ]
}
IDENTITY = {'org_id': 'org-a', 'run_kind': 'dev'}


def identity():
    return IDENTITY


# Every Telemetry a test made: shut down after the test, whether its assertions passed or not
_made = []


def make(**kwargs):
    kwargs.setdefault('org_attribute', True)  # the per-customer metrics (off by default)
    spans = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    telemetry = Telemetry(span_exporter=spans, metric_reader=reader, service_version='9.9.9', **kwargs)
    _made.append(telemetry)
    return telemetry, spans, reader


@pytest.fixture(autouse=True)
def _shutdown_made():
    yield
    while _made:
        telemetry = _made.pop()
        if not telemetry._measure_stop.is_set():  # the test did not shut it down itself
            telemetry.shutdown(1)


@pytest.fixture
def sink():
    telemetry, spans, reader = make()
    yield telemetry, spans, reader
    telemetry.shutdown(1)


def points(reader):
    """{metric name: [(attributes, point)]} from one collection."""
    out = {}
    data = reader.get_metrics_data()
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                out.setdefault(metric.name, []).extend((dict(p.attributes), p) for p in metric.data.data_points)
    return out


def value(collected, name, **attributes):
    for attrs, point in collected.get(name, []):
        if all(attrs.get(k.replace('__', '.')) == v for k, v in attributes.items()):
            return point
    return None


def finished(telemetry, spans):
    assert telemetry.flush_traces(5)  # pipeline traces are built on the otel-traces thread
    telemetry._tracer_provider.force_flush()
    return spans.get_finished_spans()


def begin(telemetry, task_id='task-1'):
    telemetry.task_event(task_id, {'event': 'apaevt_status_update', 'body': {'state': 1}}, identity)
    telemetry.task_event(
        task_id, {'event': 'apaevt_task', 'body': {'action': 'begin', 'name': 'Secret task'}}, identity
    )
    telemetry.task_event(task_id, {'event': 'apaevt_status_update', 'body': {'state': 3}}, identity)


def call(telemetry, component, pipe=0, start=100.0, end=101.0, result=None, task_id='task-1'):
    telemetry.component_trace(task_id, 'enter', pipe, component, start, {}, PIPELINE)
    trace = {'result': result} if result else {}
    telemetry.component_trace(task_id, 'leave', pipe, component, end, trace, PIPELINE)


# ── Runs ──────────────────────────────────────────────────────────────────────


def test_successful_run(sink, capsys):
    telemetry, spans, reader = sink
    begin(telemetry)
    call(telemetry, 'my_secret_llm_name')
    telemetry.task_event(
        'task-1',
        {'event': 'apaevt_status_update', 'body': {'state': 3, 'completedCount': 3, 'failedCount': 1}},
        identity,
    )
    collected = points(reader)
    assert value(collected, 'rocketride.task.count', rocketride__task__state='running').value == 1

    telemetry.task_ended('task-1', IDENTITY, 'ok', None, None)
    collected = points(reader)

    assert (
        value(collected, 'rocketride.task.started', rocketride__task__run_kind='dev', rocketride__org__id='org-a').value
        == 1
    )
    ended = value(collected, 'rocketride.task.ended', rocketride__task__outcome='ok', rocketride__org__id='org-a')
    assert ended.value == 1 and 'error.type' not in dict(ended.attributes)
    assert value(collected, 'rocketride.task.duration', rocketride__task__outcome='ok').count == 1
    assert value(collected, 'rocketride.task.startup.duration').count == 1
    component = value(collected, 'rocketride.component.duration', rocketride__component__type='llm_openai')
    assert component.count == 1
    assert (
        value(
            collected,
            'rocketride.object.processed',
            rocketride__object__outcome='completed',
            rocketride__org__id='org-a',
        ).value
        == 3
    )
    assert (
        value(
            collected, 'rocketride.object.processed', rocketride__object__outcome='failed', rocketride__org__id='org-a'
        ).value
        == 1
    )
    assert value(collected, 'rocketride.task.count', rocketride__task__state='running').value == 0

    names = [s.name for s in finished(telemetry, spans)]
    assert names.count('pipeline') == 1 and names.count('component llm_openai') == 1 and names.count('task') == 1
    pipeline = [s for s in finished(telemetry, spans) if s.name == 'pipeline'][0]
    child = [s for s in finished(telemetry, spans) if s.name == 'component llm_openai'][0]
    assert child.parent.span_id == pipeline.context.span_id
    assert pipeline.attributes['rocketride.sampling.reason'] == 'ratio'

    line = json.loads([out for out in capsys.readouterr().out.splitlines() if 'rocketride.task.ended' in out][-1])
    task_span = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert line['trace_id'] == format(task_span.context.trace_id, '032x')
    assert line['outcome'] == 'ok' and line['org_id'] == 'org-a' and line['task_id'] == 'task-1'


def test_failed_run_carries_error_type(sink):
    telemetry, spans, reader = sink
    begin(telemetry)
    telemetry.task_ended('task-1', IDENTITY, 'error', None, 'signal')
    ended = value(points(reader), 'rocketride.task.ended', error__type='signal', rocketride__org__id='org-a')
    assert ended.value == 1
    task = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert task.status.status_code == StatusCode.ERROR and task.attributes['error.type'] == 'signal'


def test_a_failed_run_says_what_failed_on_its_trace(sink):
    """The run's first error on the task span: its status message and an OTel exception event."""
    telemetry, spans, _ = sink
    begin(telemetry)
    cause = {
        'message': 'Profile claude-haiku is not defined in llm_anthropic',
        'location': '/workspace/rocketride-server/packages/ai/src/ai/common/config.py:465',
        'type': 'ValueError',
    }
    telemetry.task_ended('task-1', IDENTITY, 'error', None, 'start_error', 'Exception', cause)
    task = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert task.status.description == (
        'run ended in error (start_error): Profile claude-haiku is not defined in llm_anthropic'
    )
    (event,) = task.events
    assert event.name == 'exception'
    assert dict(event.attributes) == {
        'exception.message': 'Profile claude-haiku is not defined in llm_anthropic',
        'exception.type': 'ValueError',
        'code.file.path': '/workspace/rocketride-server/packages/ai/src/ai/common/config.py',
        'code.line.number': 465,
    }


def test_a_long_cause_is_cut(sink):
    telemetry, spans, _ = sink
    begin(telemetry)
    telemetry.task_ended('task-1', IDENTITY, 'error', None, 'signal', None, {'message': 'x' * 1000})
    task = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert task.status.description == 'run ended in error (signal): ' + 'x' * 300


def test_error_type_only_on_errors(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    telemetry.task_ended('task-1', IDENTITY, 'cancelled', 'disconnect', 'signal')
    ended = value(
        points(reader), 'rocketride.task.ended', rocketride__task__stop_reason='disconnect', rocketride__org__id='org-a'
    )
    assert ended.value == 1 and 'error.type' not in dict(ended.attributes)


def test_failed_start_still_counts_as_ended(sink):
    telemetry, spans, reader = sink
    telemetry.task_ended('task-2', {'org_id': 'org-b', 'run_kind': 'deploy'}, 'error', 'start_failed', 'start_failed')
    collected = points(reader)
    assert value(collected, 'rocketride.task.ended', error__type='start_failed', rocketride__org__id='org-b').value == 1
    assert value(collected, 'rocketride.task.duration', rocketride__task__run_kind='deploy') is None
    assert not [s for s in finished(telemetry, spans) if s.name == 'task']


def test_sli_counters_start_at_zero(sink):
    telemetry, _, reader = sink
    collected = points(reader)
    for run_kind in RUN_KINDS:
        assert value(collected, 'rocketride.task.started', rocketride__task__run_kind=run_kind).value == 0
        for outcome, stop, error_type in OUTCOMES:
            match = {
                'rocketride__task__run_kind': run_kind,
                'rocketride__task__outcome': outcome,
                'rocketride__task__stop_reason': stop,
            }
            if error_type:
                match['error__type'] = error_type
            assert value(collected, 'rocketride.task.ended', **match).value == 0


def test_runs_lost_are_errors(sink):
    telemetry, _, reader = sink
    telemetry.runs_lost([{'org_id': 'org-a', 'run_kind': 'deploy'}] * 2)
    assert (
        value(points(reader), 'rocketride.task.ended', error__type='engine_lost', rocketride__org__id='org-a').value
        == 2
    )


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_late_status_after_end_is_ignored(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    final = {'event': 'apaevt_status_update', 'body': {'state': 6, 'completedCount': 5, 'failedCount': 0}}
    telemetry.task_event('task-1', final, identity)
    telemetry.task_ended('task-1', IDENTITY, 'cancelled', 'user', None)
    telemetry.task_event('task-1', final, identity)

    collected = points(reader)
    assert (
        value(
            collected,
            'rocketride.object.processed',
            rocketride__object__outcome='completed',
            rocketride__org__id='org-a',
        ).value
        == 5
    )
    assert all(p.value == 0 for _, p in collected['rocketride.task.count'])

    begin(telemetry)  # a new run of the same task is tracked again
    assert value(points(reader), 'rocketride.task.count', rocketride__task__state='running').value == 1


def test_duplicate_begin_counts_once(sink):
    telemetry, spans, reader = sink
    begin(telemetry)
    begin(telemetry)
    telemetry.task_ended('task-1', IDENTITY, 'ok', None, None)
    assert value(points(reader), 'rocketride.task.started', rocketride__org__id='org-a').value == 1
    assert len([s for s in finished(telemetry, spans) if s.name == 'task']) == 1


def test_restart_resets_open_calls_and_types(sink):
    telemetry, spans, reader = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.0, {}, PIPELINE)
    telemetry.task_restarted('task-1')
    # the new process's pipeline has a different component under the same id
    new_pipeline = {'components': [{'id': 'parse_1', 'provider': 'ocr'}]}
    telemetry.component_trace('task-1', 'leave', 0, 'parse_1', 101.0, {}, new_pipeline)  # stale leave: ignored
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 102.0, {}, new_pipeline)
    telemetry.component_trace('task-1', 'leave', 0, 'parse_1', 103.0, {}, new_pipeline)
    collected = points(reader)
    assert value(collected, 'rocketride.component.duration', rocketride__component__type='ocr').count == 1
    assert value(collected, 'rocketride.component.duration', rocketride__component__type='parse') is None
    assert telemetry._buffered == 0


def test_restart_counts_objects_and_tokens_from_zero(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    status = {'event': 'apaevt_status_update', 'body': {'state': 3, 'completedCount': 3}}
    telemetry.task_event('task-1', status, identity)
    metrics = {'event': 'apaevt_status_metrics', 'body': {'metrics': {'counters': {'llm_input_tokens': 50}}}}
    telemetry.task_event('task-1', metrics, identity)
    telemetry.task_restarted('task-1')
    # the new subprocess's first report is already above the old count
    status['body']['completedCount'] = 10
    telemetry.task_event('task-1', status, identity)
    metrics['body']['metrics']['counters']['llm_input_tokens'] = 80
    telemetry.task_event('task-1', metrics, identity)
    collected = points(reader)
    completed = value(
        collected, 'rocketride.object.processed', rocketride__object__outcome='completed', rocketride__org__id='org-a'
    )
    assert completed.value == 13
    assert (
        value(collected, 'rocketride.llm.tokens', gen_ai__token__type='input', rocketride__org__id='org-a').value == 130
    )


def test_leave_without_enter_and_stack_cap(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'leave', 0, 'parse_1', 100.0, {}, PIPELINE)
    assert value(points(reader), 'rocketride.component.duration') is None
    for _ in range(_common.MAX_STACK_DEPTH + 50):  # leaves lost (killed process)
        telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.0, {}, PIPELINE)
    assert len(telemetry._runs['task-1'].invocations[0].stack) == _common.MAX_STACK_DEPTH


def test_invocation_cap(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    for pipe in range(_common.MAX_OPEN_INVOCATIONS + 10):
        telemetry.component_trace('task-1', 'enter', pipe, 'parse_1', 100.0, {}, PIPELINE)
    assert len(telemetry._runs['task-1'].invocations) == _common.MAX_OPEN_INVOCATIONS
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='invocation_cap')
    assert dropped.value == 10


def test_a_lost_leave_does_not_stall_the_pipe_when_enabled(monkeypatch):
    # With RR_OTEL_STALE_CALL_SECONDS: a call whose leave never came is closed by
    # the next enter on that pipe after the threshold, and the pipe traces again
    telemetry, spans, reader = make(stale_call_seconds=60)
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'my_secret_llm_name', 100.0, {}, PIPELINE)  # never left
    call(telemetry, 'parse_1', pipe=0)
    assert telemetry._buffered == 1
    clock = time.monotonic() + 61
    monkeypatch.setattr(_otel.time, 'monotonic', lambda: clock)
    call(telemetry, 'parse_1', pipe=0)  # a new pass on the same pipe
    pipelines = [s for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert len(pipelines) == 2
    assert sum(1 for s in pipelines if s.attributes.get('rocketride.span.unclosed')) == 1
    assert telemetry._buffered == 0
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='stale_call')
    assert dropped.value == 1
    telemetry.shutdown(1)


def test_a_long_nested_pass_stays_one_trace_by_default(monkeypatch, sink):
    # The outer call stays open for the whole pass: off by default, nothing splits it
    telemetry, spans, reader = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'my_secret_llm_name', 100.0, {}, PIPELINE)
    clock = time.monotonic() + 7200  # a two-hour pass
    monkeypatch.setattr(_otel.time, 'monotonic', lambda: clock)
    for _ in range(3):
        call(telemetry, 'parse_1', pipe=0)
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 7300.0, {}, PIPELINE)
    pipelines = [s for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert len(pipelines) == 1 and not pipelines[0].attributes.get('rocketride.span.unclosed')
    assert pipelines[0].attributes['rocketride.pipeline.component_calls'] == 4
    collected = points(reader)
    assert value(collected, 'rocketride.component.duration', rocketride__component__type='llm_openai').count == 1


def test_request_error_codes_are_bounded(monkeypatch, sink):
    telemetry, _, reader = sink
    monkeypatch.setattr(_otel, 'MAX_ERROR_TYPES', 2)
    for code in ('A_CODE', 'B_CODE', 'C_CODE'):  # any exception's .code reaches error.type
        telemetry.request_done('execute', code, 0.01)
    collected = points(reader)
    assert value(collected, 'rpc.server.call.duration', error__type='_OTHER').count == 1
    assert value(collected, 'rpc.server.call.duration', error__type='A_CODE').count == 1
    # request codes have their own budget: node error codes still get theirs
    begin(telemetry)
    telemetry.task_event(
        'task-1', {'event': 'apaevt_status_error', 'body': {'message': 'NodeCode*x*a.cpp:1'}}, identity
    )
    assert value(points(reader), 'rocketride.component.errors', error__type='NodeCode').value == 1


def test_request_codes_must_look_like_codes(sink):
    telemetry, _, reader = sink
    telemetry.request_done('execute', 'user secret@example.com not found', 0.01)  # free text, never a label
    assert value(points(reader), 'rpc.server.call.duration', error__type='_OTHER').count == 1


def test_an_evicted_run_that_reports_again_is_tracked_anew(monkeypatch):
    # Documented limit (past MAX_LIVE_TASKS only): the run comes back with no
    # baseline, so its cumulative object count is counted again
    monkeypatch.setattr(_runs, 'MAX_LIVE_TASKS', 1)
    telemetry, _, reader = make()
    begin(telemetry, 't0')
    telemetry.task_event('t0', {'event': 'apaevt_status_update', 'body': {'state': 3, 'completedCount': 2}}, identity)
    begin(telemetry, 't1')  # evicts t0
    telemetry.task_event('t0', {'event': 'apaevt_status_update', 'body': {'state': 3, 'completedCount': 3}}, identity)
    assert 't0' in telemetry._runs
    collected = points(reader)
    assert (
        value(
            collected,
            'rocketride.object.processed',
            rocketride__object__outcome='completed',
            rocketride__org__id='org-a',
        ).value
        == 5
    )
    assert value(collected, 'rocketride.telemetry.dropped', rocketride__telemetry__reason='run_evicted').value == 2
    telemetry.shutdown(1)


def test_component_span_cap_has_a_ceiling():
    telemetry, _, _ = make(component_span_cap=10**9)
    assert telemetry._span_cap == _otel.MAX_COMPONENT_SPANS
    telemetry.shutdown(1)


def test_buffered_calls_budget_is_global_and_given_back(monkeypatch):
    monkeypatch.setattr(_runs, 'MAX_BUFFERED_CALLS', 3)
    monkeypatch.setattr(_dependencies, 'MAX_BUFFERED_CALLS', 3)
    telemetry, spans, reader = make()
    begin(telemetry)
    begin(telemetry, 'task-2')
    for task_id in ('task-1', 'task-2'):
        telemetry.component_trace(task_id, 'enter', 0, 'my_secret_llm_name', 100.0, {}, PIPELINE)
        for _ in range(2):
            call(telemetry, 'parse_1', task_id=task_id)
    assert telemetry._buffered == 3  # task-2's second call is over the budget
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='span_budget')
    assert dropped.value == 1
    # the duration metric does not depend on trace buffering
    assert value(points(reader), 'rocketride.component.duration', rocketride__component__type='parse').count == 4
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 110.0, {}, PIPELINE)
    telemetry.task_ended('task-2', IDENTITY, 'ok', None, None)
    assert telemetry._buffered == 0
    calls = sorted(
        s.attributes['rocketride.pipeline.component_calls'] for s in finished(telemetry, spans) if s.name == 'pipeline'
    )
    assert calls == [2, 3]  # task-2 (unclosed): 2 kept + 1 left out; task-1: 3
    telemetry.shutdown(1)


def test_a_full_trace_queue_drops_and_counts(monkeypatch):
    monkeypatch.setattr(_otel, 'TRACE_QUEUE', 2)
    telemetry, spans, reader = make()
    release = threading.Event()
    original = telemetry._export_invocation
    monkeypatch.setattr(telemetry, '_export_invocation', lambda *a: (release.wait(5), original(*a)))
    try:
        begin(telemetry)
        for pipe in range(6):
            call(telemetry, 'parse_1', pipe=pipe)  # never blocks the caller
    finally:
        release.set()
    exported = len([s for s in finished(telemetry, spans) if s.name == 'pipeline'])
    assert exported in (2, 3)
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='trace_queue_full')
    assert dropped.value in (3, 4)  # one may be in the thread's hands, not the queue
    assert exported + dropped.value == 6  # every pass accounted for
    telemetry.shutdown(1)


def test_live_run_cap_evicts_least_recently_active_and_still_counts(monkeypatch):
    monkeypatch.setattr(_runs, 'MAX_LIVE_TASKS', 3)
    telemetry, _, reader = make()
    for n in range(3):
        begin(telemetry, f't{n}')
    # t0 is a long-running deploy that keeps reporting: it must not go first
    telemetry.task_event('t0', {'event': 'apaevt_status_update', 'body': {'state': 3, 'completedCount': 1}}, identity)
    begin(telemetry, 't3')
    begin(telemetry, 't4')
    assert list(telemetry._runs) == ['t0', 't3', 't4']
    collected = points(reader)
    assert value(collected, 'rocketride.task.started', rocketride__org__id='org-a').value == 5
    assert value(collected, 'rocketride.telemetry.dropped', rocketride__telemetry__reason='run_evicted').value == 2
    telemetry.shutdown(1)


def test_span_never_ends_before_it_starts(sink):
    telemetry, spans, _ = sink
    begin(telemetry)
    call(telemetry, 'parse_1', start=200.0, end=150.0)  # leave stamped earlier than enter
    got = finished(telemetry, spans)
    assert {'pipeline', 'component parse'} <= {span.name for span in got}
    for span in got:
        assert span.end_time >= span.start_time


def test_nested_and_reentrant_calls_pair_by_identity(sink):
    telemetry, spans, reader = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'my_secret_llm_name', 100.0, {}, PIPELINE)
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.5, {}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 101.0, {}, PIPELINE)  # out of order
    telemetry.component_trace('task-1', 'leave', 0, 'parse_1', 101.5, {}, PIPELINE)
    collected = points(reader)
    assert value(collected, 'rocketride.component.duration', rocketride__component__type='llm_openai').count == 1
    assert value(collected, 'rocketride.component.duration', rocketride__component__type='parse').count == 1
    pipelines = [s for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert len(pipelines) == 1 and pipelines[0].attributes['rocketride.pipeline.component_calls'] == 2


def test_open_invocations_close_with_the_run(sink):
    telemetry, spans, _ = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.0, {}, PIPELINE)
    telemetry.task_ended('task-1', IDENTITY, 'error', None, 'signal')
    pipeline = [s for s in finished(telemetry, spans) if s.name == 'pipeline'][0]
    assert pipeline.attributes['rocketride.span.unclosed'] is True
    assert pipeline.status.status_code == StatusCode.ERROR


# ── Sampling ──────────────────────────────────────────────────────────────────


def test_sampling_keeps_failures_and_slow_runs_at_ratio_zero():
    telemetry, spans, _ = make(sample_ratio=0.0, slow_seconds=5)
    begin(telemetry)
    call(telemetry, 'parse_1', pipe=0, start=100, end=101)  # ok, fast: dropped
    call(telemetry, 'parse_1', pipe=1, start=100, end=101, result='error')  # failed: kept
    call(telemetry, 'parse_1', pipe=2, start=100, end=110)  # slow: kept
    reasons = sorted(
        s.attributes['rocketride.sampling.reason'] for s in finished(telemetry, spans) if s.name == 'pipeline'
    )
    assert reasons == ['error', 'slow']
    failed = [
        s
        for s in finished(telemetry, spans)
        if s.name == 'component parse' and s.status.status_code == StatusCode.ERROR
    ]
    assert len(failed) == 1
    telemetry.shutdown(1)


def test_ratio_zero_exports_errors_only(capsys):
    # RR_OTEL_TRACES_SAMPLE_RATIO=0: no trace for what did not fail, every failure kept
    telemetry, spans, _ = make(sample_ratio=0.0)
    begin(telemetry, 'ok-run')
    call(telemetry, 'parse_1', task_id='ok-run')
    telemetry.task_ended('ok-run', IDENTITY, 'ok', None, None)
    begin(telemetry, 'bad-run')
    call(telemetry, 'parse_1', task_id='bad-run', result='error')
    telemetry.task_ended('bad-run', IDENTITY, 'error', None, 'signal')
    parent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
    telemetry.request_done('execute', None, 0.01, parent)
    telemetry.request_done('execute', 'OUT_OF_CREDITS', 0.01, parent)
    got = finished(telemetry, spans)
    assert [s.attributes['rocketride.task.id'] for s in got if s.name == 'task'] == ['bad-run']
    assert [s.attributes['rocketride.task.id'] for s in got if s.name == 'pipeline'] == ['bad-run']
    assert [s.status.status_code for s in got if s.name == 'execute'] == [StatusCode.ERROR]
    lines = [json.loads(o) for o in capsys.readouterr().out.splitlines() if 'rocketride.task.ended' in o]
    assert [(e['outcome'], e['level'], bool(e['trace_id'])) for e in lines] == [
        ('ok', 'info', False),
        ('error', 'error', True),
    ]
    telemetry.shutdown(1)


def test_engine_error_code_on_span_log_and_a_bounded_label(capsys):
    telemetry, spans, reader = make(sample_ratio=0.0)
    begin(telemetry, 'r')
    telemetry.task_ended('r', IDENTITY, 'error', None, 'start_rejected', 'InvalidSchema')
    span = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert span.attributes['rocketride.engine.error_code'] == 'InvalidSchema'
    line = json.loads([o for o in capsys.readouterr().out.splitlines() if 'rocketride.task.ended' in o][-1])
    assert (line['error_type'], line['error_code']) == ('start_rejected', 'InvalidSchema')
    # why the start failed, on the metric: the engine's codes are a fixed set (bounded all the same)
    labelled = [(a, p) for a, p in points(reader)['rocketride.task.ended'] if p.value]
    assert [a.get('rocketride.engine.error_code') for a, _ in labelled] == ['InvalidSchema']
    telemetry.shutdown(1)


def test_trace_ids_are_assigned_when_the_run_and_pass_begin(capsys):
    # A log line written while the run is going names the trace its span gets later
    telemetry, spans, _ = make(sample_ratio=0.0)
    begin(telemetry)
    run = telemetry._runs['task-1']
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.0, {}, PIPELINE)
    invocation = run.invocations[0]
    logged = telemetry.error_log_trace('task-1')  # an error log names the run's trace
    assert logged == format(run.trace_id, '032x')
    telemetry.component_trace('task-1', 'leave', 0, 'parse_1', 101.0, {'result': 'error'}, PIPELINE)
    telemetry.task_ended('task-1', IDENTITY, 'ok', None, None)  # an OK run, but it was logged about
    got = finished(telemetry, spans)
    task = [s for s in got if s.name == 'task'][0]
    assert format(task.context.trace_id, '032x') == logged and task.context.span_id == run.span_id
    assert task.attributes['rocketride.sampling.reason'] == 'error_log'
    pipeline = [s for s in got if s.name == 'pipeline'][0]
    assert pipeline.context.trace_id == invocation.trace_id and pipeline.context.span_id == invocation.span_id
    # the pass links to its run's trace
    assert [(link.context.trace_id, link.context.span_id) for link in pipeline.links] == [(run.trace_id, run.span_id)]
    line = json.loads([o for o in capsys.readouterr().out.splitlines() if 'rocketride.task.ended' in o][-1])
    assert line['trace_id'] == logged
    telemetry.shutdown(1)


def test_a_kept_pass_keeps_its_run(capsys):
    telemetry, spans, _ = make(sample_ratio=0.0)
    begin(telemetry)
    call(telemetry, 'parse_1', result='error')  # a failed pass: kept
    telemetry.task_ended('task-1', IDENTITY, 'ok', None, None)
    task = [s for s in finished(telemetry, spans) if s.name == 'task'][0]
    assert task.attributes['rocketride.sampling.reason'] == 'pass_kept'
    telemetry.shutdown(1)


def test_error_log_trace_for_an_unknown_run_is_empty(sink):
    telemetry, _, _ = sink
    assert telemetry.error_log_trace('nope') == ''


def test_errors_and_slow_are_kept_and_slow_is_capped(monkeypatch):
    telemetry, spans, reader = make(sample_ratio=0.0, slow_seconds=5, slow_traces_per_minute=10)
    begin(telemetry)
    for pipe in range(30):  # 30 slow passes: the per-run cap (a tenth, at least 1) keeps one
        call(telemetry, 'parse_1', pipe=pipe, start=100, end=110)
    for pipe in range(30, 60):  # 30 failed passes: errors are never capped
        call(telemetry, 'parse_1', pipe=pipe, start=100, end=101, result='error')
    reasons = [s.attributes['rocketride.sampling.reason'] for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert reasons.count('error') == 30
    assert reasons.count('slow') == 1
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='slow_rate_limited')
    assert dropped.value == 29
    telemetry.shutdown(1)


def test_slow_runs_are_kept_but_never_deploys(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(_otel.time, 'monotonic', lambda: clock[0])
    telemetry, spans, _ = make(sample_ratio=0.0, slow_run_seconds=60)
    deploy = {'org_id': 'org-a', 'run_kind': 'deploy'}
    begin(telemetry, 'dev-run')
    telemetry.task_event('deploy-run', {'event': 'apaevt_task', 'body': {'action': 'begin'}}, lambda: deploy)
    clock[0] += 120
    telemetry.task_ended('dev-run', IDENTITY, 'ok', None, None)
    telemetry.task_ended('deploy-run', deploy, 'ok', None, None)
    kept = {
        s.attributes['rocketride.task.id']: s.attributes['rocketride.sampling.reason']
        for s in finished(telemetry, spans)
        if s.name == 'task'
    }
    assert kept == {'dev-run': 'slow'}
    telemetry.shutdown(1)


def test_slow_requests_are_kept_except_long_lived_methods():
    telemetry, spans, _ = make(sample_ratio=0.0, slow_request_seconds=2, slow_request_exclude='rrext_cprofile_*')
    parent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
    telemetry.request_done('execute', None, 0.5, parent)  # fast OK: dropped
    telemetry.request_done('execute', None, 3.0, parent)  # slow: kept
    telemetry.request_done('rrext_cprofile_stop', None, 30.0, parent)  # long by design: dropped
    got = [(s.name, s.attributes['rocketride.sampling.reason']) for s in finished(telemetry, spans)]
    assert got == [('execute', 'slow')]
    telemetry.shutdown(1)


def test_sampling_ratio_is_honoured():
    telemetry, spans, _ = make(sample_ratio=0.1)
    telemetry._random.seed(7)
    begin(telemetry)
    for pipe in range(2000):
        call(telemetry, 'parse_1', pipe=pipe)
    kept = len([s for s in finished(telemetry, spans) if s.name == 'pipeline'])
    assert 120 < kept < 280  # ~10% of 2000
    telemetry.shutdown(1)


def test_component_span_cap(monkeypatch):
    telemetry, spans, reader = make(component_span_cap=2)
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'my_secret_llm_name', 100.0, {}, PIPELINE)
    for _ in range(5):
        call(telemetry, 'parse_1', pipe=0)
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 110.0, {}, PIPELINE)
    pipeline = [s for s in finished(telemetry, spans) if s.name == 'pipeline'][0]
    assert pipeline.attributes['rocketride.pipeline.component_calls'] == 6
    assert len([s for s in finished(telemetry, spans) if s.name.startswith('component')]) == 2
    assert value(points(reader), 'rocketride.component.duration', rocketride__component__type='parse').count == 5
    telemetry.shutdown(1)


# ── Nodes: errors and tokens, for every node with no node code ────────────────


def test_node_errors_are_attributed_by_folder(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    call(telemetry, 'my_secret_llm_name')  # learns the pipeline's providers

    def report(severity, message):
        event = 'apaevt_status_error' if severity == 'error' else 'apaevt_status_warning'
        telemetry.task_event('task-1', {'event': event, 'body': {'message': message}}, identity)

    report('warning', 'Exception*rate limited*/opt/rocketride/nodes/llm_openai/IInstance.py:88:writeQuestions')
    report('error', 'Error*engine failure*store.cpp:10')  # the engine itself
    report('error', 'Exception*bad pdf*parse.cpp:120')  # a C++ node, by its source file
    report('error', 'x*y*/a/nodes/not_in_pipe/x.py:1')  # a node this run does not have
    report('error', 'Cancelled*Condition wait*Queue.hpp:179')  # a stop winding down: not an error
    report('error', 'Weird Code!*text*scan.cpp:1')  # an unexpected code is bounded

    collected = points(reader)
    assert (
        value(
            collected,
            'rocketride.component.errors',
            rocketride__component__type='llm_openai',
            rocketride__component__severity='warning',
        ).value
        == 1
    )
    warning = value(collected, 'rocketride.component.errors', rocketride__component__severity='warning')
    assert 'error.type' not in dict(warning.attributes)  # a warning is not an error
    assert dict(warning.attributes)['rocketride.component.code'] == 'Exception'  # but it says which one
    assert (
        value(
            collected, 'rocketride.component.errors', rocketride__component__type='_engine', error__type='Error'
        ).value
        == 1
    )
    assert (
        value(
            collected, 'rocketride.component.errors', rocketride__component__type='parse', error__type='Exception'
        ).value
        == 1
    )
    assert value(collected, 'rocketride.component.errors', rocketride__component__type='_other').value == 1
    assert value(collected, 'rocketride.component.errors', error__type='Cancelled') is None
    assert value(collected, 'rocketride.component.errors', error__type='_OTHER').value == 1


def test_node_error_codes_are_bounded(monkeypatch, sink):
    telemetry, _, reader = sink
    monkeypatch.setattr(_otel, 'MAX_ERROR_TYPES', 2)
    begin(telemetry)
    for code in ('CodeA', 'CodeB', 'CodeC', 'CodeD'):  # e.g. a community node inventing codes
        telemetry.task_event(
            'task-1', {'event': 'apaevt_status_error', 'body': {'message': f'{code}*x*a.cpp:1'}}, identity
        )
    collected = points(reader)
    assert value(collected, 'rocketride.component.errors', error__type='_OTHER').value == 2
    assert (
        value(collected, 'rocketride.telemetry.dropped', rocketride__telemetry__reason='error_type_overflow').value == 2
    )


def test_llm_tokens_from_cumulative_metrics(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    snapshot = {
        'event': 'apaevt_status_metrics',
        'body': {'metrics': {'counters': {'llm_input_tokens': 100, 'llm_output_tokens': 20}}},
    }
    telemetry.task_event('task-1', snapshot, identity)
    snapshot['body']['metrics']['counters'] = {'llm_input_tokens': 150, 'llm_output_tokens': 20}
    telemetry.task_event('task-1', snapshot, identity)
    snapshot['body']['metrics']['counters'] = {'llm_input_tokens': 10, 'llm_output_tokens': 5}  # restart: new baseline
    telemetry.task_event('task-1', snapshot, identity)
    collected = points(reader)
    assert (
        value(collected, 'rocketride.llm.tokens', gen_ai__token__type='input', rocketride__org__id='org-a').value == 160
    )
    assert (
        value(collected, 'rocketride.llm.tokens', gen_ai__token__type='output', rocketride__org__id='org-a').value == 25
    )


# ── Labels: bounded and never identifying beyond the org id ───────────────────

ALLOWED = {
    'rocketride.task.run_kind',
    'rocketride.task.outcome',
    'rocketride.task.stop_reason',
    'rocketride.task.state',
    'rocketride.object.outcome',
    'rocketride.turn.outcome',  # ok | error | abandoned
    'rocketride.turn.cause',  # platform | provider | customer
    'rocketride.turn.carried',  # true | false: the pass carried a question or a document
    'rocketride.component.type',
    'rocketride.component.result',
    'rocketride.component.severity',
    'rocketride.component.code',  # a warning's engine code, bounded
    'gen_ai.token.type',
    'rocketride.telemetry.hook',
    'rocketride.telemetry.reason',
    'rocketride.org.id',
    'rpc.system.name',
    'rpc.method',
    'error.type',
}


def test_labels_are_bounded_and_anonymous(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    call(telemetry, 'unknown_id', result='error')
    telemetry.request_done('execute', 'OUT_OF_CREDITS', 0.01)
    telemetry.task_ended('task-1', IDENTITY, 'ok', None, None)
    collected = points(reader)
    assert value(
        collected,
        'rocketride.component.duration',
        rocketride__component__type='_unknown',
        rocketride__component__result='error',
    )
    request = value(collected, 'rpc.server.call.duration', rpc__method='execute', error__type='OUT_OF_CREDITS')
    assert dict(request.attributes)['rpc.system.name'] == 'rocketride_dap'
    for name, entries in collected.items():
        for attrs, _ in entries:
            assert set(attrs) <= ALLOWED, (name, attrs)
            assert not {'task-1', 'Secret task', 'my_secret_llm_name'} & set(map(str, attrs.values()))


def test_org_cardinality_cap_and_switch():
    telemetry, _, reader = make(org_cardinality=2)
    for n in range(4):
        telemetry.task_ended(f't{n}', {'org_id': f'org-{n}', 'run_kind': 'dev'}, 'ok', None, None)
    collected = points(reader)
    orgs = {dict(p.attributes).get('rocketride.org.id') for _, p in collected['rocketride.task.ended'] if p.value}
    assert orgs == {'org-0', 'org-1', '_other'}
    assert value(collected, 'rocketride.telemetry.dropped', rocketride__telemetry__reason='org_overflow').value >= 2
    telemetry.shutdown(1)

    telemetry, _, reader = make(org_attribute=False)
    telemetry.task_ended('t', {'org_id': 'org-a', 'run_kind': 'dev'}, 'ok', None, None)
    assert all('rocketride.org.id' not in attrs for attrs, _ in points(reader)['rocketride.task.ended'])
    telemetry.shutdown(1)


def test_the_org_id_is_on_spans_and_lines_always_and_on_metrics_only_when_asked(monkeypatch, capsys):
    built = []
    with monkeypatch.context() as patched:
        patched.setattr(_otel, '_check_transport', lambda environ: '')
        patched.setattr(Telemetry, '__init__', lambda self, **kwargs: built.append(kwargs['org_attribute']))
        Telemetry.from_environment(environ={})
        Telemetry.from_environment(environ={'RR_OTEL_ORG_ATTRIBUTE': 'true'})
    assert built == [False, True]  # off on metrics unless asked

    telemetry, spans, reader = make(org_attribute=False, org_cardinality=1)
    for n in range(2):
        identity = {'org_id': f'org-{n}', 'run_kind': 'dev'}
        for event in (
            {'event': 'apaevt_status_update', 'body': {'state': 1}},
            {'event': 'apaevt_task', 'body': {'action': 'begin', 'name': 'x'}},
            {'event': 'apaevt_status_update', 'body': {'state': 3}},
        ):
            telemetry.task_event(f't{n}', event, lambda i=identity: i)
        telemetry.task_ended(f't{n}', identity, 'error', 'start_error', None)
    telemetry.request_done(
        'execute', None, 0.05, f'00-{PARENT_TRACE}-b7ad6b7169203331-01', identity={'org_id': 'org-1'}
    )
    collected = points(reader)
    for name, entries in collected.items():
        assert all('rocketride.org.id' not in attrs for attrs, _ in entries), name
    assert telemetry.attributed_orgs() == frozenset()
    kept = finished(telemetry, spans)
    # spans: every org, uncapped (a span makes no series)
    assert {s.attributes.get('rocketride.org.id') for s in kept if s.name == 'task'} == {'org-0', 'org-1'}
    assert [s.attributes['rocketride.org.id'] for s in kept if s.name == 'execute'] == ['org-1']
    lines = [json.loads(x) for x in capsys.readouterr().out.splitlines() if 'rocketride.task.ended' in x]
    assert {line['org_id'] for line in lines} == {'org-0', 'org-1'}
    telemetry.shutdown(1)


def test_clean_attributes_for_declared_metrics(sink, monkeypatch):
    telemetry, _, _ = sink
    keys = {'outcome': 'rocketride.auth.outcome', 'org': 'rocketride.org.id'}
    assert telemetry.clean_attributes('m', keys, {'outcome': 'x' * 200, 'secret': 'no', 'org': 'org-a'}) == {
        'rocketride.auth.outcome': 'x' * _otel.MAX_VALUE_LENGTH,
        'rocketride.org.id': 'org-a',
    }
    monkeypatch.setattr(_otel, 'MAX_ATTRIBUTE_SETS', 2)
    monkeypatch.setattr(_runs, 'MAX_ATTRIBUTE_SETS', 2)
    telemetry.clean_attributes('n', keys, {'outcome': 'a'})
    telemetry.clean_attributes('n', keys, {'outcome': 'b'})
    assert telemetry.clean_attributes('n', keys, {'outcome': 'c'}) == {'rocketride.auth.outcome': '_other'}


# ── Health, resource, shutdown ────────────────────────────────────────────────


def test_hook_errors_are_counted_not_raised(sink):
    telemetry, _, reader = sink

    def boom(*_):
        raise RuntimeError('boom')

    telemetry.guard('task_event', boom)
    assert (
        value(points(reader), 'rocketride.telemetry.hook_errors', rocketride__telemetry__hook='task_event').value == 1
    )


def test_heartbeat_and_connections(sink):
    telemetry, _, reader = sink
    telemetry.observe_connections(lambda: 7)
    collected = points(reader)
    assert value(collected, 'rocketride.telemetry.up').value == 1
    assert value(collected, 'rocketride.ws.connection.count').value == 7
    telemetry.observe_connections(lambda: 1 / 0)  # a failing source is skipped, not raised
    collected = points(reader)  # must not raise
    assert value(collected, 'rocketride.telemetry.up').value == 1


def test_resource_defaults_and_environment(monkeypatch):
    monkeypatch.delenv('OTEL_SERVICE_NAME', raising=False)
    monkeypatch.setenv('OTEL_RESOURCE_ATTRIBUTES', 'deployment.environment.name=stg')
    resource = _otel._resource('1.2.3').attributes
    assert resource['service.name'] == 'rocketride-eaas'
    assert resource['service.version'] == '1.2.3'
    assert resource['deployment.environment.name'] == 'stg'
    # Unique per process (the SDK's own id when it sets one, else ours)
    assert resource['service.instance.id'] and resource['host.name']
    monkeypatch.setenv('OTEL_SERVICE_NAME', 'custom')
    assert _otel._resource('').attributes['service.name'] == 'custom'


def test_shutdown_exports_open_work(sink):
    telemetry, spans, _ = sink
    begin(telemetry)
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 100.0, {}, PIPELINE)
    telemetry.shutdown(1)
    pipelines = [s for s in spans.get_finished_spans() if s.name == 'pipeline']
    assert pipelines and pipelines[0].attributes['rocketride.span.unclosed'] is True


def test_pipeline_traces_are_built_off_the_caller_thread(sink):
    telemetry, spans, _ = sink
    threads = []
    original = telemetry._export_invocation
    telemetry._export_invocation = lambda *a: (threads.append(threading.current_thread().name), original(*a))
    begin(telemetry)
    call(telemetry, 'parse_1')
    assert [s for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert threads == ['otel-traces']


def test_a_wide_pipeline_costs_the_event_loop_little(sink, monkeypatch):
    # 50 components per object at ratio 1.0: the hooks only record and queue.
    # The export thread is held so this times the hooks alone (building the
    # spans costs CPU on that thread, measured by load tests, not here).
    telemetry, _, _ = sink
    release = threading.Event()
    original = telemetry._export_invocation
    monkeypatch.setattr(telemetry, '_export_invocation', lambda *a: (release.wait(30), original(*a)))
    pipeline = {'components': [{'id': f'c{n}', 'provider': f'node{n}'} for n in range(50)]}
    begin(telemetry)
    objects = 200
    started = time.perf_counter()
    for pipe in range(objects):
        for n in range(50):
            telemetry.component_trace('task-1', 'enter', pipe, f'c{n}', 100.0, {}, pipeline)
        for n in reversed(range(50)):
            telemetry.component_trace('task-1', 'leave', pipe, f'c{n}', 101.0, {}, pipeline)
    per_object = (time.perf_counter() - started) / objects
    release.set()
    # 100 hook calls per object; generous for a loaded CI machine (~1 ms idle)
    assert per_object < 0.02, per_object


def test_hooks_are_cheap(sink):
    telemetry, _, _ = sink
    begin(telemetry)
    started = time.perf_counter()
    for n in range(20000):
        call(telemetry, 'parse_1', pipe=n % 8)
    per_call = (time.perf_counter() - started) / 40000
    assert per_call < 0.001  # generous bound: a hook must stay far below a millisecond


# ── Distributed tracing from SDK callers ──────────────────────────────────────

PARENT_TRACE = '0af7651916cd43dd8448eb211c80319c'


def test_sampled_traceparent_makes_a_child_server_span(sink):
    telemetry, spans, _ = sink
    telemetry.request_done('execute', None, 0.05, f'00-{PARENT_TRACE}-b7ad6b7169203331-01')
    span = [s for s in finished(telemetry, spans) if s.name == 'execute'][0]  # semconv: {rpc.method}
    assert format(span.context.trace_id, '032x') == PARENT_TRACE
    assert format(span.parent.span_id, '016x') == 'b7ad6b7169203331'
    assert span.attributes['rpc.system.name'] == 'rocketride_dap'


@pytest.mark.parametrize(
    'traceparent',
    [
        None,
        f'00-{PARENT_TRACE}-b7ad6b7169203331-00',  # not sampled by the caller
        'garbage',
        f'00-{"0" * 32}-b7ad6b7169203331-01',  # invalid all-zero trace id
        {'not': 'a string'},
    ],
)
def test_no_span_without_a_sampled_valid_traceparent(sink, traceparent):
    telemetry, spans, reader = sink
    telemetry.request_done('execute', None, 0.05, traceparent)
    assert not [s for s in finished(telemetry, spans) if s.name == 'execute']
    assert value(points(reader), 'rpc.server.call.duration', rpc__method='execute').count == 1


def test_caller_sampled_requests_are_limited_per_org(sink):
    telemetry, spans, reader = sink
    sampled = f'00-{PARENT_TRACE}-b7ad6b7169203331-01'
    for _ in range(11):  # one org, within a minute: 10 kept for the caller's trace
        telemetry.request_done('execute', None, 0.05, sampled, identity={'org_id': 'org-a'})
    telemetry.request_done('execute', None, 0.05, sampled, identity={'org_id': 'org-b'})  # its own budget
    kept = [s for s in finished(telemetry, spans) if s.name == 'execute']
    assert len(kept) == 11
    assert all(s.attributes['rocketride.sampling.reason'] == 'ratio' for s in kept)
    assert [s.attributes.get('rocketride.org.id') for s in kept].count('org-b') == 1
    dropped = value(points(reader), 'rocketride.telemetry.dropped', rocketride__telemetry__reason='caller_rate_limited')
    assert dropped.value == 1
    assert format(kept[0].context.trace_id, '032x') == PARENT_TRACE  # still the caller's trace


# ── Stalled passes ────────────────────────────────────────────────────────────


def _open_call(telemetry, component='my_secret_llm_name', idle=0.0, task_id='task-1'):
    """A component call entered and not left, its last enter/leave `idle` seconds ago."""
    telemetry.component_trace(task_id, 'enter', 0, component, 100.0, {}, PIPELINE)
    with telemetry._lock:
        invocation = telemetry._runs[task_id].invocations[0]
        invocation.last_mono -= idle
    return invocation


def _idle(reader):
    return {
        (attrs.get('rocketride.component.type'), attrs.get('rocketride.org.id')): point.value
        for attrs, point in points(reader).get('rocketride.pass.idle', [])
    }


def test_a_pass_without_progress_shows_its_idle_time_by_type_and_org(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    _open_call(telemetry, idle=120)
    # The innermost open call names the type (never the user's component id)
    assert _idle(reader) == {('llm_openai', 'org-a'): 120}


def test_any_enter_or_leave_is_progress(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    _open_call(telemetry, idle=120)
    call(telemetry, 'parse_1')  # a nested call: an agent's tool keeps the pass alive
    assert _idle(reader)[('llm_openai', 'org-a')] < 5


def test_a_finished_pass_reports_zero_and_its_series_stays(sink):
    telemetry, _, reader = sink
    begin(telemetry)
    _open_call(telemetry, idle=120)
    assert _idle(reader) == {('llm_openai', 'org-a'): 120}
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 101.0, {}, PIPELINE)
    assert _idle(reader) == {('llm_openai', 'org-a'): 0}


def test_debugged_runs_and_exempt_types_are_not_stalled(monkeypatch):
    telemetry, _, reader = make(stall_exempt_types='llm_*')
    begin(telemetry)
    _open_call(telemetry, idle=3600)
    assert _idle(reader) == {}  # a node type that waits for a person (or anything fnmatch-ed)
    telemetry.shutdown(1)
    telemetry, _, reader = make()
    begin(telemetry)
    telemetry.task_event('task-1', {'event': 'apaevt_status_update', 'body': {'debuggerAttached': True}}, identity)
    _open_call(telemetry, idle=3600)
    assert _idle(reader) == {}  # paused at a breakpoint on purpose
    telemetry.shutdown(1)


def test_a_stalled_pass_writes_one_error_line(capsys):
    telemetry, _, reader = make(stall_log_seconds=600)
    begin(telemetry)
    _open_call(telemetry, idle=700)
    _idle(reader)
    _idle(reader)  # collected twice: one line
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if 'rocketride.pass.stalled' in line]
    assert len(lines) == 1
    assert lines[0]['level'] == 'error' and lines[0]['task_id'] == 'task-1'
    assert lines[0]['component_type'] == 'llm_openai' and lines[0]['idle_s'] == 700
    assert lines[0]['org_id'] == 'org-a' and len(lines[0]['trace_id']) == 32
    # Progress, then a second stall: logged again
    telemetry.component_trace('task-1', 'enter', 0, 'parse_1', 101.0, {}, PIPELINE)
    with telemetry._lock:
        telemetry._runs['task-1'].invocations[0].last_mono -= 700
    _idle(reader)
    assert sum('rocketride.pass.stalled' in line for line in capsys.readouterr().out.splitlines()) == 1
    telemetry.shutdown(1)


def test_a_series_idle_no_more_is_dropped_later(sink, monkeypatch):
    telemetry, _, reader = sink
    begin(telemetry)
    _open_call(telemetry, idle=120)
    _idle(reader)
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 101.0, {}, PIPELINE)
    assert _idle(reader) == {('llm_openai', 'org-a'): 0}  # 0 while recent
    monkeypatch.setattr(_runs, 'IDLE_KEY_SECONDS', -1.0)
    assert _idle(reader) == {}  # then gone: no series kept forever


def test_info_gauge_only_attributed_orgs_never_capped(monkeypatch):
    """An info gauge names only orgs already attributed, takes no cap slot and keeps no lifetime set."""
    from ai.common import telemetry as facade

    telemetry, _, reader = make(org_cardinality=2)
    monkeypatch.setattr(facade, '_impl', telemetry)
    rows = []
    info = facade.InfoGauge(
        'rocketride.test.info',
        '',
        'test',
        observe=lambda: list(rows),
        attributes={'org': 'rocketride.org.id', 'name': 'rocketride.org.name'},
        limit=2,
    )
    info._bind(telemetry)
    telemetry.task_ended('t0', {'org_id': 'org-0', 'run_kind': 'dev'}, 'ok', None, None)
    assert facade.attributed_orgs() == frozenset({'org-0'})

    # An org that never ran here is left out, and does not take a cap slot
    rows[:] = [(1, {'org': 'org-0', 'name': 'Acme'}), (1, {'org': 'org-9', 'name': 'Never ran'})]
    collected = points(reader)
    names = {dict(p.attributes)['rocketride.org.name'] for _, p in collected['rocketride.test.info']}
    assert names == {'Acme'}
    assert facade.attributed_orgs() == frozenset({'org-0'})

    # Renames never exhaust anything: thousands of names over time, one org
    for n in range(2500):
        rows[:] = [(1, {'org': 'org-0', 'name': f'Acme {n}'})]
        assert info._observations() == [(1, {'rocketride.org.id': 'org-0', 'rocketride.org.name': f'Acme {n}'})]

    # Past `limit` in one collection: left out and counted
    telemetry.task_ended('t1', {'org_id': 'org-1', 'run_kind': 'dev'}, 'ok', None, None)
    rows[:] = [
        (1, {'org': 'org-0', 'name': 'A'}),
        (1, {'org': 'org-1', 'name': 'B'}),
        (1, {'org': 'org-0', 'name': 'C'}),
    ]
    assert len(info._observations()) == 2
    collected = points(reader)
    assert value(collected, 'rocketride.telemetry.dropped', rocketride__telemetry__reason='info_overflow').value >= 1
    telemetry.shutdown(1)


def test_attributed_orgs_empty_when_the_org_id_is_off(monkeypatch):
    from ai.common import telemetry as facade

    telemetry, _, _ = make(org_attribute=False)
    monkeypatch.setattr(facade, '_impl', telemetry)
    telemetry.task_ended('t0', {'org_id': 'org-0', 'run_kind': 'dev'}, 'ok', None, None)
    assert facade.attributed_orgs() == frozenset()
    assert telemetry.clean_info_attributes({'org': 'rocketride.org.id'}, {'org': 'org-0'}) is None
    telemetry.shutdown(1)


def test_a_pass_named_by_a_stall_line_is_kept_after_it_resumes(capsys):
    telemetry, spans, reader = make(stall_log_seconds=600, sample_ratio=0.0, slow_seconds=0)
    begin(telemetry)
    invocation = _open_call(telemetry, idle=700)
    _idle(reader)  # writes the stall line
    (line,) = [json.loads(x) for x in capsys.readouterr().out.splitlines() if 'rocketride.pass.stalled' in x]
    telemetry.component_trace('task-1', 'leave', 0, 'my_secret_llm_name', 101.0, {}, PIPELINE)  # resumes, ends fine
    assert not invocation.stall_logged and invocation.stall_noted
    (pipeline,) = [s for s in finished(telemetry, spans) if s.name == 'pipeline']
    assert f'{pipeline.context.trace_id:032x}' == line['trace_id']
    assert pipeline.attributes['rocketride.sampling.reason'] == 'stalled'


def test_a_full_trace_queue_does_not_keep_the_worker_from_stopping(monkeypatch):
    monkeypatch.setattr(_otel, 'TRACE_QUEUE', 2)
    telemetry, _, _ = make()
    release = threading.Event()
    original = telemetry._export_invocation
    monkeypatch.setattr(telemetry, '_export_invocation', lambda *a: (release.wait(5), original(*a)))
    try:
        begin(telemetry)
        for pipe in range(4):
            call(telemetry, 'parse_1', pipe=pipe)
        assert telemetry._traces.full()
        telemetry.shutdown(0.05)  # the queue is still full when the stop signal goes in
    finally:
        release.set()
    telemetry._trace_thread.join(5)
    assert not telemetry._trace_thread.is_alive()
