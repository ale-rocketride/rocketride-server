"""
Unit tests for the relay of a run's dependency calls: the subprocess writes each
call with its (pipe, component) (ai.common.telemetry.relay), and EaaS attaches it
to the pass and component span that made it (Telemetry.dependency_relayed).
"""

import json
import time

import pytest

pytest.importorskip('opentelemetry.sdk')

from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402

from ai.common.telemetry import _deps, relay  # noqa: E402
from ai.common.telemetry._otel import Telemetry  # noqa: E402

PIPELINE = {'components': [{'id': 'my_llm', 'provider': 'llm_openai'}, {'id': 'parse_1', 'provider': 'parse'}]}


def identity():
    return {'org_id': 'org-a', 'run_kind': 'dev'}


# ── The subprocess side ──────────────────────────────────────────────────────


def test_flags_come_from_the_argument_eaas_adds():
    assert relay.flags_from_argv(['node.py', 'task.json', '--otel_relay=http,model']) == ['http', 'model']
    assert relay.flags_from_argv(['node.py', 'task.json']) == []
    assert relay.flags_from_argv(['--otel_relay=none']) == []


@pytest.fixture
def written(monkeypatch):
    lines = []
    monkeypatch.setattr(relay, '_write', lines.append)
    monkeypatch.setattr(relay, '_pending', [])
    monkeypatch.setattr(relay, '_overflow', 0)
    return lines


def records(lines):
    """Every call in the written lines (each line is a JSON array, a batch)."""
    return [record for line in lines for record in json.loads(line)]


def _node_classes():
    class Base:
        instance = None

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)

        def writeText(self, text):
            pass

        def open(self, obj):
            pass

    relay._wrap_subclasses(Base)

    class Instance:
        pipeId = 3
        pipeType = {'id': 'my_llm', 'logicalType': 'llm_openai'}

    class Node(Base):
        def __init__(self, calls):
            self.instance = Instance()
            self.calls = calls

        def writeText(self, text):
            now = time.time_ns()
            for _ in range(self.calls):
                relay.emit(
                    _deps.Call(
                        'http.client.request.duration',
                        'POST',
                        now,
                        now + 5,
                        {'http.request.method': 'POST', 'http.request.header.authorization': 'x'},
                        None,
                    )
                )

    return Node


def test_a_call_carries_the_pipe_and_component_of_the_handler_that_made_it(written):
    _node_classes()(1).writeText('hi')  # written when the handler returns
    (record,) = records(written)
    assert (record['p'], record['c'], record['n'], record['m']) == (3, 'my_llm', 'POST', 'http.client.request.duration')
    assert record['a'] == {'http.request.method': 'POST'}  # only the bounded attributes, never a header


def test_a_call_outside_any_handler_has_no_component(written):
    now = time.time_ns()
    relay.emit(
        _deps.Call('db.client.operation.duration', 'GET', now, now, {'db.system.name': 'redis'}, 'ConnectionError')
    )
    assert written == []  # off a handler: written by the flush thread, or with the next batch
    relay.flush()
    (record,) = records(written)
    assert 'p' not in record and record['x'] == 'ConnectionError'


def test_calls_past_the_cap_are_measured_without_a_place_in_the_trace(written, monkeypatch):
    monkeypatch.setattr(relay, 'MAX_CALLS_PER_SCOPE', 3)
    _node_classes()(5).writeText('hi')
    sent = records(written)
    assert [('p' in r, r.get('o')) for r in sent] == [(True, None)] * 3 + [(False, 1)] * 2


def test_calls_are_written_in_batches_and_a_full_queue_is_counted(written, monkeypatch):
    monkeypatch.setattr(relay, 'BATCH', 4)
    monkeypatch.setattr(relay, 'MAX_PENDING', 6)
    now = time.time_ns()
    call = _deps.Call('db.client.operation.duration', 'GET', now, now, {'db.system.name': 'redis'}, None)
    for _ in range(3):
        relay.emit(call)
    assert written == []
    relay.emit(call)  # the fourth fills a batch
    assert len(written) == 1 and len(json.loads(written[0])) == 4
    flush = relay.flush
    monkeypatch.setattr(relay, 'flush', lambda: None)  # a stuck writer: the queue fills
    for _ in range(9):
        relay.emit(call)
    flush()
    assert {'dropped': 3, 'reason': 'relay_overflow'} in records(written)


def test_relay_spans_turn_library_spans_into_calls(written):
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    provider.add_span_processor(relay.relay_spans())
    tracer = provider.get_tracer('opentelemetry.instrumentation.httpx')
    with tracer.start_as_current_span(
        'POST',
        kind=SpanKind.CLIENT,
        attributes={
            'http.request.method': 'POST',
            'url.full': 'https://api.openai.com/v1/chat?key=sk-123',
            'server.address': 'api.openai.com',
            'http.response.status_code': 429,
            'error.type': '429',
        },
    ):
        pass
    relay.flush()
    (record,) = records(written)
    assert 'url.full' not in record['a'] and record['a']['url.scheme'] == 'https'  # never the URL
    assert record['x'] == '429' and record['a']['http.response.status_code'] == 429


def test_a_library_span_named_after_its_database_is_named_from_its_attributes(written):
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    provider.add_span_processor(relay.relay_spans())
    tracer = provider.get_tracer('opentelemetry.instrumentation.psycopg')
    with tracer.start_as_current_span(
        'SELECT customers_db',
        kind=SpanKind.CLIENT,
        attributes={
            'db.system.name': 'postgresql',
            'db.operation.name': 'SELECT',
            'db.namespace': 'customers_db',
            'server.address': 'db.internal',
        },
    ):
        pass
    relay.flush()
    (record,) = records(written)
    assert record['n'] == 'SELECT postgresql'
    assert 'customers_db' not in json.dumps(record)


# ── The EaaS side ────────────────────────────────────────────────────────────


@pytest.fixture
def sink():
    spans = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    telemetry = Telemetry(
        span_exporter=spans, metric_reader=reader, sample_ratio=1.0, service_version='9.9.9', org_attribute=True
    )
    yield telemetry, spans, reader
    telemetry.shutdown(1)


def _begin(telemetry, task_id='task-1'):
    telemetry.task_event(task_id, {'event': 'apaevt_status_update', 'body': {'state': 1}}, identity)
    telemetry.task_event(task_id, {'event': 'apaevt_task', 'body': {'action': 'begin'}}, identity)
    telemetry.task_event(task_id, {'event': 'apaevt_status_update', 'body': {'state': 3}}, identity)


def _call(name, start, end, pipe=3, component='my_llm', error=None):
    body = {
        'm': 'http.client.request.duration',
        'n': name,
        's': int(start * 1e9),
        'e': int(end * 1e9),
        'a': {'http.request.method': 'POST', 'server.address': 'api.openai.com', 'http.response.status_code': 200},
        'p': pipe,
        'c': component,
    }
    if error:
        body['x'] = error
    return body


def _dropped(reader):
    data = reader.get_metrics_data()
    out = {}
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == 'rocketride.telemetry.dropped':
                    for p in metric.data.data_points:
                        out[p.attributes['rocketride.telemetry.reason']] = p.value
    return out


def test_a_relayed_call_is_a_client_span_under_its_component(sink):
    telemetry, spans, reader = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'parse_1', t, {}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'parse_1', t + 0.1, {}, PIPELINE)
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t + 0.2, {}, PIPELINE)
    telemetry.dependency_relayed('task-1', {'calls': [_call('POST', t + 0.25, t + 0.9, error='429')]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 1.0, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()

    by_name = {s.name: s for s in spans.get_finished_spans()}
    client, llm = by_name['POST'], by_name['component llm_openai']
    assert client.kind == SpanKind.CLIENT and client.parent.span_id == llm.context.span_id
    assert client.status.status_code == StatusCode.ERROR and client.attributes['error.type'] == '429'
    assert client.attributes['rocketride.component.type'] == 'llm_openai'
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    names = {m.name for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics}
    assert 'http.client.request.duration' in names


def test_relayed_calls_outside_a_pass_are_measured_and_counted(sink):
    telemetry, spans, reader = sink
    _begin(telemetry)
    t = time.time()
    batch = [
        _call('POST', t, t + 0.1),  # no pass open on pipe 3
        dict(_call('POST', t, t + 0.1), p=None),  # off the handler
        dict(_call('POST', t, t + 0.1), p=None, c=None, o=1),  # past the cap for its node call
        {'dropped': 7, 'reason': 'relay_overflow'},
    ]
    telemetry.dependency_relayed('task-1', {'calls': batch}, PIPELINE)
    dropped = _dropped(reader)
    assert [dropped[r] for r in ('relay_late', 'relay_unattributed', 'relay_scope_cap', 'relay_overflow')] == [
        1,
        1,
        1,
        7,
    ]
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    counts = [
        p.count
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'http.client.request.duration'
        for p in m.data.data_points
    ]
    assert sum(counts) == 3  # every call is measured, placed or not


def test_a_relayed_dropped_count_is_bounded(sink):
    telemetry, _, reader = sink
    batch = [
        {'dropped': 10**12, 'reason': 'relay_overflow'},
        {'dropped': -5, 'reason': 'relay_overflow'},
        {'dropped': True, 'reason': 'relay_overflow'},
        {'dropped': '7', 'reason': 'relay_overflow'},
    ]
    telemetry.dependency_relayed('task-1', {'calls': batch}, PIPELINE)
    from ai.common.telemetry import _otel

    assert _dropped(reader)['relay_overflow'] == _otel.MAX_RELAYED_DROPPED


def _mark(when, **changes):
    record = {'k': 'err', 'x': 'provider_auth', 'g': 'refused', 'w': int(when * 1e9), 'p': 3, 'c': 'my_llm'}
    record.update(changes)
    return {key: value for key, value in record.items() if value is not None}


@pytest.mark.parametrize(
    'changes',
    [
        {'extra': 'x'},  # a key the relay never writes
        {'w': None},  # a key it always writes, missing
        {'p': '3'},  # a pipe that is not a number
        {'x': {'nested': 1}},  # a type that is not text
        {'k': 'gen'},  # another kind's keys
        {'k': 'exc'},
        {'k': 'other'},
    ],
)
def test_a_relayed_record_not_shaped_like_the_relays_is_ignored(sink, changes):
    telemetry, spans, _ = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('task-1', {'calls': [_mark(t + 0.1, **changes)]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    (llm,) = [s for s in spans.get_finished_spans() if s.name == 'component llm_openai'] or [None]
    assert llm is None or llm.status.status_code != StatusCode.ERROR


def test_a_relayed_record_shaped_like_the_relays_is_taken(sink):
    telemetry, spans, _ = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('task-1', {'calls': [_mark(t + 0.1)]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    (llm,) = [s for s in spans.get_finished_spans() if s.name == 'component llm_openai']
    assert llm.status.status_code == StatusCode.ERROR


def test_one_relayed_record_that_cannot_be_read_does_not_cost_the_batch(sink):
    telemetry, _, reader = sink
    _begin(telemetry)
    t = time.time()
    batch = [
        dict(_call('POST', t, t + 0.1), s='not a number'),  # raises when read
        _call('POST', t, t + 0.1),
        {'dropped': 2, 'reason': 'relay_overflow'},
    ]
    telemetry.dependency_relayed('task-1', {'calls': batch}, PIPELINE)
    assert _dropped(reader)['relay_overflow'] == 2
    telemetry.flush_measurements()
    (measured,) = _points_of(reader, 'http.client.request.duration')
    assert measured.count == 1
    (errors,) = [p for p in _points_of(reader, 'rocketride.telemetry.hook_errors') if p.value]
    assert dict(errors.attributes)['rocketride.telemetry.hook'] == 'relay_record' and errors.value == 1


def test_a_relayed_sql_call_names_no_database(sink, written):
    from types import SimpleNamespace

    class Engine:  # a named database on a server (SQLAlchemy's engine, as the recorder reads it)
        url = SimpleNamespace(database='customers_db', host='db.internal', port=5432)
        dialect = SimpleNamespace(name='postgresql')

    calls = []
    engine = Engine()
    _deps.SqlRecorder(calls.append)._record(engine, 'SELECT * FROM orders', time.time_ns() - 1000, None)
    (call,) = calls
    assert call.name == 'SELECT postgresql' and 'db.namespace' not in call.attributes

    telemetry, spans, reader = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    call.start_ns, call.end_ns = int((t + 0.1) * 1e9), int((t + 0.2) * 1e9)
    relay.emit(call, relay._Scope(3, 'my_llm'))
    relay.flush()
    telemetry.dependency_relayed('task-1', {'calls': records(written)}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    (span,) = [s for s in spans.get_finished_spans() if s.kind == SpanKind.CLIENT]
    assert span.name == 'SELECT postgresql'
    assert 'customers_db' not in json.dumps(dict(span.attributes))
    telemetry.flush_measurements()
    (measured,) = _points_of(reader, 'db.client.operation.duration')
    assert 'customers_db' not in json.dumps(dict(measured.attributes))
    assert dict(measured.attributes)['db.system.name'] == 'postgresql'


def test_relayed_attributes_are_only_the_known_ones(sink):
    telemetry, spans, _ = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    call = _call('POST', t, t + 0.01, error='500')
    call['a'] = dict(call['a'], **{'http.request.header.authorization': 'Bearer x', 'server.address': 'h' * 500})
    telemetry.dependency_relayed('task-1', {'calls': [call]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.1, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    client = next(s for s in spans.get_finished_spans() if s.kind == SpanKind.CLIENT)
    assert 'http.request.header.authorization' not in client.attributes
    assert len(client.attributes['server.address']) == 64


def test_the_relay_and_eaas_agree_on_the_attributes():
    from ai.common.telemetry import _otel

    assert frozenset(relay._KEEP) == _otel._RELAY_ATTRIBUTES


def test_relayed_calls_count_against_the_buffer_and_are_released(sink):
    telemetry, _, _ = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    for _ in range(3):
        telemetry.dependency_relayed('task-1', {'calls': [_call('POST', t, t + 0.01)]}, PIPELINE)
    assert telemetry._buffered == 3
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.1, {}, PIPELINE)
    assert telemetry._buffered == 0


def test_the_relay_flags_match_the_instrumented_ones():
    assert relay.FLAGS == _deps.FLAGS


def test_a_failed_dependency_call_keeps_a_pass_that_went_on():
    spans = InMemorySpanExporter()
    telemetry = Telemetry(span_exporter=spans, metric_reader=InMemoryMetricReader(), sample_ratio=0.0)
    try:
        _begin(telemetry)
        t = time.time()
        for pipe, error in ((1, '401'), (2, None)):
            telemetry.component_trace('task-1', 'enter', pipe, 'my_llm', t, {}, PIPELINE)
            telemetry.dependency_relayed(
                'task-1', {'calls': [_call('POST', t + 0.01, t + 0.02, pipe=pipe, error=error)]}, PIPELINE
            )
            telemetry.component_trace('task-1', 'leave', pipe, 'my_llm', t + 0.1, {}, PIPELINE)
        assert telemetry.flush_traces(5)
        telemetry._tracer_provider.force_flush()
        roots = [s for s in spans.get_finished_spans() if s.name == 'pipeline']
        assert [r.attributes['rocketride.sampling.reason'] for r in roots] == ['dependency_error']
    finally:
        telemetry.shutdown(1)


def test_the_component_id_comes_from_the_engines_pipe_type_object():
    class IPipeType:  # engLib.IPipeType: attributes, not a dict
        id = 'my_llm'

    assert relay._component_id(IPipeType()) == 'my_llm'
    assert relay._component_id({'id': 'parse_1'}) == 'parse_1'
    assert relay._component_id(None) is None


def test_the_engines_pass_trace_id_names_the_pass(sink):
    telemetry, spans, _ = sink
    _begin(telemetry)
    t = time.time()
    tid = '4bf92f3577b34da6a3ce929d0e0e4736'
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {'tid': tid}, PIPELINE)
    model = _call('model rrext_ms_inference', t + 0.01, t + 0.02, error='Exception')
    model.update(
        m='rpc.client.call.duration',
        i='00f067aa0ba902b7',
        a={'rpc.system.name': 'rocketride_dap', 'rpc.method': 'rrext_ms_inference'},
    )
    telemetry.dependency_relayed('task-1', {'calls': [model]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.1, {'tid': tid}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    by_name = {s.name: s for s in spans.get_finished_spans()}
    assert format(by_name['pipeline'].context.trace_id, '032x') == tid
    client = by_name['model rrext_ms_inference']
    # The span id its traceparent carried: the model server's span is its child
    assert format(client.context.span_id, '016x') == '00f067aa0ba902b7'
    assert client.context.trace_id == by_name['pipeline'].context.trace_id


def test_a_model_request_carries_the_pass_in_its_traceparent():
    import asyncio

    from rocketride.core.dap_base import current_traceparent

    async def build_request():
        return current_traceparent()

    traceparent = asyncio.run(
        _deps._in_trace(build_request(), int('4bf92f3577b34da6a3ce929d0e0e4736', 16), 0x00F067AA0BA902B7)
    )
    assert traceparent == '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'


def test_the_pass_trace_id_comes_from_the_engine_instance():
    class Instance:
        passTraceId = '4bf92f3577b34da6a3ce929d0e0e4736'

    assert relay._pass_trace_id(Instance()) == int('4bf92f3577b34da6a3ce929d0e0e4736', 16)
    assert relay._pass_trace_id(object()) == 0  # an engine without it


def test_a_call_with_a_traceparent_sends_its_span_id(written):
    now = time.time_ns()
    relay.emit(_deps.Call('rpc.client.call.duration', 'model x', now, now, {}, None, span_id=0x00F067AA0BA902B7))
    relay.flush()
    (record,) = records(written)
    assert record['i'] == '00f067aa0ba902b7'


def _passes(telemetry, spans, enters):
    """Run one pass per list of enter traces on pipe 3; the pipeline spans' trace ids (hex)."""
    t = time.time()
    for traces in enters:
        for trace_body in traces:
            telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, trace_body, PIPELINE)
        for _ in traces:
            telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.1, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    return [format(s.context.trace_id, '032x') for s in spans.get_finished_spans() if s.name == 'pipeline']


def test_a_repeated_pass_trace_id_is_not_trusted(sink):
    telemetry, spans, reader = sink
    _begin(telemetry)
    tid = '4bf92f3577b34da6a3ce929d0e0e4736'
    # The engine sends the id on every enter: the second pass has two
    first, second = _passes(telemetry, spans, [[{'tid': tid}], [{'tid': tid}, {'tid': tid}]])
    assert first == tid and second != tid  # two passes never share a trace
    assert _dropped(reader)['pass_tid_reused'] == 1  # once for the pass, not for each enter


def test_a_pass_whose_leave_an_exception_skipped_ends_at_the_next_pass(sink):
    telemetry, spans, reader = sink
    _begin(telemetry)
    first, second = '4bf92f3577b34da6a3ce929d0e0e4736', '0af7651916cd43dd8448eb211c80319c'
    t = time.time()
    # The first pass's leave never comes (the engine ended its level without an event)
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {'tid': first}, PIPELINE)
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t + 1, {'tid': second}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 1.1, {'tid': second}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    passes = [s for s in spans.get_finished_spans() if s.name == 'pipeline']
    assert sorted(format(s.context.trace_id, '032x') for s in passes) == sorted([first, second])
    assert _dropped(reader)['pass_unwound'] == 1


def test_a_pass_begun_without_a_trace_id_takes_the_first_it_sees(sink):
    telemetry, spans, _ = sink
    _begin(telemetry)
    tid = '0af7651916cd43dd8448eb211c80319c'
    # A tool call's enter (no tid), then the engine's enter inside it
    (trace_id,) = _passes(telemetry, spans, [[{}, {'tid': tid}]])
    assert trace_id == tid


# ── Calls from a node's threads, nested client spans, the pass of a call ─────


def test_work_a_node_hands_to_an_executor_keeps_its_component_and_pass(written):
    threading_instrumentation = pytest.importorskip('opentelemetry.instrumentation.threading')
    from concurrent.futures import ThreadPoolExecutor

    def call():
        now = time.time_ns()
        relay.emit(_deps.Call('http.client.request.duration', 'POST', now, now + 5, {}, None))

    class Base:
        instance = None

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)

        def writeText(self, text):
            pass

    relay._wrap_subclasses(Base)

    class Instance:
        pipeId = 3
        pipeType = {'id': 'my_llm'}
        passTraceId = '4bf92f3577b34da6a3ce929d0e0e4736'

    class Node(Base):
        def __init__(self, pool):
            self.instance = Instance()
            self.pool = pool

        def writeText(self, text):
            self.pool.submit(call).result()  # the node's work, on a pooled thread

    instrumentor = threading_instrumentation.ThreadingInstrumentor()
    instrumentor.instrument()
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        Node(pool).writeText('x')
        pool.submit(call).result()  # the same pooled thread, after the handler: no pass
        relay.flush()
        first, later = records(written)
        assert (first['p'], first['c'], first['t']) == (3, 'my_llm', '4bf92f3577b34da6a3ce929d0e0e4736')
        assert 'p' not in later and 't' not in later
    finally:
        pool.shutdown()
        instrumentor.uninstrument()


def test_a_client_span_inside_another_is_not_a_second_call(written):
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    provider.add_span_processor(relay.relay_spans())
    tracer = provider.get_tracer('test')
    # Elasticsearch's own span, with urllib3's under it: one call, a database one
    with tracer.start_as_current_span(
        'search', kind=SpanKind.CLIENT, attributes={'db.system': 'elasticsearch', 'server.address': 'es.local'}
    ):
        with tracer.start_as_current_span(
            'POST', kind=SpanKind.CLIENT, attributes={'http.request.method': 'POST', 'server.address': 'es.local'}
        ):
            pass
    relay.flush()
    (record,) = records(written)
    assert record['m'] == 'db.client.operation.duration' and record['a']['db.system.name'] == 'elasticsearch'


def test_a_relayed_call_of_another_pass_is_not_placed_in_the_open_one(sink):
    telemetry, spans, reader = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {'tid': '4bf92f3577b34da6a3ce929d0e0e4736'}, PIPELINE)
    late = _call('POST', t + 0.01, t + 0.02)
    late['t'] = '0af7651916cd43dd8448eb211c80319c'  # made for an earlier pass, finishing now
    telemetry.dependency_relayed('task-1', {'calls': [late]}, PIPELINE)
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.1, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    assert 'POST' not in {s.name for s in spans.get_finished_spans()}
    assert _dropped(reader)['relay_late'] == 1


def test_a_customer_cannot_take_every_pipeline_server(sink, monkeypatch):
    telemetry, _, reader = sink
    from ai.common.telemetry import _otel

    monkeypatch.setattr(_otel, 'MAX_SERVERS_PER_ORG', 2)
    for org, server in (('org-a', 'a1'), ('org-a', 'a2'), ('org-a', 'a3'), ('org-b', 'b1')):
        attributes = {'server.address': server, 'rocketride.dependency.origin': 'pipeline', '_org': org}
        telemetry._measure_dependency('http.client.request.duration', 0.01, attributes)
    data = reader.get_metrics_data()
    servers = {
        dict(p.attributes).get('server.address')
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'http.client.request.duration'
        for p in m.data.data_points
    }
    assert servers == {'a1', 'a2', '_other', 'b1'}  # org-a's third is '_other'; org-b still has room
    assert _dropped(reader)['server_overflow'] == 1


def test_a_driver_a_node_installs_with_depends_is_recorded_right_after(monkeypatch):
    import sys
    import types

    installed = []
    module = types.ModuleType('depends')
    module.after_install_hooks = []

    def depends(requirements=None):  # as depends.depends: the hooks run after the install
        installed.append(requirements)
        for hook in module.after_install_hooks:
            hook(requirements)

    module.depends = depends
    monkeypatch.setitem(sys.modules, 'depends', module)
    recorded = []
    monkeypatch.setattr(_deps, 'instrument_new', lambda: recorded.append(list(installed)) or [])
    relay._watch_depends()
    relay._watch_depends()  # once
    assert module.after_install_hooks == [relay._depends_installed]
    assert module.depends is depends  # not replaced

    depends('nodes/store_qdrant/requirements.txt')
    assert installed == ['nodes/store_qdrant/requirements.txt']
    assert recorded == [['nodes/store_qdrant/requirements.txt']]  # after the install, once


def test_node_classes_defined_before_install_are_named_in_one_warning(monkeypatch):
    import rocketlib

    class Base:
        def writeText(self, text):
            pass

    class Early(Base):
        def writeText(self, text):
            pass

    class EarlyChild(Early):  # found at any depth
        def writeText(self, text):
            pass

    class Plain(Base):  # no handler of its own: nothing to wrap
        pass

    class GlobalBase:
        def beginGlobal(self):
            pass

    class EarlyGlobal(GlobalBase):
        def beginGlobal(self):
            pass

    class Others(GlobalBase):  # not a global handler
        def helper(self):
            pass

    warnings = []
    monkeypatch.setattr(rocketlib, 'warning', warnings.append)
    monkeypatch.setattr(relay, 'untraced_classes', 0)
    count = relay._warn_untraced([(Base, None), (GlobalBase, relay.GLOBAL_HANDLERS)])
    assert count == 3 and relay.untraced_classes == 3
    (line,) = warnings
    assert line.startswith('telemetry: 3 node classes defined before telemetry started are not traced: ')
    for cls in (Early, EarlyChild, EarlyGlobal):
        assert cls.__qualname__ in line
    assert 'Plain' not in line and 'Others' not in line

    # Classes defined after the wrapping are traced, not warned about
    relay._wrap_subclasses(Base)

    class Late(Base):
        def writeText(self, text):
            pass

    warnings.clear()
    relay._warn_untraced([(Base, None)])
    assert 'Late' not in warnings[0] and 'Early' in warnings[0]


def test_the_untraced_warning_names_at_most_ten_classes(monkeypatch):
    import rocketlib

    class Base:
        def writeText(self, text):
            pass

    classes = [type(f'Node{i}', (Base,), {'writeText': lambda self, text: None}) for i in range(12)]
    warnings = []
    monkeypatch.setattr(rocketlib, 'warning', warnings.append)
    assert relay._warn_untraced([(Base, None)]) == 12
    assert warnings[0].count('Node') == relay.MAX_UNTRACED_NAMES
    assert warnings[0].endswith('(+2 more)')
    assert len(classes) == 12


def test_a_failed_relayed_call_is_counted_by_node_server_error_and_customer(sink):
    """A provider refusing a node's call (a customer's rejected key) is counted even when the node goes on."""
    telemetry, _, reader = sink
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'task-1',
        {'calls': [_call('POST', t + 0.1, t + 0.2, error='401'), _call('POST', t + 0.3, t + 0.4)]},
        PIPELINE,
    )
    telemetry.component_trace('task-1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    points = [
        (dict(p.attributes), p.value)
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.component.dependency_errors'
        for p in m.data.data_points
    ]
    # Only the failure, with the node type, the server, the error and the customer
    assert points == [
        (
            {
                'rocketride.component.type': 'llm_openai',
                'error.type': '401',
                'server.address': 'api.openai.com',
                'rocketride.org.id': 'org-a',
            },
            1,
        )
    ]


def test_a_node_reporting_its_call_failed_marks_its_span_and_the_pass(sink):
    """An LLM node answering with the provider's error: its span and the pass are red, with why."""
    telemetry, spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [
                _call('POST', t + 0.1, t + 0.2, error='401'),
                _call('POST', t + 0.3, t + 0.4, error='401'),
                {
                    'k': 'err',
                    'x': 'provider_auth',
                    'g': 'AuthenticationError: Incorrect API key provided: sk-****abcd',
                    'w': int((t + 0.45) * 1e9),
                    'p': 3,
                    'c': 'my_llm',
                },
            ]
        },
        PIPELINE,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.5, {'result': 'continue'}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()

    by_name = {}
    for s in spans.get_finished_spans():
        by_name.setdefault(s.name, []).append(s)
    (root,), (llm,) = by_name['pipeline'], by_name['component llm_openai']
    assert root.status.status_code == StatusCode.ERROR and root.status.description == 'llm_openai: provider_auth'
    assert root.attributes['error.type'] == 'provider_auth'
    assert root.attributes['rocketride.pipeline.id'] == 'tok' and root.attributes['rocketride.source'] == 'chat_1'
    assert llm.status.status_code == StatusCode.ERROR and llm.attributes['error.type'] == 'provider_auth'
    assert llm.attributes['rocketride.component.id'] == 'my_llm'
    (event,) = llm.events
    assert event.name == 'exception' and event.attributes['exception.type'] == 'provider_auth'
    assert 'sk-****abcd' in event.attributes['exception.message']
    # Without the node's model call record nothing is inferred: no resend count
    assert all('http.request.resend_count' not in s.attributes for s in by_name['POST'])
    # Metrics unchanged: the node's result is still 'continue', no node error counted
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    errors = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.component.errors'
        for p in m.data.data_points
        if p.value
    ]
    assert errors == []


def test_a_node_error_off_a_node_call_is_not_relayed(written):
    relay.node_error('provider_auth', 'x')  # no scope: nothing written
    relay.flush()
    assert written == []


def test_a_streamed_call_ending_after_its_handler_keeps_its_component(written, monkeypatch):
    """A client span started in a node call and ended outside it is still that node's call."""
    pytest.importorskip('opentelemetry.sdk')
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import SpanKind as Kind

    provider = TracerProvider()
    provider.add_span_processor(relay.relay_spans())
    tracer = provider.get_tracer('t')
    context = relay._context()
    scope = relay._Scope(3, 'my_llm', 0xAB)
    token = context.attach(context.set_value(relay._SCOPE_KEY, scope))
    span = tracer.start_span(
        'POST', kind=Kind.CLIENT, attributes={'http.request.method': 'POST', 'server.address': 'api.openai.com'}
    )
    context.detach(token)  # the handler returned: the stream is read later
    span.end()
    relay.flush()
    (record,) = records(written)
    assert (record['p'], record['c'], record['t']) == (3, 'my_llm', f'{0xAB:032x}')


def test_a_global_handler_call_carries_the_node_type(written):
    """A node's key check at start (validateConfig) is measured with its node type, in no pass."""

    class GlobalBase:
        def beginGlobal(self):
            pass

    relay._wrap_subclasses(GlobalBase, relay._scoped_global, relay.GLOBAL_HANDLERS)

    class Glb:
        logicalType = 'llm_openai'

    class Global(GlobalBase):
        glb = Glb()

        def validateConfig(self, syntax_only):
            now = time.time_ns()
            relay.emit(_deps.Call('http.client.request.duration', 'POST', now, now + 5, {}, '401'))

    Global().validateConfig(False)
    (record,) = records(written)
    assert record['ct'] == 'llm_openai' and 'p' not in record and 't' not in record


def test_a_global_call_counts_under_its_node_type(sink):
    telemetry, _, reader = sink
    _begin(telemetry)
    t = time.time()
    body = _call('POST', t, t + 0.1, pipe=None, component=None, error='401')
    del body['p'], body['c']
    body['ct'] = 'llm_openai'
    telemetry.dependency_relayed('task-1', {'calls': [body]}, PIPELINE)
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    types = {
        dict(p.attributes).get('rocketride.component.type')
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.component.dependency_errors'
        for p in m.data.data_points
    }
    assert types == {'llm_openai'}
    assert _dropped(reader).get('relay_global') == 1


# ── Turn outcome: whether the user got a result, and whose failure it was ─────


def _turns(reader):
    data = reader.get_metrics_data()
    out = {}
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == 'rocketride.turn':
                    for p in metric.data.data_points:
                        if p.value:
                            a = p.attributes
                            key = (a['rocketride.turn.outcome'], a.get('rocketride.turn.cause'), a.get('error.type'))
                            out[key] = out.get(key, 0) + p.value
    return out


def _pass(telemetry, task_id, marks=(), result='continue', leave=True):
    t = time.time()
    telemetry.component_trace(task_id, 'enter', 3, 'my_llm', t, {}, PIPELINE)
    if marks:
        telemetry.dependency_relayed(
            task_id,
            {
                'calls': [
                    {'k': 'err', 'x': kind, 'g': kind, 'w': int((t + 0.1) * 1e9), 'p': 3, 'c': 'my_llm'}
                    for kind in marks
                ]
            },
            PIPELINE,
        )
    if leave:
        telemetry.component_trace(task_id, 'leave', 3, 'my_llm', t + 0.2, {'result': result}, PIPELINE)


@pytest.mark.parametrize(
    'marks, expected',
    [
        ((), ('ok', None, None)),
        (('provider_unavailable',), ('error', 'provider', 'provider_unavailable')),
        (('provider_auth',), ('error', 'customer', 'provider_auth')),
        (('provider_rate_limited',), ('error', 'customer', 'provider_rate_limited')),
        (('provider_error',), ('error', 'platform', 'provider_error')),
        (('something_new',), ('error', 'platform', 'something_new')),
        # several: the platform's first, then the provider's, then the customer's
        (('provider_auth', 'provider_unavailable'), ('error', 'provider', 'provider_unavailable')),
        (('provider_unavailable', 'provider_error'), ('error', 'platform', 'provider_error')),
    ],
)
def test_a_finished_pass_counts_one_turn_with_its_cause(sink, marks, expected):
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', marks)
    telemetry.flush_measurements()
    assert _turns(reader) == {expected: 1}


def test_a_component_error_is_the_platforms_failed_turn(sink):
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', result='error')
    telemetry.flush_measurements()
    assert _turns(reader) == {('error', 'platform', 'component_error'): 1}


def test_a_pass_open_when_the_user_stops_the_run_is_abandoned(sink):
    """Stopping is the user's choice: not a failure (as a run the user stops)."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', leave=False)
    telemetry.task_ended('tok.chat_1', identity, 'cancelled', 'user', None)
    telemetry.flush_measurements()
    assert _turns(reader) == {('abandoned', None, None): 1}


def test_a_pass_open_at_a_restart_is_abandoned_and_its_model_calls_count(sink):
    """The user restarts a chat mid-answer: the turn is abandoned, its model call is still measured."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [
                {
                    'k': 'gen',
                    's': int(t * 1e9),
                    'e': int((t + 0.5) * 1e9),
                    'p': 3,
                    'c': 'my_llm',
                    'a': {'gen_ai.operation.name': 'chat', 'gen_ai.provider.name': 'openai'},
                    'r': [],
                }
            ]
        },
        PIPELINE,
    )
    telemetry.task_restarted('tok.chat_1')
    telemetry.flush_measurements()
    assert _turns(reader) == {('abandoned', None, None): 1}
    assert _points_of(reader, 'gen_ai.client.operation.duration')


def test_a_pass_open_when_the_run_fails_is_the_platforms_failed_turn(sink):
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', leave=False)
    telemetry.task_ended('tok.chat_1', identity, 'error', None, 'signal')
    telemetry.flush_measurements()
    assert _turns(reader) == {('error', 'platform', 'task_end'): 1}


def test_passes_open_at_shutdown_are_the_platforms_failed_turns():
    reader = InMemoryMetricReader()
    telemetry = Telemetry(span_exporter=InMemorySpanExporter(), metric_reader=reader, sample_ratio=0.0)
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', leave=False)
    telemetry.shutdown(1)
    assert _turns(reader) == {('error', 'platform', 'shutdown'): 1}


def _turn_points(reader):
    data = reader.get_metrics_data()
    return [
        (dict(p.attributes), p.value)
        for rm in (data.resource_metrics if data else [])
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.turn'
        for p in m.data.data_points
        if p.value
    ]


def test_a_turn_names_the_llm_server_it_used(sink):
    """Whether a provider is down is the share of its questions failing: each turn names its server."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    # answered: one call to the provider
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('tok.chat_1', {'calls': [_call('POST', t + 0.1, t + 0.2)]}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.3, {'result': 'continue'}, PIPELINE)
    # failed: three refused attempts, then the node's report
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [_call('POST', t + 0.1 * i, t + 0.1 * i + 0.05, error='503') for i in range(1, 4)]
            + [{'k': 'err', 'x': 'provider_unavailable', 'g': 'down', 'w': int((t + 0.5) * 1e9), 'p': 3, 'c': 'my_llm'}]
        },
        PIPELINE,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.6, {'result': 'continue'}, PIPELINE)
    telemetry.flush_measurements()
    points = _turn_points(reader)
    assert len(points) == 2  # one ok series and one error series, nothing else
    by_outcome = {a['rocketride.turn.outcome']: (a, v) for a, v in points}
    assert by_outcome['ok'][0]['server.address'] == 'api.openai.com' and by_outcome['ok'][1] == 1
    failed, count = by_outcome['error']
    assert count == 1  # one question, not one per attempt
    assert failed['server.address'] == 'api.openai.com' and failed['rocketride.turn.cause'] == 'provider'


def test_a_question_with_several_calls_is_one_turn(sink):
    """An agent's tool calls and retries inside one pass are not turns of their own."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'parse_1', t + 0.05, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {'calls': [_call('POST', t + 0.1, t + 0.2), _call('GET', t + 0.2, t + 0.3, component='parse_1')]},
        PIPELINE,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'parse_1', t + 0.35, {'result': 'continue'}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.4, {'result': 'continue'}, PIPELINE)
    telemetry.flush_measurements()
    assert [v for _, v in _turn_points(reader)] == [1]


def test_a_pass_with_no_llm_call_names_no_server(sink):
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.webhook_1')
    _pass(telemetry, 'tok.webhook_1')
    telemetry.flush_measurements()
    ((attributes, _),) = _turn_points(reader)
    assert 'server.address' not in attributes


def test_a_call_to_our_model_server_is_not_a_customers_model(sink):
    """The model server is ours (alerted on its own): a turn does not name it as an LLM provider."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    body = {
        'm': 'rpc.client.call.duration',
        'n': 'infer',
        's': int((t + 0.1) * 1e9),
        'e': int((t + 0.2) * 1e9),
        'a': {'rpc.system.name': 'rocketride_dap', 'rpc.method': 'infer', 'server.address': 'model-server'},
        'p': 3,
        'c': 'my_llm',
        'x': 'UNAVAILABLE',
    }
    telemetry.dependency_relayed('tok.chat_1', {'calls': [body]}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.3, {'result': 'continue'}, PIPELINE)
    telemetry.flush_measurements()
    ((attributes, _),) = _turn_points(reader)
    assert 'server.address' not in attributes


def test_a_turn_records_its_answer_time_by_server(sink):
    """How long the customer waited for the answer, by the LLM server the pass used (no org)."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('tok.chat_1', {'calls': [_call('POST', t + 0.1, t + 0.2)]}, PIPELINE)
    # the answer goes on to the next component: a question went through the pipeline
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'parse_1', t + 1.2, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'parse_1', t + 1.4, {'result': 'continue'}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 1.5, {'result': 'continue'}, PIPELINE)
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    points = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.turn.duration'
        for p in m.data.data_points
    ]
    assert len(points) == 1
    attributes = dict(points[0].attributes)
    assert attributes['server.address'] == 'api.openai.com' and attributes['rocketride.turn.outcome'] == 'ok'
    assert 'rocketride.org.id' not in attributes
    assert points[0].count == 1 and 1.0 <= points[0].sum <= 2.0


def test_a_pass_carries_its_question_past_the_component_span_cap():
    """RR_OTEL_COMPONENT_SPANS=0 keeps no component span; the answer time still counts."""
    spans, reader = InMemorySpanExporter(), InMemoryMetricReader()
    telemetry = Telemetry(span_exporter=spans, metric_reader=reader, sample_ratio=1.0, component_span_cap=0)
    try:
        _begin(telemetry, 'tok.chat_1')
        t = time.time()
        telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
        telemetry.component_trace('tok.chat_1', 'enter', 3, 'parse_1', t + 0.1, {}, PIPELINE)
        telemetry.component_trace('tok.chat_1', 'leave', 3, 'parse_1', t + 0.2, {'result': 'continue'}, PIPELINE)
        telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.3, {'result': 'continue'}, PIPELINE)
        telemetry.flush_measurements()
        data = reader.get_metrics_data()
        names = {m.name for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics}
        assert 'rocketride.turn.duration' in names
    finally:
        telemetry.shutdown(1)


def test_the_answer_time_names_the_source_type(sink):
    """A chat question's answer time is not a document's: the source component's type, never its id."""
    telemetry, _, reader = sink
    pipeline = {'components': [*PIPELINE['components'], {'id': 'chat_1', 'provider': 'chat'}]}
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, pipeline)
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'parse_1', t + 0.5, {}, pipeline)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'parse_1', t + 0.8, {'result': 'continue'}, pipeline)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 1.0, {'result': 'continue'}, pipeline)
    telemetry.flush_measurements()
    points = [
        dict(p.attributes)
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.turn.duration'
        for p in m.data.data_points
    ]
    assert [p.get('rocketride.source.type') for p in points] == ['chat']


def _durations(reader):
    return [
        (dict(p.attributes), p.count)
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == 'rocketride.turn.duration'
        for p in m.data.data_points
    ]


def test_a_pass_through_one_component_alone_has_no_answer_time(sink):
    """A component's own open or close step (one component, no question) is a turn but no answer."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.0002, {'result': 'continue'}, PIPELINE)
    telemetry.flush_measurements()
    assert _durations(reader) == []


def test_a_question_the_user_gave_up_on_is_timed_as_abandoned(sink):
    """Stopped while a question was in the pipeline: how long the user waited, as abandoned."""
    telemetry, _, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time() - 45
    # the question went in (parse_1) and on to the LLM, which has not answered yet
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'parse_1', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t + 0.6, {}, PIPELINE)
    telemetry.task_ended('tok.chat_1', identity(), 'cancelled', 'user', None)
    telemetry.flush_measurements()
    ((attributes, count),) = _durations(reader)
    assert attributes['rocketride.turn.outcome'] == 'abandoned' and count == 1


def test_the_runs_and_questions_traces_name_the_pipelines_project(sink):
    """Which of a customer's pipelines failed: the project id (the same for every run of the pipeline)."""
    telemetry, spans, _ = sink

    def with_project():
        return {'org_id': 'org-a', 'run_kind': 'deploy', 'project_id': 'proj-123'}

    telemetry.task_event('tok.chat_1', {'event': 'apaevt_status_update', 'body': {'state': 1}}, with_project)
    telemetry.task_event('tok.chat_1', {'event': 'apaevt_task', 'body': {'action': 'begin'}}, with_project)
    telemetry.task_event('tok.chat_1', {'event': 'apaevt_status_update', 'body': {'state': 3}}, with_project)
    _pass(telemetry, 'tok.chat_1')
    assert telemetry.flush_traces(5)
    telemetry.task_ended('tok.chat_1', with_project(), 'ok', None, None)
    telemetry._tracer_provider.force_flush()
    named = {s.name: dict(s.attributes) for s in spans.get_finished_spans()}
    assert named['pipeline']['rocketride.project.id'] == 'proj-123'
    assert named['task']['rocketride.project.id'] == 'proj-123'


def test_a_model_call_is_a_genai_span_holding_its_http_tries(sink):
    """relay.model_call: 'chat <model>' under the node, its tries under it with the resend count, retries as events."""
    telemetry, spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [
                _call('POST /v1/chat/completions', t + 0.1, t + 0.2, error='503'),
                _call('POST /v1/chat/completions', t + 0.3, t + 0.4, error='503'),
                {
                    'k': 'gen',
                    's': int((t + 0.1) * 1e9),
                    'e': int((t + 0.4) * 1e9),
                    'p': 3,
                    'c': 'my_llm',
                    'a': {
                        'gen_ai.operation.name': 'chat',
                        'gen_ai.provider.name': 'openai',
                        'gen_ai.request.model': 'gpt-4o',
                        'not.allowed': 'x',
                    },
                    'r': [[int((t + 0.2) * 1e9), 'provider_unavailable', 0.1]],
                    'x': 'provider_unavailable',
                    'g': 'Service Unavailable',
                },
            ]
        },
        PIPELINE,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()

    by_name = {}
    for s in spans.get_finished_spans():
        by_name.setdefault(s.name, []).append(s)
    (llm,), (model,) = by_name['component llm_openai'], by_name['chat gpt-4o']
    assert model.parent.span_id == llm.context.span_id
    assert model.attributes['gen_ai.provider.name'] == 'openai' and 'not.allowed' not in model.attributes
    assert model.status.status_code == StatusCode.ERROR
    assert model.status.description == 'provider_unavailable: Service Unavailable'
    retry, exception = model.events
    assert retry.name == 'rocketride.model.retry' and retry.attributes['error.type'] == 'provider_unavailable'
    assert retry.attributes['rocketride.retry.delay_s'] == 0.1
    assert exception.name == 'exception'
    tries = sorted(by_name['POST /v1/chat/completions'], key=lambda s: s.start_time)
    assert [s.parent.span_id for s in tries] == [model.context.span_id] * 2
    assert 'http.request.resend_count' not in tries[0].attributes
    assert tries[1].attributes['http.request.resend_count'] == 1


def test_a_model_call_inside_a_handler_carries_its_place_retries_and_error(written):
    base = _node_classes()

    class Chatting(base):
        def writeText(self, text):
            relay.model_call(
                'chat', 'openai', 'gpt-4o', 10, 50, 'provider_unavailable', 'Service Unavailable', [(20, '503', 0.5)]
            )

    Chatting(0).writeText('hi')
    (record,) = records(written)
    assert (record['k'], record['p'], record['c'], record['s'], record['e']) == ('gen', 3, 'my_llm', 10, 50)
    assert record['a'] == {
        'gen_ai.operation.name': 'chat',
        'gen_ai.provider.name': 'openai',
        'gen_ai.request.model': 'gpt-4o',
    }
    assert record['r'] == [[20, '503', 0.5]] and record['x'] == 'provider_unavailable'
    assert record['g'] == 'Service Unavailable'


def _points_of(reader, name, data=None):
    """A metric's points; pass one collection's data to read several (a collection takes the exemplars)."""
    data = data or reader.get_metrics_data()
    return [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for p in m.data.data_points
    ]


def test_model_calls_are_measured_by_provider_and_model_with_an_exemplar_of_their_span(sink):
    """gen_ai.client.* and rocketride.turn name the model; their exemplars lead to the kept spans."""
    telemetry, spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [
                _call('POST /v1/chat/completions', t + 0.1, t + 0.2, error='503'),
                {
                    'k': 'gen',
                    's': int((t + 0.1) * 1e9),
                    'e': int((t + 0.3) * 1e9),
                    'p': 3,
                    'c': 'my_llm',
                    'a': {
                        'gen_ai.operation.name': 'chat',
                        'gen_ai.provider.name': 'openai',
                        'gen_ai.request.model': 'gpt-4o',
                    },
                    'r': [],
                    'u': {'input': 16, 'output': 4, 'cache_read': 6},
                    'rm': 'gpt-4o-2024-08-06',
                    'x': 'provider_unavailable',
                    'g': 'Service Unavailable',
                },
            ]
        },
        PIPELINE,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    telemetry.flush_measurements()
    (model_span,) = [s for s in spans.get_finished_spans() if s.name == 'chat gpt-4o']
    assert model_span.attributes['gen_ai.usage.input_tokens'] == 16
    assert model_span.attributes['gen_ai.usage.cache_read.input_tokens'] == 6
    assert model_span.attributes['gen_ai.response.model'] == 'gpt-4o-2024-08-06'

    data = reader.get_metrics_data()
    (duration,) = _points_of(reader, 'gen_ai.client.operation.duration', data)
    assert dict(duration.attributes) == {
        'gen_ai.operation.name': 'chat',
        'gen_ai.provider.name': 'openai',
        'gen_ai.request.model': 'gpt-4o',
        'error.type': 'provider_unavailable',
    }
    (exemplar,) = duration.exemplars
    assert (exemplar.trace_id, exemplar.span_id) == (
        model_span.context.trace_id,
        model_span.context.span_id,
    )
    tokens = {p.attributes['gen_ai.token.type']: p.sum for p in _points_of(reader, 'gen_ai.client.token.usage', data)}
    assert tokens == {'input': 16, 'output': 4}

    (turn,) = [p for p in _points_of(reader, 'rocketride.turn', data) if p.value]
    assert turn.attributes['gen_ai.provider.name'] == 'openai' and turn.attributes['gen_ai.request.model'] == 'gpt-4o'
    (pass_span,) = [s for s in spans.get_finished_spans() if s.name == 'pipeline']
    assert [(e.trace_id, e.span_id) for e in turn.exemplars] == [
        (pass_span.context.trace_id, pass_span.context.span_id)
    ]
    # The failed HTTP try's measurement has no exemplar: when it is measured, whether its
    # pass is exported is not decided yet (the model call's exemplar leads there)
    failed = [p for p in _points_of(reader, 'http.client.request.duration', data) if p.attributes.get('error.type')]
    assert failed and all(not p.exemplars for p in failed)


def test_a_pass_whose_trace_is_not_kept_leaves_no_exemplar(sink):
    telemetry, spans, reader = sink
    telemetry._ratio = 0.0  # errors only: an answered pass is not kept
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.01, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    (turn,) = [p for p in _points_of(reader, 'rocketride.turn') if p.value]
    assert turn.exemplars == [] or all(not e.trace_id for e in turn.exemplars)


def test_models_past_the_bound_of_a_provider_are_other(sink, monkeypatch):
    telemetry, _spans, _reader = sink
    from ai.common.telemetry import _otel

    monkeypatch.setattr(_otel, 'MAX_MODELS_PER_PROVIDER', 2)
    impl = telemetry
    assert [impl._bounded_model('openai', m) for m in ('a', 'b', 'c', 'a')] == ['a', 'b', '_other', 'a']
    assert impl._bounded_model('anthropic', 'c') == 'c'
    assert impl._bounded_model('openai', '') == '_unknown'


def test_a_handler_that_raises_reports_the_node_class_and_place_once(written):
    base = _node_classes()

    class Raising(base):
        def writeText(self, text):
            raise KeyError('sk-proj-abcdefghijklmnopqrstuvwxyz missing')

    with pytest.raises(KeyError):
        Raising(0).writeText('hi')
    (record,) = records(written)
    assert (record['k'], record['p'], record['c'], record['x']) == ('exc', 3, 'my_llm', 'KeyError')
    assert 'abcdefghijklmnop' not in record['g']  # redacted
    assert record['l'].endswith('test_telemetry_relay.py:' + record['l'].rsplit(':', 1)[1])


def test_an_engine_error_is_the_node_whose_handler_raised_with_its_class_on_the_span(sink):
    """The engine's '>ERR' text names no node (common code raised): the relayed raise does."""
    telemetry, spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {
            'calls': [
                {
                    'k': 'exc',
                    'x': 'KeyError',
                    'g': "'choices'",
                    'l': 'ai/common/chat.py:412',
                    'w': int((t + 0.1) * 1e9),
                    'p': 3,
                    'c': 'my_llm',
                }
            ]
        },
        PIPELINE,
    )
    telemetry.task_event(
        'tok.chat_1',
        {'event': 'apaevt_status_error', 'body': {'message': "Error*'choices'*/app/ai/common/chat.py:412"}},
        identity,
    )
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.2, {'result': 'error'}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    errors = [p for p in _points_of(reader, 'rocketride.component.errors') if p.value]
    assert [dict(p.attributes)['rocketride.component.type'] for p in errors] == ['llm_openai']
    by_name = {s.name: s for s in spans.get_finished_spans()}
    llm, root = by_name['component llm_openai'], by_name['pipeline']
    assert llm.status.description == "KeyError: 'choices'" and llm.attributes['error.type'] == 'Error'
    (event,) = [e for e in llm.events if e.name == 'exception']
    assert dict(event.attributes) == {
        'exception.type': 'KeyError',
        'exception.message': "'choices'",
        'code.file.path': 'ai/common/chat.py',
        'code.line.number': 412,
    }
    assert root.status.description == "llm_openai: KeyError: 'choices'"


def test_a_stale_raise_does_not_name_a_later_engine_error(sink, monkeypatch):
    telemetry, _spans, reader = sink
    from ai.common.telemetry import _otel

    _begin(telemetry, 'tok.chat_1')
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', time.time(), {}, PIPELINE)
    telemetry.dependency_relayed(
        'tok.chat_1',
        {'calls': [{'k': 'exc', 'x': 'KeyError', 'g': '', 'l': '', 'w': time.time_ns(), 'p': 3, 'c': 'my_llm'}]},
        PIPELINE,
    )
    monkeypatch.setattr(_otel, 'RAISED_MATCH_SECONDS', -1.0)
    telemetry.task_event(
        'tok.chat_1', {'event': 'apaevt_status_error', 'body': {'message': 'Error*x*store.cpp:1'}}, identity
    )
    (error,) = [p for p in _points_of(reader, 'rocketride.component.errors') if p.value]
    assert dict(error.attributes)['rocketride.component.type'] == '_engine'


def test_a_customers_metrics_and_lines_name_the_pipelines_project_bounded(sink, monkeypatch, capsys):
    """rocketride.project.id on the per-customer metrics (task, turn, failed calls) and on the ended line."""
    telemetry, _spans, reader = sink

    def with_project():
        return {'org_id': 'org-a', 'run_kind': 'deploy', 'project_id': 'proj-123'}

    for event in (
        {'event': 'apaevt_status_update', 'body': {'state': 1}},
        {'event': 'apaevt_task', 'body': {'action': 'begin'}},
        {'event': 'apaevt_status_update', 'body': {'state': 3}},
    ):
        telemetry.task_event('tok.chat_1', event, with_project)
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('tok.chat_1', {'calls': [_call('POST', t + 0.1, t + 0.2, error='503')]}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.3, {}, PIPELINE)
    telemetry.task_ended('tok.chat_1', with_project(), 'ok', None, None)
    telemetry.flush_measurements()
    data = reader.get_metrics_data()
    for name in (
        'rocketride.task.ended',
        'rocketride.task.duration',
        'rocketride.turn',
        'rocketride.component.dependency_errors',
    ):
        (point,) = [p for p in _points_of(reader, name, data) if getattr(p, 'value', getattr(p, 'count', 0))]
        assert point.attributes['rocketride.project.id'] == 'proj-123', name
    (line,) = [json.loads(x) for x in capsys.readouterr().out.splitlines() if '"rocketride.task.ended"' in x]
    assert line['project_id'] == 'proj-123'

    from ai.common.telemetry import _otel

    monkeypatch.setattr(_otel, 'MAX_PROJECTS_PER_ORG', 1)
    assert telemetry._bounded_project('org-b', 'p1') == 'p1'
    assert telemetry._bounded_project('org-b', 'p2') == '_other'
    assert telemetry._bounded_project('org-c', 'p2') == 'p2'


def test_a_run_killed_by_a_signal_says_which_and_whether_memory_is_suspected(sink, capsys):
    telemetry, spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    telemetry.task_ended(
        'tok.chat_1', identity(), 'error', None, 'signal', None, None, {'signal': 'SIGKILL', 'oom_suspected': True}
    )
    telemetry._tracer_provider.force_flush()
    (ended,) = [p for p in _points_of(reader, 'rocketride.task.ended') if p.value]
    assert ended.attributes['rocketride.task.exit_signal'] == 'SIGKILL'
    assert ended.attributes['rocketride.task.oom_suspected'] is True
    (task,) = [s for s in spans.get_finished_spans() if s.name == 'task']
    assert task.status.description == 'run ended in error (signal) by SIGKILL (out of memory suspected)'
    (line,) = [json.loads(x) for x in capsys.readouterr().out.splitlines() if '"rocketride.task.ended"' in x]
    assert (line['exit_signal'], line['oom_suspected']) == ('SIGKILL', True)


def test_a_failed_start_says_the_engines_code_on_the_metric(sink):
    telemetry, _spans, reader = sink
    _begin(telemetry, 'tok.chat_1')
    telemetry.task_ended('tok.chat_1', identity(), 'error', None, 'start_error', 'InvalidParam')
    (ended,) = [p for p in _points_of(reader, 'rocketride.task.ended') if p.value]
    assert ended.attributes['rocketride.engine.error_code'] == 'InvalidParam'
    assert 'rocketride.task.exit_signal' not in ended.attributes


def test_a_pass_cut_by_the_run_end_after_a_failed_call_is_red_with_that_failure(sink):
    """The user stops while the provider fails: the pass's trace says so instead of looking fine."""
    telemetry, spans, _reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('tok.chat_1', {'calls': [_call('POST', t + 0.1, t + 0.2, error='503')]}, PIPELINE)
    telemetry.task_ended('tok.chat_1', identity(), 'cancelled', 'user', None)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    by_name = {s.name: s for s in spans.get_finished_spans()}
    root = by_name['pipeline']
    assert root.status.status_code == StatusCode.ERROR
    assert root.status.description == 'pass cut by the run ending; last dependency error: 503 (api.openai.com)'
    assert root.attributes['rocketride.pass.closed_by'] == 'task_end'


def test_the_runs_trace_links_to_its_failed_passes(sink):
    telemetry, spans, _reader = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.1, {'result': 'error'}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry.task_ended('tok.chat_1', identity(), 'error', None, 'exit_code')
    telemetry._tracer_provider.force_flush()
    by_name = {s.name: s for s in spans.get_finished_spans()}
    (link,) = by_name['task'].links
    assert (link.context.trace_id, link.context.span_id) == (
        by_name['pipeline'].context.trace_id,
        by_name['pipeline'].context.span_id,
    )
    assert link.attributes['rocketride.sampling.reason'] == 'error'


def test_a_pass_whose_trace_could_not_be_queued_leaves_no_exemplar_and_no_link(sink, monkeypatch):
    """A full export queue: the trace will not exist, so nothing may point to it."""
    import queue as queue_module

    telemetry, spans, reader = sink

    def full(item):
        raise queue_module.Full

    monkeypatch.setattr(telemetry._traces, 'put_nowait', full)
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.1, {'result': 'error'}, PIPELINE)
    run = telemetry._runs['tok.chat_1']
    assert run.pass_links == [] and run.keep_reason == ''
    (turn,) = [p for p in _points_of(reader, 'rocketride.turn') if p.value]
    assert all(not e.trace_id for e in turn.exemplars)


def test_a_call_keeps_the_scope_it_started_in(written):
    # a streamed call started in component A and ended while B is handled
    started_in = relay._Scope(1, 'comp_a', 0xA)
    context = relay._context()
    token = context.attach(context.set_value(relay._SCOPE_KEY, relay._Scope(2, 'comp_b', 0xB)))
    try:
        now = time.time_ns()
        relay.emit(_deps.Call('http.client.request.duration', 'POST', now, now, {}, None), started_in)
    finally:
        context.detach(token)
    relay.flush()
    (record,) = records(written)
    assert (record['p'], record['c'], record['t']) == (1, 'comp_a', f'{0xA:032x}')


def test_a_mark_goes_to_its_own_component_when_two_of_one_type_are_adjacent(sink):
    telemetry, spans, _ = sink
    pipeline = {
        'components': [
            {'id': 'chat_1', 'provider': 'chat'},
            {'id': 'llm_a', 'provider': 'llm_openai'},
            {'id': 'llm_b', 'provider': 'llm_openai'},
        ]
    }
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'chat_1', t, {}, pipeline)
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'llm_a', t + 0.01, {}, pipeline)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'llm_a', t + 0.1, {'result': 'continue'}, pipeline)
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'llm_b', t + 0.11, {}, pipeline)
    # within the tolerance of llm_a's end too: the component id decides
    mark = {'k': 'err', 'x': 'provider_auth', 'g': 'denied', 'w': int((t + 0.12) * 1e9), 'p': 3, 'c': 'llm_b'}
    telemetry.dependency_relayed('tok.chat_1', {'calls': [mark]}, pipeline)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'llm_b', t + 0.3, {'result': 'continue'}, pipeline)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'chat_1', t + 0.31, {'result': 'continue'}, pipeline)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    llms = {
        s.attributes['rocketride.component.id']: s
        for s in spans.get_finished_spans()
        if s.name == 'component llm_openai'
    }
    assert llms['llm_a'].status.status_code != StatusCode.ERROR
    assert llms['llm_b'].status.status_code == StatusCode.ERROR
    assert llms['llm_b'].attributes['error.type'] == 'provider_auth'


def test_distinct_requests_under_one_model_call_are_not_resends(sink):
    telemetry, spans, _ = sink
    _begin(telemetry, 'tok.chat_1')
    t = time.time()
    upload = _call('POST /v1/files', t + 0.1, t + 0.15)
    upload['a'] = dict(upload['a'], **{'url.template': '/v1/files'})
    chat = _call('POST /v1/chat/completions', t + 0.2, t + 0.3)
    chat['a'] = dict(chat['a'], **{'url.template': '/v1/chat/completions'})
    lookup = dict(_call('GET', t + 0.31, t + 0.32), m='db.client.operation.duration', a={'db.system.name': 'redis'})
    model = {
        'k': 'gen',
        's': int((t + 0.1) * 1e9),
        'e': int((t + 0.4) * 1e9),
        'p': 3,
        'c': 'my_llm',
        'a': {'gen_ai.operation.name': 'chat', 'gen_ai.provider.name': 'openai', 'gen_ai.request.model': 'gpt-4o'},
    }
    telemetry.component_trace('tok.chat_1', 'enter', 3, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed('tok.chat_1', {'calls': [upload, chat, lookup, model]}, PIPELINE)
    telemetry.component_trace('tok.chat_1', 'leave', 3, 'my_llm', t + 0.5, {}, PIPELINE)
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    tries = [s for s in spans.get_finished_spans() if s.kind == SpanKind.CLIENT and not s.name.startswith('chat ')]
    assert len(tries) == 3
    assert all('http.request.resend_count' not in s.attributes for s in tries)


def test_a_recovered_failure_past_its_budget_draws_the_sample_ratio_once(sink, monkeypatch):
    telemetry, spans, reader = sink
    draws = []
    monkeypatch.setattr(telemetry, '_keep_success', lambda: draws.append(1) or True)
    monkeypatch.setattr(telemetry._dependency_error_limit, 'allow', lambda *a: False)
    _begin(telemetry)
    t = time.time()
    telemetry.component_trace('task-1', 'enter', 1, 'my_llm', t, {}, PIPELINE)
    telemetry.dependency_relayed(
        'task-1', {'calls': [_call('POST', t + 0.01, t + 0.02, pipe=1, error='429')]}, PIPELINE
    )
    telemetry.component_trace('task-1', 'leave', 1, 'my_llm', t + 0.1, {}, PIPELINE)
    assert draws == [1]
    assert telemetry.flush_traces(5)
    telemetry._tracer_provider.force_flush()
    (root,) = [s for s in spans.get_finished_spans() if s.name == 'pipeline']
    assert root.attributes['rocketride.sampling.reason'] == 'ratio'


def test_a_run_live_at_shutdown_exports_its_task_span_for_its_passes_links():
    spans = InMemorySpanExporter()
    telemetry = Telemetry(span_exporter=spans, metric_reader=InMemoryMetricReader(), sample_ratio=0.0)
    _begin(telemetry, 'tok.chat_1')
    _pass(telemetry, 'tok.chat_1', marks=('provider_unavailable',), leave=False)  # a kept pass
    telemetry.shutdown(1)
    by_name = {}
    for s in spans.get_finished_spans():
        by_name.setdefault(s.name, []).append(s)
    (task,), (pipeline,) = by_name['task'], by_name['pipeline']
    assert task.attributes['rocketride.task.closed_by'] == 'shutdown'
    assert 'rocketride.task.outcome' not in task.attributes
    assert [link.context.trace_id for link in pipeline.links] == [task.context.trace_id]
