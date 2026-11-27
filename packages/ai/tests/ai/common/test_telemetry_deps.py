"""
Unit tests for EaaS's dependency calls (ai.common.telemetry._deps): every call
measured, traced only with a kept request (or, outside any request, when it
failed or was slow), and cleaned of statements, URL queries and secrets.
"""

import asyncio
import json
import http.server
import logging
import sqlite3
import threading
import time

import pytest

pytest.importorskip('opentelemetry.sdk')

from opentelemetry import context as otel_context  # noqa: E402
from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace import Event, ReadableSpan, TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import SpanKind, Status, StatusCode  # noqa: E402

from ai.common import logfmt  # noqa: E402
from ai.common import telemetry as telemetry_module  # noqa: E402
from ai.common.telemetry import _deps  # noqa: E402
from ai.common.telemetry._otel import Telemetry  # noqa: E402

sqlalchemy = pytest.importorskip('sqlalchemy')

HTTP_SCOPE = 'opentelemetry.instrumentation.httpx'


@pytest.fixture
def sink():
    spans = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    telemetry = Telemetry(
        span_exporter=spans,
        metric_reader=reader,
        sample_ratio=1.0,
        slow_request_seconds=2.0,
        service_version='9.9.9',
        org_attribute=True,
    )
    recorder = _deps.SqlRecorder(telemetry._dependency_call_hook)
    recorder.install()
    telemetry._recorders.append(recorder)
    telemetry_module._impl = telemetry
    yield telemetry, spans, reader
    telemetry_module._impl = None
    telemetry.shutdown(1)  # removes the recorder


@pytest.fixture
def db():
    engine = sqlalchemy.create_engine('sqlite://')
    with engine.connect() as connection:
        yield connection


def exported(telemetry, spans):
    telemetry._tracer_provider.force_flush()
    return spans.get_finished_spans()


def points(reader, name):
    if telemetry_module._impl is not None:
        telemetry_module._impl.flush_measurements()  # recorded by the otel-deps thread every second
    data = reader.get_metrics_data()
    out = []
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    out.extend((dict(p.attributes), p) for p in metric.data.data_points)
    return out


def dropped(reader):
    return {a['rocketride.telemetry.reason']: p.value for a, p in points(reader, 'rocketride.telemetry.dropped')}


def query(connection, sql="SELECT 'bob@example.com'"):
    connection.execute(sqlalchemy.text(sql)).fetchall()


def http_call(
    telemetry,
    status=200,
    url='https://api.stripe.com/v1/customers?email=bob@example.com&key=sk-abcdef123456',  # gitleaks:allow
):
    """A span as the httpx instrumentation records it, under whatever is current."""
    tracer = telemetry._tracer_provider.get_tracer(HTTP_SCOPE)
    with tracer.start_as_current_span(
        'GET',
        kind=SpanKind.CLIENT,
        attributes={
            'http.request.method': 'GET',
            'url.full': url,
            'server.address': 'api.stripe.com',
            'http.response.status_code': status,
        },
    ):
        pass


def handled(method, work, error_type=None, duration=0.01):
    """A DAP request as dap_conn handles it: request_scope around the handler, then request_done."""

    async def go():
        with telemetry_module.request_scope(method) as request:
            work()
        telemetry_module.request_done(method, error_type, duration, None, request)

    asyncio.run(go())


def test_a_failed_request_keeps_its_calls_cleaned(sink, db):
    telemetry, spans, _ = sink
    handled('rrext_get_account', lambda: (query(db), http_call(telemetry, 402)), 'Denied')

    by_name = {span.name: span for span in exported(telemetry, spans)}
    request, select, http = by_name['rrext_get_account'], by_name['SELECT sqlite'], by_name['GET']
    assert request.kind == SpanKind.SERVER and request.status.status_code == StatusCode.ERROR
    assert request.attributes['rocketride.sampling.reason'] == 'error'
    for child in (select, http):
        assert child.context.trace_id == request.context.trace_id
        assert child.parent.span_id == request.context.span_id
    assert select.kind == SpanKind.CLIENT and select.attributes['db.operation.name'] == 'SELECT'
    assert select.start_time >= request.start_time and select.end_time <= request.end_time
    # Statement text, URL query and credentials never leave the process
    assert not any('bob@example.com' in str(value) for span in by_name.values() for value in span.attributes.values())
    # Never a URL (its path can carry a token): the scheme and the server only
    assert 'url.full' not in http.attributes and http.attributes['url.scheme'] == 'https'


def test_a_fast_successful_request_is_dropped_but_measured(sink, db):
    telemetry, spans, reader = sink
    handled('rrext_get_account', lambda: (query(db), http_call(telemetry)))

    assert exported(telemetry, spans) == ()
    assert [a for a, _ in points(reader, 'db.client.operation.duration')] == [
        {'db.system.name': 'sqlite', 'db.operation.name': 'SELECT', 'rocketride.dependency.origin': 'eaas'}
    ]
    assert [a for a, _ in points(reader, 'http.client.request.duration')] == [
        {
            'http.request.method': 'GET',
            'server.address': 'api.stripe.com',
            'server.port': '443',
            'http.response.status_code': '200',
            'rocketride.dependency.origin': 'eaas',
        }
    ]
    assert telemetry._spans._held == 0  # the HTTP span was dropped with its request


def test_a_slow_request_keeps_its_calls(sink, db):
    telemetry, spans, _ = sink
    handled('rrext_list', lambda: query(db), duration=3.0)
    reasons = {s.name: (s.attributes or {}).get('rocketride.sampling.reason') for s in exported(telemetry, spans)}
    assert reasons == {'rrext_list': 'slow', 'SELECT sqlite': None}


def test_an_error_log_line_keeps_its_request_and_names_its_trace(sink, db):
    telemetry, spans, _ = sink
    seen = {}

    def work():
        query(db)
        record = logging.LogRecord('x', logging.ERROR, __file__, 1, 'it broke', None, None)
        seen['line'] = logfmt.JsonFormatter().format(record)

    handled('rrext_get', work)
    kept = exported(telemetry, spans)
    request = next(s for s in kept if s.kind == SpanKind.SERVER)
    assert request.attributes['rocketride.sampling.reason'] == 'error_log'
    assert format(request.context.trace_id, '032x') in seen['line']
    assert any(s.name == 'SELECT sqlite' for s in kept)


def test_a_call_outside_any_request_decides_itself(sink, db):
    telemetry, spans, _ = sink
    query(db)  # fast and fine: dropped
    with pytest.raises(Exception):
        query(db, 'SELEC broken')  # failed: kept
    now = time.time_ns()
    slow = _deps.Call('db.client.operation.duration', 'GET', now - 3 * 10**9, now, {'db.system.name': 'redis'}, None)
    telemetry.dependency_call(slow)  # slow: kept
    kept = exported(telemetry, spans)
    assert sorted(s.name for s in kept) == ['GET', 'SELEC sqlite']
    failed = next(s for s in kept if s.name == 'SELEC sqlite')
    assert failed.status.status_code == StatusCode.ERROR and failed.attributes['error.type'] == 'OperationalError'
    assert failed.parent is None


def test_an_engine_made_before_recording_is_recorded(sink):
    telemetry, spans, _ = sink
    telemetry._recorders[0].uninstall()
    early = sqlalchemy.create_engine('sqlite://')  # made while nothing records
    telemetry._recorders[0].install()  # the listeners are on the Engine class
    with early.connect() as connection:
        handled('rrext_q', lambda: query(connection), 'Failed')
    assert any(s.name == 'SELECT sqlite' for s in exported(telemetry, spans))


def test_calls_per_request_are_capped(sink, db, monkeypatch):
    telemetry, spans, reader = sink
    monkeypatch.setattr(_deps, 'MAX_CALLS_PER_REQUEST', 3)
    handled('rrext_many', lambda: [query(db) for _ in range(5)], 'Failed')
    kept = exported(telemetry, spans)
    assert sum(s.name == 'SELECT sqlite' for s in kept) == 3
    assert next(s for s in kept if s.kind == SpanKind.SERVER).attributes['rocketride.calls.dropped'] == 2
    assert dropped(reader)['dependency_span_cap'] == 2


def _held_request(telemetry, calls=1):
    """A request (not decided yet) with `calls` HTTP spans held for it; its id."""
    tracer = telemetry._tracer_provider.get_tracer(HTTP_SCOPE)
    with telemetry_module.request_scope('rrext_x') as request:
        for _ in range(calls):
            with tracer.start_as_current_span('GET', kind=SpanKind.CLIENT):
                pass
    return request.span_id


def test_library_spans_late_or_undecided(sink, monkeypatch):
    telemetry, spans, reader = sink
    buffer = telemetry._spans
    kept = _held_request(telemetry)
    buffer.decide(kept, True)
    assert len(exported(telemetry, spans)) == 1

    _held_request(telemetry)  # never decided
    assert buffer._held == 1
    monkeypatch.setattr(_deps, 'UNDECIDED_SECONDS', 0.0)
    http_call(telemetry)  # any span ending sweeps: the undecided request is dropped and counted
    assert buffer._held == 0
    assert dropped(reader)['dependency_undecided'] == 1


def test_library_spans_held_per_request_are_capped(sink, monkeypatch):
    telemetry, spans, reader = sink
    monkeypatch.setattr(_deps, 'MAX_SPANS_PER_TRACE', 3)
    telemetry._spans.decide(_held_request(telemetry, calls=5), True)
    assert len(exported(telemetry, spans)) == 3
    assert dropped(reader)['dependency_span_cap'] == 2


def test_two_requests_in_one_callers_trace_decide_apart(sink, db):
    """The SDK sends its current traceparent with every request: one trace, several requests."""
    telemetry, spans, _ = sink
    telemetry._ratio = 0.0  # errors only: the first request is dropped
    traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

    async def go(error):
        with telemetry_module.request_scope('rrext_x', traceparent) as request:
            query(db)
            http_call(telemetry, 500 if error else 200)
        telemetry_module.request_done('rrext_x', error, 0.01, traceparent, request)

    asyncio.run(go(None))  # fast and fine: dropped
    asyncio.run(go('Failed'))  # same trace, failed: kept with its calls
    kept = exported(telemetry, spans)
    assert sorted(s.name for s in kept) == ['GET', 'SELECT sqlite', 'rrext_x']
    assert {format(s.context.trace_id, '032x') for s in kept} == {'0af7651916cd43dd8448eb211c80319c'}


def test_a_library_span_outside_any_request_decides_itself(sink):
    telemetry, spans, _ = sink
    http_call(telemetry)  # fine and fast: dropped
    tracer = telemetry._tracer_provider.get_tracer(HTTP_SCOPE)
    with tracer.start_as_current_span('POST', kind=SpanKind.CLIENT, attributes={'http.request.method': 'POST'}) as span:
        span.set_status(Status(StatusCode.ERROR))
    assert [s.name for s in exported(telemetry, spans)] == ['POST']


def test_a_task_spawned_from_a_request_inherits_neither_trace_nor_request(sink):
    seen = {}

    async def background():
        seen['span'] = trace.get_current_span()
        seen['request'] = _deps.current_request()

    async def request():
        with telemetry_module.request_scope('rrext_execute') as current:
            await asyncio.create_task(background(), context=telemetry_module.detached_context())
            seen['inside'] = (trace.get_current_span(), _deps.current_request() is current)

    asyncio.run(request())
    assert seen['inside'][0].get_span_context().is_valid and seen['inside'][1]
    assert not seen['span'].get_span_context().is_valid
    assert seen['request'] is None


def test_hooks_are_no_ops_when_off():
    assert telemetry_module._impl is None
    with telemetry_module.request_scope('x') as request:
        assert request is None
    telemetry_module.request_done('x', None, 0.1, None, None)
    telemetry_module.error_logged()
    telemetry_module.set_span_attributes(a=1)
    assert telemetry_module.detached_context() is None
    assert otel_context.get_current() == otel_context.Context()


def test_a_callers_trace_is_continued(sink, db):
    telemetry, spans, _ = sink
    traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

    async def go():
        with telemetry_module.request_scope('rrext_x', traceparent) as request:
            query(db)
        telemetry_module.request_done('rrext_x', 'Failed', 0.01, traceparent, request)

    asyncio.run(go())
    request = next(s for s in exported(telemetry, spans) if s.kind == SpanKind.SERVER)
    assert format(request.context.trace_id, '032x') == '0af7651916cd43dd8448eb211c80319c'
    assert format(request.parent.span_id, '016x') == 'b7ad6b7169203331'


def test_a_token_in_a_url_path_never_leaves():
    tracer = TracerProvider().get_tracer('x')
    span = tracer.start_span(
        'GET', attributes={'url.full': 'https://api.telegram.org/bot123456:ABCdefSECRET/getUpdates'}
    )
    span.add_event(
        'exception', {'exception.message': 'timeout calling https://api.telegram.org/bot123456:ABCdefSECRET/getUpdates'}
    )
    span.end()
    clean = _deps.sanitized(span, logfmt.redact)
    text = str(dict(clean.attributes)) + str(dict(clean.events[0].attributes))
    assert 'SECRET' not in text and 'api.telegram.org' in text


def test_sanitized_spans_lose_statements_queries_and_secrets():
    tracer = TracerProvider().get_tracer('x')
    span = tracer.start_span(
        'INSERT',
        attributes={
            'db.query.text': 'INSERT INTO t VALUES (%s)',
            'db.statement': 'x',
            'db.query.parameter.0': 'bob',
            'url.full': 'https://user:pw@host/p?token=1#f',
        },
    )
    span.add_event('exception', {'exception.message': "dup [SQL: INSERT x] [parameters: ('bob@example.com',)]"})
    span.set_status(Status(StatusCode.ERROR, 'Bearer abcdefghijklmnop failed'))
    span.end()
    clean = _deps.sanitized(span, logfmt.redact)
    # the URL never leaves: its scheme, its port and its route only (an unknown path: _OTHER)
    assert dict(clean.attributes) == {'url.scheme': 'https', 'url.template': '_OTHER', 'server.port': 443}
    assert 'bob' not in clean.events[0].attributes['exception.message']
    assert 'abcdefghijklmnop' not in clean.status.description
    assert isinstance(clean, ReadableSpan) and isinstance(clean.events[0], Event)


@pytest.mark.parametrize(
    'text, kept',
    [
        ("dup [SQL: INSERT x] [parameters: ('bob@example.com',)]", 'dup'),
        (
            'msg\n[SQL: SELECT a] (b)]\n[parameters: (1,)]\n(Background on this error at: x)',
            'msg\n(Background on this error at: x)',
        ),
        ('no statement here', 'no statement here'),
    ],
)
def test_an_error_text_loses_the_statement_and_its_parameters(text, kept):
    assert _deps._clean_text(text, lambda t: t) == kept


@pytest.mark.parametrize('text', [' ' * 200_000 + 'x', '[SQL: ' * 50_000, 'a.' * 100_000, 'a://' + 'b' * 200_000])
def test_cleaning_a_large_error_text_stays_fast(text):
    start = time.perf_counter()
    _deps._clean_text(text, lambda t: t)
    assert time.perf_counter() - start < 0.5


def test_measurement_of_aws_calls_and_others():
    tracer = TracerProvider().get_tracer('opentelemetry.instrumentation.botocore')
    span = tracer.start_span(
        'S3.PutObject',
        kind=SpanKind.CLIENT,
        attributes={'rpc.system': 'aws-api', 'rpc.service': 'S3', 'rpc.method': 'PutObject'},
    )
    span.end()
    name, _, attrs = _deps.measurement(span)
    assert name == 'rpc.client.call.duration'
    assert attrs == {'rpc.system.name': 'aws-api', 'rpc.service': 'S3', 'rpc.method': 'PutObject', 'error.type': None}
    server = tracer.start_span('x', kind=SpanKind.SERVER, attributes={'http.request.method': 'GET'})
    server.end()
    assert _deps.measurement(server) is None


def test_flags_and_requirements(monkeypatch):
    assert _deps.parse_flags(None) == (['db', 'redis', 'http', 'aws', 'grpc', 'model'], [])
    assert _deps.parse_flags('none') == ([], [])
    assert _deps.parse_flags('') == ([], [])
    assert _deps.parse_flags('http, db,kafka') == (['db', 'http'], ['kafka'])
    monkeypatch.setattr('opentelemetry.sdk.version.__version__', '1.45.2')
    assert _deps.contrib_version() == '0.66'


def test_requirements_pin_every_instrumentation_at_the_sdks_contrib_release():
    with open(telemetry_module.SDK_REQUIREMENTS, encoding='utf-8') as file:
        pins = dict(line.split('==') for line in (raw.split('#')[0].strip() for raw in file) if line)
    sdk = pins.pop('opentelemetry-sdk')
    assert pins.pop('opentelemetry-exporter-otlp-proto-http') == sdk
    contrib = f'0.{int(sdk.split(".")[1]) + 21}'  # SDK 1.N pairs with contrib 0.(N+21)
    names = {package for *_, package in _deps.INSTRUMENTATIONS} | set(_deps.EXTRA_PACKAGES)
    assert set(pins) == names
    assert all(version.startswith(contrib + 'b') for version in pins.values()), pins


def test_redis_calls_are_recorded():
    redis = pytest.importorskip('redis')
    calls = []
    recorder = _deps.RedisRecorder(calls.append)
    assert recorder.install()
    try:
        client = redis.Redis(host='127.0.0.1', port=1, socket_connect_timeout=0.05)
        # refused at once on Linux and macOS; Windows retries the connect and times out
        with pytest.raises((redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)):
            client.get('k')
    finally:
        recorder.uninstall()
    recorded = [(c.name, c.error_type, c.attributes['server.address']) for c in calls]
    assert len(recorded) == 1 and recorded[0][1] in ('ConnectionError', 'TimeoutError')
    assert recorded[0][0] == 'GET' and recorded[0][2] == '127.0.0.1'
    assert redis.Redis.execute_command.__name__ == 'execute_command'  # restored


def test_gauges_and_request_attributes(sink):
    telemetry, spans, reader = sink
    gauge = telemetry_module.Gauge(
        'rocketride.test.depth',
        '{batch}',
        'test',
        observe=lambda: [(3, {'kind': 'whisper'}), (1, {'kind': 'gliner', 'ignored': 'x'})],
        attributes={'kind': 'rocketride.model.type'},
    )
    timings = {'rocketride.model.queue_wait_ms': 12.5, 'rocketride.model.id': 'm'}
    try:
        handled('rrext_ms_inference', lambda: telemetry_module.set_span_attributes(**timings), 'Failed')
        depth = {a['rocketride.model.type']: p.value for a, p in points(reader, 'rocketride.test.depth')}
    finally:
        telemetry_module._declared.remove(gauge)
    assert depth == {'whisper': 3, 'gliner': 1}
    request = exported(telemetry, spans)[0]
    assert request.attributes['rocketride.model.queue_wait_ms'] == 12.5
    assert request.attributes['rocketride.model.id'] == 'm'


def test_real_httpx_instrumentation_joins_the_request(sink):
    httpx = pytest.importorskip('httpx')
    pytest.importorskip('opentelemetry.instrumentation.httpx')
    telemetry, spans, reader = sink
    done, skipped = telemetry.instrument_dependencies({'RR_OTEL_INSTRUMENT': 'http'})
    assert 'httpx' in done or any(reason.startswith('httpx: already') for reason in skipped)

    class Unavailable(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(('127.0.0.1', 0), Unavailable)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_port}/v1/items'

    def call():
        with httpx.Client(trust_env=False) as client:
            client.get(url + '?token=secret-value')

    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    try:
        handled('rrext_items', call, 'Unavailable')
    finally:
        try:
            HTTPXClientInstrumentor().uninstrument()
        finally:
            server.shutdown()
            server.server_close()
    by_kind = {span.kind: span for span in exported(telemetry, spans)}
    client = by_kind[SpanKind.CLIENT]
    assert client.parent.span_id == by_kind[SpanKind.SERVER].context.span_id
    assert 'url.full' not in client.attributes and 'http.url' not in client.attributes
    attrs = [a for a, _ in points(reader, 'http.client.request.duration')]
    assert attrs == [
        {
            'http.request.method': 'GET',
            'server.address': '127.0.0.1',
            'server.port': str(server.server_port),
            'http.response.status_code': '503',
            'error.type': '503',
            'rocketride.dependency.origin': 'eaas',
        }
    ]


def test_a_failed_pre_ping_is_not_a_failed_call():
    calls = []
    recorder = _deps.SqlRecorder(calls.append)

    class Context:
        is_pre_ping = True
        execution_context = None
        original_exception = ConnectionError('gone away')
        engine = None
        statement = None

    recorder._failed(Context())
    assert calls == []


def test_stamps_of_spans_that_never_end_expire(sink, monkeypatch):
    telemetry, _, _ = sink
    tracer = telemetry._tracer_provider.get_tracer(HTTP_SCOPE)
    with telemetry_module.request_scope('rrext_stream'):
        tracer.start_span('GET', kind=SpanKind.CLIENT)  # a stream nobody closes: never ends
    assert len(telemetry._spans._owner) == 1
    monkeypatch.setattr(_deps, 'UNDECIDED_SECONDS', 0.0)
    http_call(telemetry)  # any span ending sweeps
    assert telemetry._spans._owner == {}


def test_a_failed_call_whose_request_stamp_expired_is_exported_marked(sink, monkeypatch):
    telemetry, spans, _ = sink
    tracer = telemetry._tracer_provider.get_tracer(HTTP_SCOPE)
    with telemetry_module.request_scope('rrext_stream'):
        late = tracer.start_span('GET', kind=SpanKind.CLIENT, attributes={'http.request.method': 'GET'})
    monkeypatch.setattr(_deps, 'UNDECIDED_SECONDS', 0.0)
    http_call(telemetry)  # sweeps: the stamp of the open call expires
    late.set_status(Status(StatusCode.ERROR))
    late.end()
    marked = [s for s in exported(telemetry, spans) if s.attributes.get('rocketride.dependency.owner_expired') is True]
    assert [s.context.span_id for s in marked] == [late.get_span_context().span_id]
    # a call that started outside any request is not marked
    assert not any(
        s.attributes.get('rocketride.dependency.owner_expired')
        for s in exported(telemetry, spans)
        if s.context.span_id != late.get_span_context().span_id
    )
    assert telemetry._spans._orphans == {}


def test_own_spans_are_not_tracked_as_open_client_calls(sink):
    telemetry, _, _ = sink
    own = telemetry._tracer_provider.get_tracer('rocketride.eaas')
    before = len(_deps._OPEN_CLIENTS)
    span = own.start_span('model call', kind=SpanKind.CLIENT)
    assert len(_deps._OPEN_CLIENTS) == before  # its end never calls client_ended
    span.end()


def test_a_failed_connect_lasts_as_long_as_the_attempt():
    calls = []
    recorder = _deps.SqlRecorder(calls.append)
    recorder.install()

    def slow_refusal():
        time.sleep(0.05)  # a pool timeout or a connect timeout before the error
        raise sqlite3.OperationalError('unable to open database file')

    try:
        engine = sqlalchemy.create_engine('sqlite://', creator=slow_refusal)
        with pytest.raises(Exception):
            engine.connect()
    finally:
        recorder.uninstall()
    (call,) = calls
    assert call.end_ns - call.start_ns >= 50_000_000


def test_an_exhausted_pool_is_told_apart_from_the_database():
    calls = []
    recorder = _deps.SqlRecorder(calls.append)
    recorder.install()
    try:
        engine = sqlalchemy.create_engine(
            'sqlite:///:memory:', poolclass=sqlalchemy.pool.QueuePool, pool_size=1, max_overflow=0, pool_timeout=0.01
        )
        held = engine.connect()  # the only connection
        with pytest.raises(sqlalchemy.exc.TimeoutError):
            engine.connect()
        held.close()
    finally:
        recorder.uninstall()
    assert [c.error_type for c in calls] == ['sqlalchemy.exc.TimeoutError']


def test_a_database_that_is_down_is_a_failed_connect():
    calls = []
    recorder = _deps.SqlRecorder(calls.append)
    recorder.install()
    try:
        engine = sqlalchemy.create_engine('sqlite:////nonexistent-dir/x.db')  # cannot be opened
        with pytest.raises(Exception):
            engine.connect()
    finally:
        recorder.uninstall()
    assert [(c.attributes['db.operation.name'], c.error_type) for c in calls] == [('CONNECT', 'OperationalError')]
    assert sqlalchemy.engine.Engine.raw_connection.__name__ == 'raw_connection'  # restored, not ours
    assert not hasattr(sqlalchemy.engine.Engine.raw_connection, '__wrapped__')


# ── Databases and gRPC without SQLAlchemy, drivers installed later ───────────


def _client_span(attributes, status=StatusCode.UNSET):
    tracer = TracerProvider().get_tracer('test')
    span = tracer.start_span('call', kind=SpanKind.CLIENT, attributes=attributes)
    span.set_status(Status(status))
    span.end()
    return span


def test_database_and_grpc_spans_are_measured_without_text_or_names():
    metric, _, attributes = _deps.measurement(
        _client_span(
            {
                'db.system': 'mongodb',
                'db.statement': 'find {"email": "bob@example.com"}',
                'db.mongodb.collection': 'customers',
                'net.peer.name': 'mongo.customer.net:27017',
            }
        )
    )
    assert metric == 'db.client.operation.duration'
    assert attributes == {
        'db.system.name': 'mongodb',
        'db.operation.name': 'find',  # as MongoDB spells its commands
        'server.address': 'mongo.customer.net',
        'error.type': None,
    }
    metric, _, attributes = _deps.measurement(
        _client_span(
            {
                'rpc.system': 'grpc',
                'rpc.service': 'qdrant.Points',
                'rpc.method': 'Search',
                'net.peer.name': 'qdrant.local',
                'rpc.grpc.status_code': 14,
            },
            StatusCode.ERROR,
        )
    )
    assert metric == 'rpc.client.call.duration'
    assert attributes == {
        'rpc.system.name': 'grpc',
        'rpc.service': 'qdrant.Points',
        'rpc.method': 'Search',
        'server.address': 'qdrant.local',
        'error.type': 'UNAVAILABLE',
    }


def test_a_driver_span_inside_a_sqlalchemy_statement_is_not_a_second_call(db):
    seen = []
    recorder = _deps.SqlRecorder(lambda call: None)
    recorder.install()

    def during(conn, cursor, statement, parameters, context, executemany):
        seen.append(_deps._IN_SQLALCHEMY.get())  # what a driver's span sees, inside the statement

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, 'before_cursor_execute', during)
    try:
        query(db)
    finally:
        sqlalchemy.event.remove(sqlalchemy.engine.Engine, 'before_cursor_execute', during)
        recorder.uninstall()
    assert seen == [True] and _deps._IN_SQLALCHEMY.get() is False
    _deps._IN_SQLALCHEMY.set(True)
    try:
        assert _deps.measurement(_client_span({'db.system': 'postgresql', 'db.statement': 'SELECT 1'})) is None
    finally:
        _deps._IN_SQLALCHEMY.set(False)


def test_a_dependency_call_is_timed_and_its_exception_goes_on():
    calls = []
    with _deps.DependencyCall(calls.append, 'db.client.operation.duration', 'MATCH', {'db.system.name': 'neo4j'}):
        pass

    async def failing():
        async with _deps.DependencyCall(calls.append, 'db.client.operation.duration', 'MATCH', {}):
            raise TimeoutError('slow')

    with pytest.raises(TimeoutError):
        asyncio.run(failing())
    assert [c.error_type for c in calls] == [None, 'TimeoutError']
    assert all(c.end_ns >= c.start_ns for c in calls)


def test_neo4j_queries_are_recorded():
    neo4j = pytest.importorskip('neo4j')
    calls = []
    recorder = _deps.Neo4jRecorder(calls.append)
    recorder.install()
    try:
        with pytest.raises(Exception):
            neo4j.Session.run(object(), 'MATCH (n:Customer) RETURN n.email')  # not a session: fails, recorded
    finally:
        recorder.uninstall()
    assert [(c.attributes['db.system.name'], c.name, c.error_type is not None) for c in calls] == [
        ('neo4j', 'MATCH', True)
    ]
    assert not hasattr(neo4j.Session.run, '__wrapped__')  # restored


def test_a_library_a_node_installs_later_is_instrumented_then(monkeypatch):
    import importlib.machinery
    import sys
    import types

    class Instrumentor:
        is_instrumented_by_opentelemetry = False

        def instrument(self, **kwargs):
            Instrumentor.is_instrumented_by_opentelemetry = True

    instrumentation = types.ModuleType('fake_instrumentation')
    instrumentation.Instrumentor = Instrumentor
    monkeypatch.setitem(sys.modules, 'fake_instrumentation', instrumentation)
    monkeypatch.setattr(
        _deps, 'INSTRUMENTATIONS', (('db', 'fake_instrumentation', 'Instrumentor', 'fake_driver', 'fake-pkg'),)
    )
    monkeypatch.setattr(_deps, 'RECORDERS', ())
    monkeypatch.setattr('importlib.metadata.version', lambda name: _deps.contrib_version() + 'b0')
    done, _skipped, _ = _deps.instrument(None, ['db'], lambda call: None)
    assert done == []  # the driver is not installed yet
    driver = types.ModuleType('fake_driver')
    driver.__spec__ = importlib.machinery.ModuleSpec('fake_driver', None)
    monkeypatch.setitem(sys.modules, 'fake_driver', driver)  # the node's depends() installed it
    assert _deps.instrument_new() == ['fake_driver']
    assert _deps.instrument_new() == []  # once


def test_two_nodes_installing_drivers_at_once_install_each_recorder_once(monkeypatch):
    import importlib.machinery
    import sys
    import types

    installs = []

    class Recorder:
        def __init__(self, on_call):
            pass

        def install(self):
            installs.append(1)
            time.sleep(0.05)  # long enough for the other thread to look at the same library
            return True

    monkeypatch.setattr(_deps, 'INSTRUMENTATIONS', ())
    monkeypatch.setattr(_deps, 'RECORDERS', (('db', 'fake_graph_driver', Recorder),))
    _deps.instrument(None, ['db'], lambda call: None)
    driver = types.ModuleType('fake_graph_driver')
    driver.__spec__ = importlib.machinery.ModuleSpec('fake_graph_driver', None)
    monkeypatch.setitem(sys.modules, 'fake_graph_driver', driver)
    threads = [threading.Thread(target=_deps.instrument_new) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert installs == [1]  # one recorder: one count per query


def test_grpc_calls_reach_the_provider_given_not_the_global_one():
    grpc = pytest.importorskip('grpc')
    pytest.importorskip('opentelemetry.instrumentation.grpc')
    from ai.common.telemetry._grpc import GrpcClient

    spans = InMemorySpanExporter()
    provider = TracerProvider()
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    provider.add_span_processor(SimpleSpanProcessor(spans))
    instrumentor = GrpcClient()
    if instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.uninstrument()
    instrumentor.instrument(tracer_provider=provider)
    try:
        channel = grpc.insecure_channel('127.0.0.1:1')  # nothing listens: UNAVAILABLE, no network
        with pytest.raises(grpc.RpcError):
            channel.unary_unary('/rocketride.Test/Call')(b'', timeout=2)
        channel.close()
    finally:
        instrumentor.uninstrument()
    [span] = spans.get_finished_spans()
    assert span.kind == SpanKind.CLIENT
    assert span.attributes['rpc.grpc.status_code'] == grpc.StatusCode.UNAVAILABLE.value[0]


def test_a_request_names_its_customer_connection_and_run_and_links_to_the_runs_trace(sink):
    """Who asked and about which run: on the span (with a link to the run), the org on a counter."""
    telemetry, spans, reader = sink
    identity = lambda: {'org_id': 'org-a', 'run_kind': 'dev'}  # noqa: E731
    for event in (
        {'event': 'apaevt_status_update', 'body': {'state': 1}},
        {'event': 'apaevt_task', 'body': {'action': 'begin'}},
    ):
        telemetry.task_event('tok.chat_1', event, identity)
    run = telemetry._runs['tok.chat_1']

    async def go():
        with telemetry_module.request_scope('execute') as request:
            pass
        telemetry_module.request_done(
            'execute',
            'OUT_OF_CREDITS',
            0.01,
            None,
            request,
            {'org_id': 'org-a', 'client': 'user', 'connection_id': '42', 'task_id': 'tok.chat_1'},
        )

    asyncio.run(go())
    (span,) = [s for s in exported(telemetry, spans) if s.name == 'execute']
    assert span.attributes['rocketride.org.id'] == 'org-a'
    assert span.attributes['rocketride.connection.id'] == '42'
    assert span.attributes['rocketride.task.id'] == 'tok.chat_1'
    assert span.attributes['rocketride.ws.client'] == 'user'
    (link,) = span.links
    assert (link.context.trace_id, link.context.span_id) == (run.trace_id, run.span_id)
    assert run.keep_reason == 'request_kept'  # the link leads to a kept trace

    data = reader.get_metrics_data()
    found = {
        m.name: list(m.data.data_points)
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name in ('rocketride.ws.requests', 'rpc.server.call.duration')
    }
    (counted,) = found['rocketride.ws.requests']
    assert dict(counted.attributes) == {
        'rocketride.ws.client': 'user',
        'error.type': 'OUT_OF_CREDITS',
        'rocketride.org.id': 'org-a',
    }
    (timed,) = found['rpc.server.call.duration']
    assert 'rocketride.org.id' not in timed.attributes  # no org on the histogram: bounded series
    assert [(e.trace_id, e.span_id) for e in timed.exemplars] == [(span.context.trace_id, span.context.span_id)]


def test_the_connection_says_who_made_a_request_never_the_token():
    from types import SimpleNamespace

    from ai.modules.task.task_conn import TaskConn

    account = SimpleNamespace(auth='sk_abc', userId='u1', organization={'id': 'org-a'})
    conn = SimpleNamespace(
        connection_state=(account, 'org-a', False),
        _signed_in_org='org-a',
        _connection_id=7,
        _account_info=account,
        _authenticated=True,
        _server=SimpleNamespace(_task_control={'secret-token': SimpleNamespace(id='tok.chat_1')}),
    )
    identity = TaskConn.request_identity(conn, {'arguments': {'token': 'secret-token'}})
    assert identity['task_id'] == 'tok.chat_1' and identity['connection_id'] == '7'
    assert identity['client'] == 'user'
    assert 'secret-token' not in identity.values()
    assert TaskConn.request_identity(SimpleNamespace(), {}) == {}  # never raises


def test_a_public_key_request_names_the_run_its_key_is_bound_to():
    from types import SimpleNamespace

    from ai.modules.task.task_conn import TaskConn

    bound = SimpleNamespace(id='tok.a')
    account = SimpleNamespace(auth='pk_a', userId='u1', organization={'id': 'org-a'})
    conn = SimpleNamespace(
        connection_state=(account, 'org-a', False),
        _signed_in_org='org-a',
        _connection_id=7,
        _account_info=account,
        _authenticated=True,
        _server=SimpleNamespace(
            _task_control={'token-b': SimpleNamespace(id='tok.b')},
            get_task_control_by_public_key=lambda key: bound,
        ),
    )
    # no token, or another run's token: still the run the key is bound to
    assert TaskConn.request_identity(conn, {})['task_id'] == 'tok.a'
    assert TaskConn.request_identity(conn, {'arguments': {'token': 'token-b'}})['task_id'] == 'tok.a'


def test_an_overdue_request_is_marked_kept_before_the_lock_is_released(sink, monkeypatch):
    telemetry, _spans, _reader = sink
    telemetry._overdue = 0.01
    request, _context = telemetry.request_scope('rrext_execute', None, {'org_id': 'org-a'})
    time.sleep(0.02)
    monkeypatch.setattr(telemetry, '_overdue_line', lambda *args: None)  # the line is written later
    telemetry._observe_oldest(None)
    assert request.error_logged  # a request ending before the line keeps its trace


def test_a_request_that_never_finishes_shows_in_the_gauges_and_writes_one_line(sink, capsys):
    telemetry, _spans, reader = sink
    telemetry._overdue = 0.01
    telemetry._slow_exclude = ['rrext_cprofile_*']  # the default RR_OTEL_SLOW_REQUEST_EXCLUDE
    request, _context = telemetry.request_scope(
        'rrext_execute', None, {'org_id': 'org-a', 'task_id': 'tok.chat_1', 'connection_id': '9'}
    )
    long_lived, _ = telemetry.request_scope('rrext_cprofile_start', None, {})  # excluded: long-lived
    time.sleep(0.02)

    def gauges():
        data = reader.get_metrics_data()
        return {
            m.name: {dict(p.attributes)['rpc.method']: p.value for p in m.data.data_points}
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name.startswith('rocketride.ws.requests.')
        }

    first = gauges()
    assert first['rocketride.ws.requests.in_flight'] == {'rrext_execute': 1}
    assert set(first['rocketride.ws.requests.oldest']) == {'rrext_execute'}
    gauges()  # a second collection: no second line
    lines = [json.loads(x) for x in capsys.readouterr().out.splitlines() if 'rocketride.request.overdue' in x]
    assert len(lines) == 1
    assert lines[0]['method'] == 'rrext_execute' and lines[0]['task_id'] == 'tok.chat_1'
    assert lines[0]['org_id'] == 'org-a' and lines[0]['trace_id'] == f'{request.trace_id:032x}'
    assert request.error_logged  # its trace is kept when it ends
    telemetry.request_done('rrext_execute', None, 0.02, None, request)
    assert gauges().get('rocketride.ws.requests.in_flight', {}).get('rrext_execute', 0) == 0


def test_only_known_safe_attributes_leave_a_dependency_span():
    """An allowlist: a database or user name, a header or a user agent never reach the trace store."""
    tracer = TracerProvider().get_tracer('x')
    span = tracer.start_span(
        'SELECT',
        attributes={
            'db.system.name': 'postgresql',
            'db.operation.name': 'SELECT',
            'server.address': 'db.internal',
            'db.user': 'billing_admin',
            'db.namespace': 'customers',
            'user_agent.original': 'python-requests/2.31',
            'http.request.header.authorization': ['Bearer x'],
        },
    )
    span.end()
    clean = dict(_deps.sanitized(span, logfmt.redact).attributes)
    assert clean == {'db.system.name': 'postgresql', 'db.operation.name': 'SELECT', 'server.address': 'db.internal'}


def test_retrying_missing_instrumentations_installs_no_recorder_twice(monkeypatch):
    """RR_OTEL_AUTO_INSTALL retries only what was missing: every call recorded once."""
    installs = []

    class Recorder:
        def __init__(self, on_call):
            pass

        def install(self):
            installs.append(1)
            return True

    monkeypatch.setattr(_deps, 'RECORDERS', [('db', 'json', Recorder)])
    monkeypatch.setattr(_deps, 'INSTRUMENTATIONS', [])
    monkeypatch.setattr(_deps, '_STATE', {})
    _deps.instrument(None, ['db'], lambda call: None)
    assert _deps.instrument_missing() == ([], [])
    assert installs == [1]


# ── Model server requests (ModelClient calls telemetry.model_server_call) ─────


@pytest.fixture
def model_client(monkeypatch):
    """A ModelClient without a server: its request is `request` (a coroutine function), run on a loop thread."""
    from ai import node as ai_node
    from ai.common.models.base import ModelClient

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(ai_node, 'server_loop', loop, raising=False)
    client = ModelClient.__new__(ModelClient)
    client._uri = 'ws://model-server:5590'
    try:
        yield client
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()
        telemetry_module._model_recorder = None


def test_a_model_server_request_is_recorded_in_the_pass_trace(model_client):
    from rocketride.core.dap_base import current_traceparent

    async def request(command, arguments, retry_on_error):
        return {'traceparent': current_traceparent()}

    model_client._send_command_async = request
    calls = []
    trace_id = int('4bf92f3577b34da6a3ce929d0e0e4736', 16)
    recorder = _deps.ModelRecorder(calls.append, lambda: trace_id)
    assert recorder.install()
    assert not recorder.install()  # once
    body = model_client.send_command('rrext_ms_inference', {})
    (call,) = calls
    assert call.metric == 'rpc.client.call.duration'
    assert call.name == 'model rrext_ms_inference'
    assert call.attributes == {
        'rpc.system.name': 'rocketride_dap',
        'rpc.method': 'rrext_ms_inference',
        'server.address': 'model-server',
    }
    assert call.error_type is None and call.end_ns >= call.start_ns
    # The request carried the pass's trace and the call's own span id
    assert body['traceparent'] == f'00-{trace_id:032x}-{call.span_id:016x}-01'

    async def failing(command, arguments, retry_on_error):
        raise ConnectionError('gone')

    model_client._send_command_async = failing
    with pytest.raises(ConnectionError):
        model_client.send_command('not a method!', {})
    assert calls[1].error_type == 'ConnectionError'
    assert calls[1].attributes['rpc.method'] == '_OTHER'

    recorder.uninstall()
    model_client._send_command_async = request
    model_client.send_command('rrext_ms_inference', {})
    assert len(calls) == 2  # off again


def test_a_model_request_out_of_a_pass_has_no_traceparent(model_client):
    from rocketride.core.dap_base import current_traceparent

    async def request(command, arguments, retry_on_error):
        return {'traceparent': current_traceparent()}

    model_client._send_command_async = request
    calls = []
    _deps.ModelRecorder(calls.append).install()
    body = model_client.send_command('rrext_ms_inference', {})
    assert calls[0].span_id == 0 and calls[0].error_type is None
    assert not body['traceparent']


def test_with_telemetry_off_a_model_request_is_left_alone(model_client):
    async def request(command, arguments, retry_on_error):
        return {'ok': True}

    model_client._send_command_async = request
    assert telemetry_module._model_recorder is None
    call = telemetry_module.model_server_call(model_client, 'rrext_ms_inference')
    assert call is telemetry_module._NO_MODEL_CALL
    coroutine = request('x', {}, True)
    assert call.traced(coroutine) is coroutine  # not wrapped
    coroutine.close()
    assert model_client.send_command('rrext_ms_inference', {}) == {'ok': True}


def test_the_model_flag_turns_the_model_recorder_on_and_off():
    try:
        _deps.instrument(None, [], lambda call: None)
        assert telemetry_module._model_recorder is None
        done, _skipped, recorders = _deps.instrument(None, ['model'], lambda call: None)
        assert 'ai.common.models' in done
        assert telemetry_module._model_recorder in recorders
    finally:
        for recorder in _deps._STATE.get('recorders', []):
            recorder.uninstall()
        _deps._STATE.clear()
    assert telemetry_module._model_recorder is None
