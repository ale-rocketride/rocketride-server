"""
Per-call cost of the dependency telemetry in EaaS: ./engine bench_deps.py

Telemetry is on in both columns (as before the dependency telemetry): "before"
is a WebSocket request and a database call without it, "after" with it (the
request's ids and context, the SQLAlchemy recorder, the measurement queue).
sqlite in memory is the worst case: the database adds nothing of its own, so
the difference is all telemetry. Spans go to an exporter that drops them. Each
round ends by recording what was queued for the otel-deps thread, inside the
time measured: the figures are the whole CPU cost, wherever it is spent.
"""

import asyncio
import statistics
import time

import sqlalchemy
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from ai.common import telemetry
from ai.common.telemetry import _deps
from ai.common.telemetry._otel import Telemetry

CALLS = 3000  # per measurement
ROUNDS = 9  # the median of these is reported


class Discard(SpanExporter):
    def export(self, spans):
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


def median_us(fn) -> float:
    """Median time of one call of fn, in microseconds, after a warm-up (queued measurements included)."""
    for _ in range(300):
        fn()
    telemetry._impl.flush_measurements()
    rounds = []
    for _ in range(ROUNDS):
        started = time.perf_counter()
        for _ in range(CALLS):
            fn()
        telemetry._impl.flush_measurements()
        rounds.append((time.perf_counter() - started) / CALLS * 1e6)
    return statistics.median(rounds)


def main() -> None:
    impl = Telemetry(span_exporter=Discard(), metric_reader=InMemoryMetricReader(), slow_request_seconds=2.0)
    telemetry._impl = impl
    recorder = _deps.SqlRecorder(impl._dependency_call_hook)
    loop = asyncio.new_event_loop()
    connection = sqlalchemy.create_engine('sqlite://').connect()
    statement = sqlalchemy.text('SELECT 1')

    def query(recorded: bool):
        def run():
            recorder.install() if recorded else recorder.uninstall()
            return connection.execute(statement).fetchall()

        return run

    def request(work, scoped: bool):
        async def handle():
            started = time.monotonic()
            if scoped:
                with telemetry.request_scope('rrext_bench') as current:
                    work()
                telemetry.request_done('rrext_bench', None, time.monotonic() - started, None, current)
            else:
                work()
                telemetry.request_done('rrext_bench', None, time.monotonic() - started, None)

        return lambda: loop.run_until_complete(handle())

    plain, recorded = query(False), query(True)
    rows = [
        ('Database call outside a request', median_us(plain), median_us(recorded)),
        (
            'WebSocket request, empty handler',
            median_us(request(lambda: None, False)),
            median_us(request(lambda: None, True)),
        ),
        (
            'WebSocket request with 1 database call',
            median_us(request(plain, False)),
            median_us(request(recorded, True)),
        ),
    ]
    print(f'{"case (sqlite in memory: the worst case)":42} {"before us":>10} {"after us":>9} {"added us":>9}')
    for name, before, after in rows:
        print(f'{name:42} {before:10.1f} {after:9.1f} {after - before:9.1f}')
    recorder.uninstall()
    impl.shutdown(1)


main()
