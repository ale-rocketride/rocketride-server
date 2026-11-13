"""
Per-call cost of the component telemetry in EaaS: ./engine bench_trace.py

One pass of a three-component pipeline, as the engine reports it (an enter and
a leave per component, each with the pass's trace id, new for every pass),
through the hook EaaS calls for each: the pass's bookkeeping and the component
duration histogram. Production settings: sample
ratio 0, so a pass that did not fail builds no span. Each round ends by
recording what was queued for the otel-deps thread, inside the time measured.
"""

import random
import statistics
import time

from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from ai.common.telemetry._otel import Telemetry

PASSES = 3000  # per measurement
ROUNDS = 7  # the median of these is reported
PIPELINE = {
    'components': [
        {'id': 'webhook_1', 'provider': 'webhook'},
        {'id': 'parse_1', 'provider': 'parse'},
        {'id': 'response_text_1', 'provider': 'response_text'},
    ]
}
COMPONENTS = [component['id'] for component in PIPELINE['components']]


class Discard(SpanExporter):
    def export(self, spans):
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


def main() -> None:
    impl = Telemetry(span_exporter=Discard(), metric_reader=InMemoryMetricReader(), sample_ratio=0.0)
    impl.task_event('task', {'event': 'apaevt_status_update', 'body': {'action': 'begin'}}, lambda: {})

    def one_pass():
        tid = f'{random.getrandbits(128) or 1:032x}'
        for component in COMPONENTS:
            impl.component_trace('task', 'enter', 0, component, time.time(), {'tid': tid}, PIPELINE)
        for component in reversed(COMPONENTS):
            impl.component_trace(
                'task', 'leave', 0, component, time.time(), {'tid': tid, 'result': 'continue'}, PIPELINE
            )

    for _ in range(300):
        one_pass()
    impl.flush_measurements()
    rounds = []
    for _ in range(ROUNDS):
        started = time.perf_counter()
        for _ in range(PASSES):
            one_pass()
        impl.flush_measurements()
        rounds.append((time.perf_counter() - started) / PASSES * 1e6)
    per_pass = statistics.median(rounds)
    calls = 2 * len(COMPONENTS)
    print(
        f'one pass, {len(COMPONENTS)} components ({calls} enter/leave): {per_pass:6.1f} us, {per_pass / calls:5.1f} us a call'
    )
    impl.shutdown(1)


main()
