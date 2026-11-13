"""
Per-call cost of the relay in a run's process: ./engine bench_relay.py

A node handler call with and without the relay's wrapping (the scope: pipe,
component and pass), and one relayed dependency call (a record queued for the
batched '>SPN*' line; the write itself is not measured).
"""

import statistics
import time

from ai.common.telemetry import _deps, relay

CALLS = 20000  # per measurement
ROUNDS = 7  # the median of these is reported


def median_us(fn) -> float:
    """Median time of one call of fn, in microseconds, after a warm-up."""
    for _ in range(500):
        fn()
    rounds = []
    for _ in range(ROUNDS):
        started = time.perf_counter()
        for _ in range(CALLS):
            fn()
        rounds.append((time.perf_counter() - started) / CALLS * 1e6)
    return statistics.median(rounds)


class PipeType:
    id = 'llm_1'


class Instance:
    """What the engine gives a node: its pipe, its component and the pass."""

    pipeId = 0
    pipeType = PipeType()
    passTraceId = '4bf92f3577b34da6a3ce929d0e0e4736'


def main() -> None:
    class Base:
        instance = None

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)

        def writeText(self, text):
            pass

    class Plain(Base):
        def __init__(self):
            self.instance = Instance()

        def writeText(self, text):
            return text

    plain = Plain()
    relay._wrap_subclasses(Base)  # classes defined from here on are wrapped, as in a run's process

    class Scoped(Base):
        def __init__(self):
            self.instance = Instance()

        def writeText(self, text):
            return text

    scoped = Scoped()
    before, after = median_us(lambda: plain.writeText('x')), median_us(lambda: scoped.writeText('x'))

    relay._write = lambda line: None  # the line is not written anywhere
    now = time.time_ns()
    attributes = {'http.request.method': 'POST', 'server.address': 'api.openai.com', 'http.response.status_code': 200}
    call = _deps.Call('http.client.request.duration', 'POST', now, now + 1000, attributes, None)
    relayed = median_us(lambda: relay.emit(call))

    print(f'{"node handler call":40} before {before:6.2f} us  after {after:6.2f} us  added {after - before:5.2f} us')
    print(f'{"one relayed dependency call":40} {relayed:6.2f} us')


main()
