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
Unit tests for the telemetry facade (ai.common.telemetry): what EaaS and every
module call, whether telemetry is on or off.

The guarantees under test: off means no-op and no SDK import; on never blocks
startup or raises; a missing SDK keeps it off (or, opted in, installs in the
background); shutdown is bounded and final; declared metrics are safe to use
before, during and after.
"""

import logging
import os
import sys
import threading
import time

import pytest

from ai.common import telemetry


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv('RR_OTEL_ENABLED', raising=False)
    monkeypatch.delenv('OTEL_SDK_DISABLED', raising=False)
    monkeypatch.delenv('RR_OTEL_AUTO_INSTALL', raising=False)
    monkeypatch.delenv('OTEL_EXPORTER_OTLP_PROTOCOL', raising=False)
    telemetry._reset_for_tests()
    yield
    telemetry.shutdown()
    telemetry._reset_for_tests()


class FakeImpl:
    """Stands in for the SDK-backed implementation."""

    endpoint_display = 'http://collector:4318'

    def __init__(self):
        self.calls = []
        self.instruments = []

    def guard(self, hook, fn, *args):
        try:
            fn(*args)
        except Exception:
            self.calls.append(('hook_error', hook))

    def task_event(self, *args):
        self.calls.append(('task_event', args))

    def make_instrument(self, kind, name, unit, description):
        impl = self

        class Instrument:
            def add(self, value, attributes):
                impl.calls.append(('add', name, value, attributes))

            def record(self, value, attributes):
                impl.calls.append(('record', name, value, attributes))

        return Instrument()

    def clean_attributes(self, name, keys, values):
        return {keys[k]: str(v) for k, v in values.items() if k in keys and v is not None}

    def count_hook_error(self, name):
        self.calls.append(('hook_error', name))

    def shutdown(self, timeout):
        self.calls.append(('shutdown', timeout))


def test_off_by_default_is_a_noop(monkeypatch):
    monkeypatch.setattr(telemetry, '_start', lambda *_: pytest.fail('must not start'))
    telemetry.init('1.0')
    telemetry.task_event('t', {}, lambda: {})
    telemetry.request_done('execute', None, 0.1)
    counter = telemetry.Counter('rocketride.test.off', '{x}', 'test', attributes={'outcome': 'k'})
    counter.add(outcome='ok')  # no-op, no error
    assert telemetry._impl is None


def test_otel_sdk_disabled_wins(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setenv('OTEL_SDK_DISABLED', 'true')
    assert not telemetry.is_enabled_by_environment()


def test_start_failure_leaves_it_off(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setattr(telemetry, '_sdk_installed', lambda: True)

    class Broken:
        @staticmethod
        def from_environment(version):
            raise RuntimeError('bad config')

    monkeypatch.setitem(sys.modules, 'ai.common.telemetry._otel', type(sys)('fake_otel'))
    sys.modules['ai.common.telemetry._otel'].Telemetry = Broken
    telemetry.init('1.0')
    assert telemetry._impl is None
    telemetry.task_event('t', {}, lambda: {})  # still a no-op


def test_missing_sdk_stays_off_without_installing(monkeypatch):
    # depends() would hold the engine-wide install lock: never by default
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setattr(telemetry, '_sdk_installed', lambda: False)
    warnings = []
    monkeypatch.setattr(telemetry, 'warning', warnings.append)
    monkeypatch.setattr(telemetry, '_install_then_start', lambda *_: pytest.fail('installed without opt-in'))
    telemetry.init('1.0')
    assert telemetry._impl is None and not telemetry._starting
    assert len(warnings) == 1 and 'install_sdk()' in warnings[0]


def test_missing_sdk_installs_in_background_and_never_blocks(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setenv('RR_OTEL_AUTO_INSTALL', 'true')
    monkeypatch.setattr(telemetry, '_sdk_installed', lambda: False)
    release = threading.Event()
    entered = threading.Event()
    returned = threading.Event()
    finished = threading.Event()

    def slow_install(*_):
        entered.set()
        release.wait(5)  # an index or lock that takes long
        finished.set()

    def run_init():
        telemetry.init('1.0')
        returned.set()

    monkeypatch.setattr(telemetry, '_install_then_start', slow_install)
    caller = threading.Thread(target=run_init, daemon=True)
    try:
        caller.start()
        assert entered.wait(5) and returned.wait(5)
        # startup did not wait for the install
        assert not finished.is_set()
    finally:
        release.set()
        caller.join(5)
    assert finished.wait(5)


def test_failed_install_leaves_it_off(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    fake_depends = type(sys)('depends')

    def load_depends(_):
        raise OSError('no network')

    fake_depends.load_depends = load_depends
    monkeypatch.setitem(sys.modules, 'depends', fake_depends)
    with telemetry._state_lock:
        telemetry._starting = True  # as init() leaves it for the installer
    telemetry._install_then_start('1.0')
    assert telemetry._impl is None and not telemetry._starting


def test_install_sdk_installs_the_requirements_next_to_the_package(monkeypatch):
    installed = []
    fake_depends = type(sys)('depends')
    fake_depends.load_depends = lambda current_file: installed.append(current_file)
    monkeypatch.setitem(sys.modules, 'depends', fake_depends)
    telemetry.install_sdk()
    assert installed == [telemetry.__file__]
    assert telemetry.SDK_REQUIREMENTS == os.path.join(
        os.path.dirname(os.path.realpath(telemetry.__file__)), 'requirements.txt'
    )
    assert os.path.isfile(telemetry.SDK_REQUIREMENTS)


def test_declared_metrics_bind_when_telemetry_starts(monkeypatch):
    counter = telemetry.Counter(
        'rocketride.test.late', '{x}', 'test', attributes={'outcome': 'rocketride.test.outcome'}
    )
    counter.add(outcome='before')  # off: dropped
    impl = FakeImpl()
    for instrument in list(telemetry._declared):
        instrument._bind(impl)
    telemetry._impl = impl
    counter.add(outcome='after', unknown='dropped')
    assert ('add', 'rocketride.test.late', 1, {'rocketride.test.outcome': 'after'}) in impl.calls


def test_metric_errors_never_reach_the_caller():
    impl = FakeImpl()
    histogram = telemetry.Histogram('rocketride.test.boom', 's', 'test')
    histogram._bind(impl)

    class Raising:
        def record(self, *_):
            raise RuntimeError('sdk failure')

    histogram._instrument = Raising()
    histogram.record(1.0)  # must not raise
    assert ('hook_error', 'rocketride.test.boom') in impl.calls


def test_timed_records_and_reraises():
    impl = FakeImpl()
    histogram = telemetry.Histogram(
        'rocketride.test.timed', 's', 'test', attributes={'error': 'error.type', 'outcome': 'o'}
    )
    histogram._bind(impl)
    with telemetry.timed(histogram, outcome='ok'):
        pass
    with pytest.raises(ValueError):
        with telemetry.timed(histogram, outcome='x'):
            raise ValueError('boom')
    records = [c for c in impl.calls if c[0] == 'record']
    assert records[0][3] == {'o': 'ok'}
    assert records[1][3] == {'o': 'x', 'error.type': 'ValueError'}


def test_shutdown_is_bounded(monkeypatch):
    monkeypatch.setenv('RR_OTEL_SHUTDOWN_TIMEOUT', '0.3')
    impl = FakeImpl()
    entered = threading.Event()
    release = threading.Event()

    def blocked(timeout):  # collector unreachable
        entered.set()
        release.wait(10)

    impl.shutdown = blocked
    telemetry._impl = impl
    try:
        before = time.monotonic()
        telemetry.shutdown()
        assert time.monotonic() - before < 1.0
        assert entered.is_set() and not release.is_set()  # cleanup started and is still blocked
        telemetry.shutdown()  # twice is safe
    finally:
        release.set()


def test_hooks_after_shutdown_are_noops():
    impl = FakeImpl()
    telemetry._impl = impl
    telemetry.shutdown()
    telemetry.task_event('t', {}, lambda: {})
    assert not [c for c in impl.calls if c[0] == 'task_event']


def test_org_id_of_never_raises():
    class Account:
        organization = {'id': 'org-1'}

    class Odd:
        @property
        def organization(self):
            raise RuntimeError('broken')

    assert telemetry.org_id_of(Account()) == 'org-1'
    assert telemetry.org_id_of(Odd()) is None
    assert telemetry.org_id_of(None) is None


def test_sdk_logs_are_rate_limited():
    limiter = telemetry._RateLimit(interval=60)
    record = logging.LogRecord('opentelemetry.exporter', logging.WARNING, '', 0, 'Failed to export batch', None, None)
    assert limiter.filter(record)
    for _ in range(100):
        assert not limiter.filter(
            logging.LogRecord('opentelemetry.exporter', logging.WARNING, '', 0, 'Failed to export batch', None, None)
        )
    limiter._last.clear()
    again = logging.LogRecord('opentelemetry.exporter', logging.WARNING, '', 0, 'Failed to export batch', None, None)
    assert limiter.filter(again) and '100 similar messages suppressed' in again.msg


def test_the_run_process_gets_pass_ids_and_relay_only_while_on(monkeypatch):
    assert telemetry.child_arguments() == []  # off: the engine and the run's process as before
    telemetry._impl = FakeImpl()
    monkeypatch.delenv('RR_OTEL_INSTRUMENT', raising=False)
    arguments = telemetry.child_arguments()
    assert arguments[0] == '--otel_pass_ids' and arguments[1].startswith('--otel_relay=')
    monkeypatch.setenv('RR_OTEL_INSTRUMENT', 'none')
    assert telemetry.child_arguments() == ['--otel_pass_ids']  # pass ids without the relay


def test_otel_variables_never_reach_task_subprocesses():
    from ai.modules.task.task_engine import filter_subprocess_env

    env = filter_subprocess_env({'PATH': '/bin', 'OTEL_EXPORTER_OTLP_ENDPOINT': 'http://x', 'RR_OTEL_ENABLED': 'true'})
    assert not any(key.startswith('OTEL_') for key in env)


def test_shutdown_during_install_keeps_it_off(monkeypatch):
    # EaaS exits while the SDK is still installing: the late start must not
    # leave exporter threads running in an exiting process
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    stopped = []

    class Impl(FakeImpl):
        def shutdown(self, timeout):
            stopped.append(timeout)

    fake = type(sys)('fake_otel')
    fake.Telemetry = type('T', (), {'from_environment': staticmethod(lambda version: Impl())})
    monkeypatch.setitem(sys.modules, 'ai.common.telemetry._otel', fake)
    with telemetry._state_lock:
        telemetry._starting = True
    telemetry.shutdown()
    telemetry._start('1.0')  # the install finishes after shutdown
    assert telemetry._impl is None and stopped


def test_connection_source_registered_before_start_is_bound_at_start(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    seen = []

    class Impl(FakeImpl):
        def observe_connections(self, count):
            seen.append(count())

    fake = type(sys)('fake_otel')
    fake.Telemetry = type('T', (), {'from_environment': staticmethod(lambda version: Impl())})
    monkeypatch.setitem(sys.modules, 'ai.common.telemetry._otel', fake)
    telemetry.observe_connections(lambda: 7)  # EaaS registers it before telemetry is up
    telemetry._start('1.0')
    assert seen == [7]


def test_off_never_imports_the_sdk(monkeypatch):
    monkeypatch.delitem(sys.modules, 'ai.common.telemetry._otel', raising=False)
    telemetry.init('1.0')
    telemetry.task_event('t', {'event': 'apaevt_task', 'body': {'action': 'begin'}}, lambda: {})
    telemetry.request_done('execute', None, 0.1)
    assert 'ai.common.telemetry._otel' not in sys.modules


def test_grpc_protocol_is_reported_once(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_PROTOCOL', 'grpc')
    monkeypatch.setattr(telemetry, '_sdk_installed', lambda: False)
    warnings = []
    monkeypatch.setattr(telemetry, 'warning', warnings.append)
    telemetry.init('1.0')
    assert len([w for w in warnings if 'grpc' in w]) == 1


def test_init_after_shutdown_stays_off(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    monkeypatch.setattr(telemetry, '_start', lambda *_: pytest.fail('started after shutdown'))
    telemetry.shutdown()
    telemetry.init('1.0')
    assert telemetry._impl is None and not telemetry._starting


@pytest.mark.parametrize('value', ['inf', 'nan', '-1'])
def test_an_unbounded_shutdown_timeout_takes_the_default(monkeypatch, value):
    monkeypatch.setenv('RR_OTEL_SHUTDOWN_TIMEOUT', value)
    impl = FakeImpl()
    telemetry._impl = impl
    telemetry.shutdown()  # must not raise
    assert ('shutdown', 3.0) in impl.calls


def test_shutdown_while_binding_leaves_the_metrics_unbound(monkeypatch):
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    histogram = telemetry.Histogram('rocketride.test.bindrace', 's', 'test')
    instrumented = []

    class Impl(FakeImpl):
        def make_instrument(self, *args):
            # shutdown runs after _start published the implementation, before binding ended
            if not instrumented:
                instrumented.append('shutdown')
                telemetry.shutdown()
            return super().make_instrument(*args)

        def instrument_dependencies(self):
            instrumented.append('deps')
            return [], []

    fake = type(sys)('fake_otel')
    fake.Telemetry = type('T', (), {'from_environment': staticmethod(lambda version: Impl())})
    monkeypatch.setitem(sys.modules, 'ai.common.telemetry._otel', fake)
    telemetry._start('1.0')
    assert telemetry._impl is None
    assert histogram._instrument is None
    assert 'deps' not in instrumented


def test_value_is_never_taken_from_the_attributes():
    impl = FakeImpl()
    histogram = telemetry.Histogram('rocketride.test.value', 's', 'test', attributes={'value': 'v'})
    histogram._bind(impl)
    with pytest.raises(KeyError):
        with telemetry.timed(histogram) as attrs:
            attrs['value'] = 'x'
            raise KeyError('body')  # the body's own exception propagates
    counter = telemetry.Counter('rocketride.test.cvalue', '{x}', 'test', attributes={'value': 'v'})
    counter._bind(impl)
    counter.add(2, value='y')
    assert ('add', 'rocketride.test.cvalue', 2, {'v': 'y'}) in impl.calls


def test_rate_limit_eviction_forgets_suppressed_counts():
    limiter = telemetry._RateLimit(interval=60)
    for i in range(502):
        message = f'message {i}'
        for _ in range(2):
            limiter.filter(logging.LogRecord('opentelemetry', logging.WARNING, '', 0, message, None, None))
    assert len(limiter._suppressed) <= len(limiter._last) + 1
