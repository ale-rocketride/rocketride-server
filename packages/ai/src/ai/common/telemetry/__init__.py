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
OpenTelemetry for EaaS: task, node and request telemetry, off by default.

Task, pipeline and node telemetry is derived from what EaaS already produces —
the task events that fan out to the run log and to monitoring clients, the
component enter/leave traces the engine emits for every run, the run outcome
the run log records, and each WebSocket request's handler result. The engine
and the nodes emit nothing new: every node, including one added tomorrow, is
covered with no code of its own.

Any other module (the SaaS extension, a package) declares its own business
metrics with the classes below, at import time, in one line each:

    from ai.common import telemetry

    LOGINS = telemetry.Counter(
        'rocketride.auth.logins', '{login}', 'Browser sign-ins',
        attributes={'outcome': 'rocketride.auth.outcome', 'org': 'rocketride.org.id'},
    )

    LOGINS.add(outcome='success', org=org_id)

The declared attribute keys are the only ones exported; values are capped in
number (cardinality) and length. A metric is a no-op until telemetry starts and
never raises.

Configuration (environment, or the engine's .env):

    RR_OTEL_ENABLED                 'true' turns telemetry on (it is off otherwise)
    OTEL_SDK_DISABLED               'true' keeps it off, whatever RR_OTEL_ENABLED says
    OTEL_EXPORTER_OTLP_*            standard OTLP/HTTP settings: endpoint, headers,
                                    timeout, compression, certificates
    OTEL_SERVICE_NAME               default 'rocketride-eaas'
    OTEL_RESOURCE_ATTRIBUTES        e.g. deployment.environment.name=prd
    OTEL_METRIC_EXPORT_INTERVAL     standard (default 60000 ms)
    OTEL_BSP_*                      standard span batching
    RR_OTEL_TRACES_SAMPLE_RATIO     share of traces that did not fail kept: pipeline passes,
                                    runs and traced requests (default 1.0; 0 = errors only);
                                    failed ones, and slow passes, are always kept
    RR_OTEL_TRACES_SLOW_SECONDS     a pipeline pass at least this long is kept (default 60;
                                    RR_OTEL_SLOW_RUN_SECONDS, RR_OTEL_SLOW_REQUEST_SECONDS and
                                    RR_OTEL_SLOW_TRACES_PER_MINUTE: see the docs)
    RR_OTEL_INSTRUMENT              dependency calls traced and measured (EaaS's and, relayed,
                                    the nodes'): 'db,redis,http,aws,grpc,model' (default), 'none'
    RR_OTEL_COMPONENT_SPANS         component spans per pipeline trace (default 200, at most 1000)
    RR_OTEL_ORG_ATTRIBUTE           'true' puts the customer org id on metrics (default off:
                                    up to RR_OTEL_ORG_CARDINALITY orgs times each metric's
                                    series); spans and error lines always carry it
    RR_OTEL_ORG_CARDINALITY         distinct org ids this process attributes, across all
                                    metrics (default 2000; later ones are '_other')
    RR_OTEL_AUTO_INSTALL            'true' installs a missing SDK at startup (requirements.txt)
                                    on a background thread. Off by default: depends() holds
                                    the engine-wide install lock meanwhile, so other imports
                                    and task starts would wait. Images ship the SDK instead.
    RR_OTEL_SHUTDOWN_TIMEOUT        seconds shutdown may wait to flush (default 3)
    RR_OTEL_STALE_CALL_SECONDS      a node call open this long is taken as a lost leave and
                                    its pass is exported unfinished (default 0: off; a nested
                                    pass longer than this would be split)

Not supported (the exporter is OTLP/HTTP and sampling is RocketRide's own):
OTEL_EXPORTER_OTLP_PROTOCOL=grpc (a warning; HTTP is used), OTEL_TRACES_SAMPLER
and OTEL_{TRACES,METRICS,LOGS}_EXPORTER.

Task subprocesses never see OTEL_* (filter_subprocess_env allows a fixed list,
unless an operator adds them to RR_SUBPROCESS_ENV), so nodes do not start
exporting because the engine does.

Availability guarantees, so telemetry can never slow or stop a customer's work:
    - when off, every hook and metric returns at once and the SDK is never imported;
    - when on and the SDK is missing, telemetry stays off with one warning that
      says how to install it (or, with RR_OTEL_AUTO_INSTALL, it is installed on
      a background thread and turns on when ready);
    - pipeline traces are built on their own thread, from a bounded queue;
    - hooks never await and never touch the network; export runs on SDK
      threads with bounded queues that drop on overflow;
    - while the collector is unreachable, export failures are logged at most once
      a minute per message, with a count of what was suppressed;
    - shutdown waits at most RR_OTEL_SHUTDOWN_TIMEOUT seconds.
"""

import contextlib
import importlib
import importlib.util
import logging
import math
import os
import threading
import time
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from rocketlib import debug, warning

# The running implementation (ai.common.telemetry._otel.Telemetry), or None
_impl = None
_state_lock = threading.Lock()
_starting = False
# Set by shutdown(): a start still in progress (SDK install) must not turn telemetry on after it
_closed = False
# The connection count source, kept so a later start still gets it
_connections: Optional[Callable[[], int]] = None

# The SDK and the instrumentations, installed by install_sdk() only when
# telemetry is used. The engine-wide constraints compile (depends.py) resolves
# this file with every node's; nothing installs it while telemetry is off.
SDK_REQUIREMENTS = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')

# Every metric any module declared (Counter/Histogram), bound when telemetry starts
_declared: List['_Instrument'] = []


def _truthy(value: Optional[str]) -> bool:
    return (value or '').strip().lower() in ('1', 'true', 'yes', 'on')


def install_sdk() -> None:
    """Install the OpenTelemetry SDK and the instrumentations (SDK_REQUIREMENTS) with depends()."""
    from depends import load_depends  # type: ignore

    load_depends(__file__)
    importlib.invalidate_caches()


def is_enabled_by_environment(environ: Optional[Mapping[str, str]] = None) -> bool:
    """RR_OTEL_ENABLED is set and OTEL_SDK_DISABLED is not."""
    environ = os.environ if environ is None else environ
    return _truthy(environ.get('RR_OTEL_ENABLED')) and not _truthy(environ.get('OTEL_SDK_DISABLED'))


def _sdk_installed() -> bool:
    try:
        return (
            importlib.util.find_spec('opentelemetry.sdk') is not None
            and importlib.util.find_spec('opentelemetry.exporter.otlp.proto.http') is not None
        )
    except Exception:
        return False


def init(service_version: str = '') -> None:
    """Start telemetry when enabled. Never raises and never blocks on the network.

    Args:
        service_version: The engine version, exported as service.version.
    """
    global _starting

    if not is_enabled_by_environment():
        return

    with _state_lock:
        if _impl is not None or _starting or _closed:
            return
        _starting = True

    _quiet_sdk_logging()

    _check_protocol()

    if _sdk_installed():
        _start(service_version)
        return

    if not _truthy(os.environ.get('RR_OTEL_AUTO_INSTALL')):
        warning(
            'OpenTelemetry stays off: the SDK is not installed. Install it with '
            '`./engine -c "from ai.common.telemetry import install_sdk; install_sdk()"` '
            '(or set RR_OTEL_AUTO_INSTALL=true)'
        )
        with _state_lock:
            _starting = False
        return

    # Install off the event loop: depends() can wait on a lock or a slow index,
    # and EaaS must start and serve meanwhile.
    threading.Thread(target=_install_then_start, args=(service_version,), name='otel-install', daemon=True).start()


def _check_protocol() -> None:
    """The exporter is OTLP/HTTP: say so once when the operator asked for gRPC."""
    for name in (
        'OTEL_EXPORTER_OTLP_PROTOCOL',
        'OTEL_EXPORTER_OTLP_TRACES_PROTOCOL',
        'OTEL_EXPORTER_OTLP_METRICS_PROTOCOL',
    ):
        if (os.environ.get(name) or '').strip().lower() == 'grpc':
            warning(f'{name}=grpc is not supported; OpenTelemetry exports over OTLP/HTTP (port 4318)')
            return


def _install_then_start(service_version: str) -> None:
    global _starting
    try:
        install_sdk()
    except Exception as e:
        warning(f'OpenTelemetry stays off: the SDK could not be installed ({type(e).__name__}: {e})')
        with _state_lock:
            _starting = False
        return
    _start(service_version)


def _start(service_version: str) -> None:
    global _impl, _starting
    try:
        from ._otel import Telemetry

        impl = Telemetry.from_environment(service_version)
    except Exception as e:
        warning(f'OpenTelemetry stays off: {type(e).__name__}: {e}')
        with _state_lock:
            _starting = False
        return

    with _state_lock:
        _starting = False
        closed = _closed
        if not closed:
            _impl = impl
        declared = list(_declared)
        connections = _connections
    if closed:
        # EaaS shut down while the SDK was installing: never start in an exiting process
        impl.shutdown(0.1)
        return
    for instrument in declared:
        instrument._bind(impl)
    with _state_lock:
        closed = _closed
    if closed:
        # shutdown ran while binding: it may have cleared the instruments first
        for instrument in declared:
            instrument._instrument = None
        return
    if connections is not None:
        impl.guard('observe_connections', impl.observe_connections, connections)
    debug(f'OpenTelemetry on, exporting to {impl.endpoint_display}')
    _instrument_dependencies(impl)


def _instrument_dependencies(impl: Any) -> None:
    """Trace and measure EaaS's own dependency calls (RR_OTEL_INSTRUMENT); never raises."""
    try:
        done, skipped = impl.instrument_dependencies()
        missing = [reason for reason in skipped if reason.endswith('not installed')]
        if missing and _truthy(os.environ.get('RR_OTEL_AUTO_INSTALL')):
            threading.Thread(target=_install_instrumentation, name='otel-install', daemon=True).start()
            skipped = [reason for reason in skipped if reason not in missing]
        if done:
            debug(f'OpenTelemetry traces dependency calls: {", ".join(done)}')
        for reason in skipped:
            warning(f'OpenTelemetry does not trace {reason}')
    except Exception as e:
        warning(f'OpenTelemetry does not trace dependency calls: {type(e).__name__}: {e}')


def _install_instrumentation() -> None:
    try:
        from . import _deps

        _deps.install()
        # Only what was missing: instrument() again would install every recorder twice
        done, skipped = _deps.instrument_missing()
        if done:
            debug(f'OpenTelemetry traces dependency calls: {", ".join(done)}')
        for reason in skipped:
            warning(f'OpenTelemetry does not trace {reason}')
    except Exception as e:
        warning(f'OpenTelemetry does not trace dependency calls: {type(e).__name__}: {e}')


def shutdown() -> None:
    """Flush and stop, waiting at most RR_OTEL_SHUTDOWN_TIMEOUT seconds (default 3).

    The flush runs on a daemon thread, so an unreachable collector cannot hold
    up EaaS's exit; what is not exported by then is dropped.
    """
    global _impl, _closed

    with _state_lock:
        impl, _impl = _impl, None
        _closed = True
    if impl is None:
        return
    for instrument in list(_declared):
        instrument._instrument = None

    try:
        timeout = float(os.environ.get('RR_OTEL_SHUTDOWN_TIMEOUT', '3'))
    except ValueError:
        timeout = 3.0
    if not math.isfinite(timeout) or timeout < 0:
        timeout = 3.0
    worker = threading.Thread(target=impl.shutdown, args=(timeout,), name='otel-shutdown', daemon=True)
    worker.start()
    worker.join(timeout)


# ── Hooks called by EaaS (a no-op unless on) ───────────────────────────────────


def task_event(task_id: str, message: Dict[str, Any], identity: Callable[[], Mapping[str, Any]]) -> None:
    """A task event about to fan out to clients (TaskEngine._forward_task_event).

    `identity` returns the run's {'org_id', 'run_kind'}; it is called only when needed.
    """
    impl = _impl
    if impl is not None:
        impl.guard('task_event', impl.task_event, task_id, message, identity)


def component_trace(
    task_id: str,
    op: str,
    pipe_index: Any,
    component_id: str,
    event_time: Optional[float],
    trace: Any,
    pipeline: Optional[Dict[str, Any]],
) -> None:
    """A component enter/leave from the engine, seen at every trace level."""
    impl = _impl
    if impl is not None:
        impl.guard(
            'component_trace', impl.component_trace, task_id, op, pipe_index, component_id, event_time, trace, pipeline
        )


def task_restarted(task_id: str) -> None:
    """The task restarted in place: same run, new subprocess and pipeline."""
    impl = _impl
    if impl is not None:
        impl.guard('task_restarted', impl.task_restarted, task_id)


def task_ended(
    task_id: str,
    identity: Mapping[str, Any],
    outcome: str,
    stop_reason: Optional[str],
    error_type: Optional[str],
    error_code: Optional[str] = None,
    cause: Optional[Mapping[str, str]] = None,
    exit_cause: Optional[Mapping[str, Any]] = None,
) -> None:
    """A run ended with the outcome the run log records (classify_run_outcome).

    error_code is the engine error code it exited with (e.g. 'InvalidSchema'):
    on the run's span and run-ended log line, and on rocketride.task.ended as a
    bounded label (the engine's error codes are a fixed set). cause is the
    run's first error from its error log ({'message', 'location', 'type'},
    redacted): on the span of a run that ended in error, never a metric label.
    exit_cause is how a signal ended its process ({'signal', 'oom_suspected'}):
    on the span, the line and rocketride.task.ended.
    """
    impl = _impl
    if impl is not None:
        impl.guard(
            'task_ended',
            impl.task_ended,
            task_id,
            identity,
            outcome,
            stop_reason,
            error_type,
            error_code,
            cause,
            exit_cause,
        )


@contextlib.contextmanager
def request_scope(method: str, traceparent: Any = None, identity: Optional[Mapping[str, Any]] = None) -> Iterator[Any]:
    """While a client WebSocket request is handled: its trace is current (yields None when off).

    Pass what it yields to request_done. The handler's dependency calls belong
    to the request and follow its keep decision.
    """
    impl = _impl
    request = token = request_token = None
    if impl is not None:
        try:
            from opentelemetry import context

            from . import _deps

            request, current = impl.request_scope(method, traceparent, identity)
            token = context.attach(current)
            request_token = _deps.set_request(request)
        except Exception:
            impl.count_hook_error('request_scope')
    try:
        yield request
    finally:
        if token is not None or request_token is not None:
            try:
                from opentelemetry import context

                from . import _deps

                if request_token is not None:
                    _deps.reset_request(request_token)
                if token is not None:
                    context.detach(token)
            except Exception:
                impl.count_hook_error('request_scope')


def request_done(
    method: str,
    error_type: Optional[str],
    duration_s: float,
    traceparent: Any = None,
    span: Any = None,
    identity: Optional[Mapping[str, Any]] = None,
) -> None:
    """A client WebSocket request was handled (error_type None means success).

    `span` is what request_scope yielded: the request's span is built here when
    it is kept. Without it, a caller's sampled W3C `traceparent` still gets the
    request as a child span (under the same rule). `identity` (DAPConn.request_identity):
    org and client kind on the metric (bounded), connection and run on the span, which
    links to the run's trace.
    """
    impl = _impl
    if impl is not None:
        impl.guard('request_done', impl.request_done, method, error_type, duration_s, traceparent, span, identity)


def child_arguments() -> List[str]:
    """
    The run's process arguments while telemetry is on, [] when off: the
    engine's pass trace ids (--otel_pass_ids: 'tid' on its enter/leave events,
    passTraceId for the nodes) and the relay of the nodes' dependency calls
    (--otel_relay=<flags>, relay.py), unless RR_OTEL_INSTRUMENT turns it off.
    """
    if _impl is None:
        return []
    from . import _deps

    flags, _ = _deps.parse_flags(os.environ.get('RR_OTEL_INSTRUMENT'))
    return ['--otel_pass_ids'] + ([f'--otel_relay={",".join(flags)}'] if flags else [])


def dependency_relayed(task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
    """Dependency calls a task's nodes made, as its subprocess relayed them ({'calls': [...]}, from '>SPN*'); never forwarded to clients."""
    impl = _impl
    if impl is not None:
        impl.guard('dependency_relayed', impl.dependency_relayed, task_id, body, pipeline)


def error_logged() -> None:
    """An ERROR log line was written with the current trace id: keep that trace."""
    impl = _impl
    if impl is not None:
        impl.guard('error_logged', impl.error_logged)


def run_detached(fn: Callable, *args: Any) -> Any:
    """Call fn with no current trace or request: what it schedules captures that detached context.

    For example run_detached(asyncio.ensure_future, coro) or run_detached(loop.call_later, delay, callback).
    """
    context = detached_context()
    return context.run(fn, *args) if context is not None else fn(*args)


def detached_context() -> Any:
    """A context with no current span, for asyncio.create_task(..., context=) of a long-lived task.

    A task spawned while a request is handled would otherwise inherit the
    request's span, and its dependency calls, long after, would join that
    request's trace. None when off (create_task then copies the context as usual).
    """
    impl = _impl
    if impl is None:
        return None
    try:
        return impl.detached_context()
    except Exception:
        impl.count_hook_error('detached_context')
        return None


def observe_connections(count: Callable[[], int]) -> None:
    """Register the source of the open client connection count."""
    global _connections
    _connections = count
    impl = _impl
    if impl is not None:
        impl.guard('observe_connections', impl.observe_connections, count)


def error_log_trace(task_id: str) -> str:
    """The trace id to put on an error log line about this run ('' when off or unknown).

    Naming the trace keeps it: a trace id in a log always leads to a trace.
    """
    impl = _impl
    if impl is None:
        return ''
    try:
        return impl.error_log_trace(task_id)
    except Exception:
        impl.count_hook_error('error_log_trace')
        return ''


def count_dropped(reason: str, count: int = 1) -> None:
    """Count telemetry dropped to stay within a bound (rocketride.telemetry.dropped)."""
    impl = _impl
    if impl is not None:
        impl.count_dropped(reason, count)


def runs_lost(identities: List[Mapping[str, Any]]) -> None:
    """Runs that were in progress when the previous EaaS process died."""
    impl = _impl
    if impl is not None:
        impl.guard('runs_lost', impl.runs_lost, identities)


def attributed_orgs() -> frozenset:
    """The org ids this process has attributed to its metrics so far; empty when off."""
    impl = _impl
    if impl is None:
        return frozenset()
    try:
        return impl.attributed_orgs()
    except Exception:
        impl.count_hook_error('attributed_orgs')
        return frozenset()


def is_running() -> bool:
    """Whether telemetry is on in this process (cheap: callers skip work for it when off)."""
    return _impl is not None


def org_id_of(account: Any) -> Optional[str]:
    """The org id of an AccountInfo (or anything with .organization['id']); None, never an error."""
    try:
        organization = getattr(account, 'organization', None)
        if isinstance(organization, Mapping):
            value = organization.get('id')
        else:
            value = getattr(organization, 'id', None)
        return str(value) if value else None
    except Exception:
        return None


# ── Declared metrics, for any module ──────────────────────────────────────────


class _Instrument:
    """A metric declared at import time; bound to the SDK when telemetry starts."""

    kind = ''

    def __init__(
        self,
        name: str,
        unit: str,
        description: str,
        attributes: Optional[Mapping[str, str]] = None,
        sets_per_org: Optional[int] = None,
    ) -> None:
        """Declare the metric.

        Args:
            name: Metric name, namespaced (e.g. 'rocketride.billing.webhook').
            unit: UCUM unit, e.g. 's' or '{event}'.
            description: One line.
            attributes: Keyword name → exported attribute key. Only these are exported.
            sets_per_org: For a metric whose attributes are the org and a few fixed
                values: the distinct sets one org can take. Its lifetime bound on
                attribute sets is then sized to the org cap (which already bounds
                it) instead of the fixed MAX_ATTRIBUTE_SETS.
        """
        self.name = name
        self.unit = unit
        self.description = description
        self.attributes: Dict[str, str] = dict(attributes or {})
        self.sets_per_org = sets_per_org
        self._instrument = None
        self._impl = None
        with _state_lock:
            _declared.append(self)
            impl = _impl
        if impl is not None:
            self._bind(impl)

    def _bind(self, impl: Any) -> None:
        try:
            if self.sets_per_org:
                impl.size_attribute_sets(self.name, self.sets_per_org)
            self._instrument = self._make(impl)
            self._impl = impl
        except Exception:
            self._instrument = None

    def _make(self, impl: Any) -> Any:
        """The SDK instrument for this kind."""
        return impl.make_instrument(self.kind, self.name, self.unit, self.description)

    def _attributes(self, values: Mapping[str, Any]) -> Dict[str, str]:
        return self._impl.clean_attributes(self.name, self.attributes, values)


class Counter(_Instrument):
    """A monotonic count, e.g. logins by outcome."""

    kind = 'counter'

    def add(self, value: int = 1, /, **attributes: Any) -> None:
        """Add to the count (no-op unless telemetry is on; never raises)."""
        instrument = self._instrument
        if instrument is None:
            return
        try:
            instrument.add(value, self._attributes(attributes))
        except Exception:
            self._impl.count_hook_error(self.name)


class Histogram(_Instrument):
    """A distribution, e.g. a duration in seconds."""

    kind = 'histogram'

    def record(self, value: float, /, **attributes: Any) -> None:
        """Record one value (no-op unless telemetry is on; never raises)."""
        instrument = self._instrument
        if instrument is None:
            return
        try:
            instrument.record(value, self._attributes(attributes))
        except Exception:
            self._impl.count_hook_error(self.name)


class Gauge(_Instrument):
    """A value read when metrics are collected, e.g. a queue's depth.

    `observe` returns (value, attributes) pairs, attributes by keyword name as
    for the other instruments. It runs on the SDK's export thread: keep it
    cheap and thread-safe. What it raises is counted and ignored. Values that
    end in the same series (orgs past the cap) are added up: give a level that
    must not be added (a percentage, a temperature) no capped attribute.
    """

    kind = 'gauge'

    def __init__(
        self,
        name: str,
        unit: str,
        description: str,
        observe: Callable[[], Iterable[Tuple[float, Mapping[str, Any]]]],
        attributes: Optional[Mapping[str, str]] = None,
        sets_per_org: Optional[int] = None,
    ) -> None:
        """Declare the gauge; see _Instrument for the other arguments."""
        self._observe = observe
        super().__init__(name, unit, description, attributes, sets_per_org)

    def _make(self, impl: Any) -> Any:
        return impl.make_gauge(self.name, self.unit, self.description, self._observations, self.kind)

    def _observations(self) -> List[Tuple[float, Dict[str, str]]]:
        """
        One observation per exported attribute set. Values whose attributes end up
        the same once cleaned (orgs past the cap, all '_other') are added up: an
        SDK keeps only the last of two observations with the same attributes.
        """
        totals: Dict[frozenset, float] = {}
        for value, attributes in self._observe():
            key = frozenset(self._attributes(attributes).items())
            totals[key] = totals.get(key, 0) + value
        return [(value, dict(key)) for key, value in totals.items()]


class ObservableCount(Gauge):
    """A count read when metrics are collected that adds up across its attribute sets,
    e.g. open connections by organization (an observable up-down counter).
    """

    kind = 'updown'


class InfoGauge(Gauge):
    """A gauge that describes entities, e.g. each organization's display name (value 1).

    Unlike Gauge it keeps no lifetime set of attribute combinations (a rename
    or a deleted entity would fill MAX_ATTRIBUTE_SETS for good) and never takes
    a slot of the org cap: an org id this process has not attributed to its
    metrics is left out. Each collection exports at most `limit` observations;
    the rest are counted as dropped ('info_overflow').
    """

    def __init__(
        self,
        name: str,
        unit: str,
        description: str,
        observe: Callable[[], Iterable[Tuple[float, Mapping[str, Any]]]],
        attributes: Optional[Mapping[str, str]] = None,
        limit: int = 20_000,
    ) -> None:
        """Declare the gauge; `limit` bounds the observations of one collection."""
        self._limit = limit
        super().__init__(name, unit, description, observe, attributes)

    def _observations(self) -> List[Tuple[float, Dict[str, str]]]:
        out: List[Tuple[float, Dict[str, str]]] = []
        over = 0
        for value, attributes in self._observe():
            cleaned = self._impl.clean_info_attributes(self.attributes, attributes)
            if cleaned is None:
                continue
            if len(out) >= self._limit:
                over += 1
                continue
            out.append((value, cleaned))
        if over:
            self._impl.count_dropped('info_overflow', over)
        return out


def set_span_attributes(**attributes: Any) -> None:
    """Add attributes to the current span (a request's live span), e.g. timings a handler measured.

    Keys are attribute names (dots allowed via **{...}); values str, bool, int or
    float. A no-op when off or when no span is current.
    """
    impl = _impl
    if impl is not None:
        impl.guard('set_span_attributes', impl.set_span_attributes, attributes)


@contextlib.contextmanager
def timed(histogram: Histogram, **attributes: Any) -> Iterator[Dict[str, Any]]:
    """Time a block into `histogram` (seconds). An exception sets error.type and re-raises.

    The yielded dict can add attributes from inside the block:

        with telemetry.timed(WEBHOOK_TIME, event='invoice.paid') as attrs:
            ...
            attrs['outcome'] = 'skipped'
    """
    extra: Dict[str, Any] = {}
    started = time.monotonic()
    try:
        yield extra
    except BaseException as e:
        extra.setdefault('error', type(e).__name__)
        raise
    finally:
        histogram.record(time.monotonic() - started, **{**attributes, **extra})


# ── Model server requests (ModelClient calls this) ─────────────────────────────


class _NoModelCall:
    """model_server_call() with the 'model' recorder off: nothing timed, the coroutine as it is."""

    __slots__ = ()

    def __enter__(self) -> '_NoModelCall':
        return self

    def __exit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        return False

    def traced(self, coroutine: Any) -> Any:
        return coroutine


_NO_MODEL_CALL = _NoModelCall()
# Set by _deps.ModelRecorder.install() (RR_OTEL_INSTRUMENT 'model', or the relay's
# --otel_relay=...,model), cleared by its uninstall(); None while off
_model_recorder: Any = None


def model_server_call(client: Any, command: str) -> Any:
    """
    Time one model server request a ModelClient makes, as `with`; a no-op while off.

        with telemetry.model_server_call(self, command) as call:
            future = run_coroutine_threadsafe(call.traced(self._request(...)), loop)
            body = future.result()

    call.traced(coroutine) runs the request in the call's trace (its traceparent
    names the pipeline pass, the model server's span is the call's child). An
    exception goes on; its class is the call's error type.
    """
    recorder = _model_recorder
    if recorder is None:
        return _NO_MODEL_CALL
    try:
        return recorder.call(client, command)
    except Exception:
        return _NO_MODEL_CALL  # a model call never fails for telemetry


# ── SDK logging during a collector outage ─────────────────────────────────────


class _RateLimit(logging.Filter):
    """Let each distinct message through at most once a minute; count the rest."""

    def __init__(self, interval: float = 60.0) -> None:
        super().__init__()
        self._interval = interval
        self._last: Dict[str, float] = {}
        self._suppressed: Dict[str, int] = {}
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        key = f'{record.name}:{record.msg}'
        now = time.monotonic()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self._interval:
                self._suppressed[key] = self._suppressed.get(key, 0) + 1
                return False
            suppressed = self._suppressed.pop(key, 0)
            self._last[key] = now
            if len(self._last) > 500:
                self._last.clear()
                self._suppressed.clear()
        if suppressed:
            record.msg = f'{record.msg} ({suppressed} similar messages suppressed in the last minute)'
        return True


_quieted = False


def _quiet_sdk_logging() -> None:
    """Route the SDK's own logs through a rate limit, so an outage is one line a minute."""
    global _quieted
    if _quieted:
        return
    _quieted = True
    try:
        logger = logging.getLogger('opentelemetry')
        handler = logging.StreamHandler()
        from ai.common import logfmt

        if logfmt.log_format() == 'json':
            handler.setFormatter(logfmt.JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter('[otel] %(levelname)s %(name)s: %(message)s'))
        handler.addFilter(_RateLimit())
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        logger.propagate = False
    except Exception:
        pass


def _reset_for_tests() -> None:
    """Forget the running implementation (tests only)."""
    global _impl, _starting, _closed, _connections, _model_recorder
    _model_recorder = None
    with _state_lock:
        _impl = None
        _starting = False
        _closed = False
        _connections = None
    for instrument in list(_declared):
        instrument._instrument = None
        instrument._impl = None
