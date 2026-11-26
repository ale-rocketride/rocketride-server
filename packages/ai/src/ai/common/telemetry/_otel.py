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
The running telemetry sink: maps EaaS task events to OpenTelemetry metrics and spans.

Imported only by ai.common.telemetry once the SDK is installed. The catalogue
of what it exports, with units and attributes, is in
docs/public/product/operate/self-hosting/telemetry.md.

Traces. Two kinds, both rooted in EaaS:
    'pipeline'  one per invocation of the pipeline on an object (from the first
                component enter to the last leave on that pipe), with a
                'component <type>' child per component call. Exported as the
                invocation ends, so a long-running deploy has live traces.
                Sampled at the end: failed and slow invocations are always
                kept, the rest at RR_OTEL_TRACES_SAMPLE_RATIO (default 1.0).
                The hook only decides and queues; the spans are built on the
                'otel-traces' thread, so the event loop never waits on a whole
                trace (the thread still shares the interpreter's CPU: see the
                cost table in the docs).
    'task'      one per run, begin → end, carrying the outcome. Kept for a run
                that ended in error, otherwise at the same ratio.
    'request'   a SERVER span per DAP request (a child of the caller's sampled
                traceparent, when there is one), with a CLIENT span per
                dependency call it made (_deps). Its ids are set when it
                starts, so its trace is current while the handler runs; the
                spans are built when it ends, only when kept: failed, slow, or
                an ERROR log named its trace; with a caller that traces, also
                at the ratio.
    RR_OTEL_TRACES_SAMPLE_RATIO=0 therefore exports errors (and slow ones) only.

Times: durations come from the monotonic clock at the hook; span timestamps
come from the events' eventTime (EaaS ingress, as in the run log), clamped so a
span never ends before it starts.
"""

import os
import queue
import random
import socket
import threading
import time
import uuid
from collections import OrderedDict, deque
from types import SimpleNamespace
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from ai.common import logfmt

from . import _deps

from ._common import (
    SCHEMA_URL,
    SCOPE,
    RUN_KINDS,
    OUTCOMES,
    MAX_PROJECTS_PER_ORG,
    MAX_ATTRIBUTE_SETS,
    SLI_SETS_PER_ORG,
    MAX_ERROR_TYPES,
    TRACE_QUEUE,
    MAX_VALUE_LENGTH,
    ORIGIN_KEY,
    PROJECT_KEY,
    MAX_COMPONENT_SPANS,
    CALLER_TRACES_PER_MINUTE,
    DROP_REASONS,
    ORG_KEY,
    _truthy,
    _float,
    _ns,
    _PresetIdGenerator,
    _SlowLimiter,
    _Run,
)
from ._dependencies import _DependenciesTelemetry
from ._requests import _RequestsTelemetry
from ._runs import _RunsTelemetry


class Telemetry(_RunsTelemetry, _RequestsTelemetry, _DependenciesTelemetry):
    """The SDK providers, the instruments, and the per-run state behind the hooks."""

    @classmethod
    def from_environment(cls, service_version: str = '', environ: Optional[Mapping[str, str]] = None) -> 'Telemetry':
        """Build from the standard OTEL_* variables and the RR_OTEL_* ones (see the package doc)."""
        environ = os.environ if environ is None else environ
        endpoint_display = _check_transport(environ)
        return cls(
            service_version=service_version,
            sample_ratio=min(1.0, max(0.0, _float(environ, 'RR_OTEL_TRACES_SAMPLE_RATIO', 1.0))),
            slow_seconds=_float(environ, 'RR_OTEL_TRACES_SLOW_SECONDS', 60.0),
            slow_run_seconds=_float(environ, 'RR_OTEL_SLOW_RUN_SECONDS', 600.0),
            slow_request_seconds=_float(environ, 'RR_OTEL_SLOW_REQUEST_SECONDS', 2.0),
            slow_request_exclude=environ.get('RR_OTEL_SLOW_REQUEST_EXCLUDE', 'rrext_cprofile_*'),
            slow_traces_per_minute=_float(environ, 'RR_OTEL_SLOW_TRACES_PER_MINUTE', 100.0),
            component_span_cap=int(_float(environ, 'RR_OTEL_COMPONENT_SPANS', 200)),
            org_attribute=_truthy(environ.get('RR_OTEL_ORG_ATTRIBUTE')),
            org_cardinality=int(_float(environ, 'RR_OTEL_ORG_CARDINALITY', 2000)),
            stale_call_seconds=_float(environ, 'RR_OTEL_STALE_CALL_SECONDS', 0.0),
            stall_log_seconds=_float(environ, 'RR_OTEL_STALL_LOG_SECONDS', 900.0),
            stall_exempt_types=environ.get('RR_OTEL_STALL_EXEMPT_TYPES', ''),
            overdue_request_seconds=_float(environ, 'RR_OTEL_OVERDUE_REQUEST_SECONDS', 300.0),
            exponential_histograms=not environ.get('OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION'),
            endpoint_display=endpoint_display,
        )

    def __init__(
        self,
        service_version: str = '',
        sample_ratio: float = 1.0,
        slow_seconds: float = 0.0,
        slow_run_seconds: float = 0.0,
        slow_request_seconds: float = 0.0,
        slow_request_exclude: str = '',
        slow_traces_per_minute: float = 100.0,
        component_span_cap: int = 200,
        org_attribute: bool = False,
        org_cardinality: int = 2000,
        stale_call_seconds: float = 0.0,
        stall_log_seconds: float = 900.0,
        stall_exempt_types: str = '',
        overdue_request_seconds: float = 300.0,
        exponential_histograms: bool = True,
        endpoint_display: str = '',
        span_exporter=None,
        metric_reader=None,
    ) -> None:
        """Build the providers and instruments; tests pass in-memory exporter/reader."""
        self.endpoint_display = endpoint_display
        self._ratio = sample_ratio
        self._slow = max(0.0, slow_seconds)  # a pipeline pass (0: off)
        self._slow_run = max(0.0, slow_run_seconds)  # a one-shot run; never a deploy
        self._slow_request = max(0.0, slow_request_seconds)  # a DAP request
        # Long-lived requests are not slow: their methods, as fnmatch patterns
        self._slow_exclude = [p.strip() for p in slow_request_exclude.split(',') if p.strip()]
        self._slow_limit = _SlowLimiter(slow_traces_per_minute)
        self._dependency_error_limit = _SlowLimiter(slow_traces_per_minute)
        # A caller's sampled traceparent keeps the request's span in the caller's
        # trace, but a caller cannot choose how many: CALLER_TRACES_PER_MINUTE per
        # process, a tenth of it per org
        self._caller_limit = _SlowLimiter(CALLER_TRACES_PER_MINUTE)
        self._span_cap = min(MAX_COMPONENT_SPANS, max(0, component_span_cap))
        # The org id on metrics (opt-in: up to RR_OTEL_ORG_CARDINALITY orgs times each
        # metric's attribute sets); spans and error lines always carry it (no series)
        self._org_attribute = org_attribute
        self._org_cap = max(0, org_cardinality)
        # Off by default: the engine emits enter/leave in pairs, and the outermost
        # call of a nested pass stays open for the whole pass, so a threshold
        # also splits legitimately long passes. On, a call open this long is
        # taken as a lost leave and its pass is closed unfinished.
        self._stale_call = max(0.0, stale_call_seconds)
        # A pass with a call open and no enter or leave for this long writes one
        # error line (rocketride.pass.stalled) naming its run (0: never)
        self._stall_log = max(0.0, stall_log_seconds)
        # Node types that wait for a person (an approval, an input): never stalled
        self._stall_exempt = [p.strip() for p in stall_exempt_types.split(',') if p.strip()]
        # WebSocket requests being handled (not the long-lived methods): span id →
        # [method, monotonic start, identity, request, overdue line written]. A
        # request open this long writes one error line (0: never)
        self._inflight: Dict[int, List[Any]] = {}
        self._overdue = max(0.0, overdue_request_seconds)
        # Reported before → when last idle: reported 0 when idle no more, then
        # dropped after IDLE_KEY_SECONDS (no series kept forever)
        self._idle_keys: Dict[Tuple[str, str], float] = {}
        self._random = random.Random()

        self._lock = threading.Lock()
        self._runs: 'OrderedDict[str, _Run]' = OrderedDict()
        # Runs that ended: a status update can still arrive after the end and
        # must neither revive the run nor recount its objects or tokens
        self._ended_ids: 'OrderedDict[str, None]' = OrderedDict()
        self._component_types: Set[str] = set()
        self._models: Dict[str, Set[str]] = {}  # provider → the models its metrics name
        self._projects: Dict[str, Set[str]] = {}  # org → the projects its metrics name
        # Distinct error.type values, nodes and requests apart so neither can
        # exhaust the other's (MAX_ERROR_TYPES each)
        self._error_types: Dict[str, Set[str]] = {
            'node': set(),
            'request': set(),
            'dependency': set(),
            'warning': set(),
            'model': set(),
            'engine': set(),
            'signal': set(),
        }
        self._buffered = 0  # component calls held in open invocations (MAX_BUFFERED_CALLS)
        self._evicted = 0  # runs evicted under the lock, counted once it is released
        # model calls those runs' open passes held: measured once the lock is released
        self._evicted_models: List[Tuple[Any, ...]] = []
        self._orgs: Set[str] = set()
        self._attribute_sets: Dict[str, Set[FrozenSet]] = {}
        # a metric bounded by the org cap instead (size_attribute_sets): its own limit
        self._set_limits: Dict[str, int] = {}
        self._connections: Optional[Callable[[], int]] = None

        resource = _resource(service_version)

        # Providers are this module's own, never the global ones, and register
        # no atexit hook: shutdown is ours, bounded (telemetry.shutdown).
        self._tracer_provider = TracerProvider(
            resource=resource, sampler=ALWAYS_ON, shutdown_on_exit=False, id_generator=_PresetIdGenerator()
        )
        # Dependency calls wait in front of the exporter for their trace's decision
        self._spans = _deps.DependencyBuffer(
            BatchSpanProcessor(span_exporter or OTLPSpanExporter()),
            SCOPE,
            keep_root=self._keep_dependency_root,
            measure=lambda name, seconds, attributes: self._queue_measurement(
                name, seconds, dict(attributes, **{ORIGIN_KEY: 'eaas'})
            ),
            count_dropped=self.count_dropped,
            redact=logfmt.redact,
        )
        self._tracer_provider.add_span_processor(self._spans)
        self._recorders: List[Any] = []  # _deps recorders installed (removed at shutdown)
        self._pipeline_servers: Set[str] = set()  # server.address values from pipelines (MAX_PIPELINE_SERVERS)
        self._org_servers: Dict[str, Set[str]] = {}  # the same, by customer (MAX_SERVERS_PER_ORG)
        self._tracer = self._tracer_provider.get_tracer(SCOPE, service_version or None, schema_url=SCHEMA_URL)

        if metric_reader is None:
            # Exponential histograms (Mimir native histograms) unless the operator
            # chose the aggregation with the standard variable
            aggregation = {Histogram: ExponentialBucketHistogramAggregation()} if exponential_histograms else None
            metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(preferred_aggregation=aggregation))
        self._meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader], shutdown_on_exit=False)
        meter = self._meter_provider.get_meter(SCOPE, service_version or None, schema_url=SCHEMA_URL)
        self._meter = meter

        self._started = meter.create_counter('rocketride.task.started', '{task}', 'Runs begun')
        self._ended = meter.create_counter('rocketride.task.ended', '{task}', 'Runs ended, by outcome')
        self._duration = meter.create_histogram('rocketride.task.duration', 's', 'Run duration, begin to end')
        self._startup = meter.create_histogram('rocketride.task.startup.duration', 's', 'Run startup, begin to running')
        self._component = meter.create_histogram(
            'rocketride.component.duration', 's', 'Component call duration, enter to leave'
        )
        self._component_errors = meter.create_counter(
            'rocketride.component.errors', '{error}', 'Errors and warnings reported by a node'
        )
        # Failures only (few series): which node of which customer a provider
        # refused, e.g. a 401 for a customer's API key, which the node may answer
        # with an error message and go on
        self._dependency_errors = meter.create_counter(
            'rocketride.component.dependency_errors',
            '{call}',
            'Failed dependency calls a node made, by node type, server and error',
        )
        self._objects = meter.create_counter('rocketride.object.processed', '{object}', 'Objects processed, by outcome')
        # One per pass (a question, a document) when it ends, kept or not by the
        # trace sampling: whether the user got a result, and if not, whose failure
        # it was (TURN_CAUSES). The engine counts an LLM error answered as text as
        # a completed object; this counts it as the failed turn it is.
        self._turns = meter.create_counter(
            'rocketride.turn', '{turn}', 'Passes (questions, documents) ended, by outcome and cause of failure'
        )
        # How long a customer waited for a question's (or a document's) answer,
        # answered or failed, by the LLM server it used: the answer time a customer
        # feels. No org: per-customer latency is a trace question (bounded series)
        self._turn_duration = meter.create_histogram(
            'rocketride.turn.duration', 's', 'Pass (question, document) duration, begin to end, answered or failed'
        )
        self._tokens = meter.create_counter('rocketride.llm.tokens', '{token}', 'LLM tokens used by runs')
        # A node's logical model calls (GenAI conventions; every call, kept or not by
        # sampling): which provider and model fail or are slow. No org (bounded series):
        # per customer it is rocketride.turn, which names the model too
        self._genai_duration = meter.create_histogram(
            'gen_ai.client.operation.duration', 's', 'Model calls made by pipelines, tries and waits included'
        )
        self._genai_tokens = meter.create_histogram(
            'gen_ai.client.token.usage', '{token}', 'Tokens used by one model call, input or output'
        )
        self._request = meter.create_histogram(
            'rpc.server.call.duration', 's', 'Client WebSocket (DAP) request handling time'
        )
        # A customer's requests and their failures (rpc.server.call.duration has no org)
        self._requests = meter.create_counter(
            'rocketride.ws.requests',
            '{request}',
            'Client WebSocket (DAP) requests handled, by organization and outcome',
        )
        # Dependency calls (semconv names; derived from the spans, every call)
        self._dependency = {
            'db.client.operation.duration': meter.create_histogram(
                'db.client.operation.duration', 's', 'Database and Redis calls made by EaaS'
            ),
            'http.client.request.duration': meter.create_histogram(
                'http.client.request.duration', 's', 'HTTP requests made by EaaS'
            ),
            'rpc.client.call.duration': meter.create_histogram(
                'rpc.client.call.duration', 's', 'AWS API calls made by EaaS'
            ),
        }
        self._hook_errors = meter.create_counter(
            'rocketride.telemetry.hook_errors', '{error}', 'Telemetry hooks that raised (and were ignored)'
        )
        self._dropped = meter.create_counter(
            'rocketride.telemetry.dropped', '{item}', 'Telemetry dropped to stay within its bounds, by reason'
        )
        meter.create_observable_up_down_counter(
            'rocketride.task.count', [self._observe_runs], '{task}', 'Live runs, by state'
        )
        meter.create_observable_up_down_counter(
            'rocketride.ws.connection.count', [self._observe_connections], '{connection}', 'Open client connections'
        )
        meter.create_observable_gauge(
            'rocketride.telemetry.up', [self._observe_up], '', '1 while telemetry exports (absent: telemetry down)'
        )
        meter.create_observable_up_down_counter(
            'rocketride.ws.requests.in_flight',
            [self._observe_inflight],
            '{request}',
            'WebSocket requests being handled, by method (long-lived methods excluded)',
        )
        meter.create_observable_gauge(
            'rocketride.ws.requests.oldest',
            [self._observe_oldest],
            's',
            'Age of the oldest WebSocket request being handled, by method: a request that never finishes grows it',
        )
        meter.create_observable_gauge(
            'rocketride.pass.idle',
            [self._observe_idle],
            's',
            'Longest time without an enter or leave in a pass with a call still open, by the innermost open component type',
        )

        # Lifetime bounds on the attribute sets of the per-customer SLI metrics: per
        # org (sized by the org cap), then '_other' (counted as attribute_overflow)
        for name, per_org in SLI_SETS_PER_ORG.items():
            self._set_limits[name] = max(MAX_ATTRIBUTE_SETS, (self._org_cap + 2) * per_org)

        for run_kind in RUN_KINDS:
            self._started.add(0, {'rocketride.task.run_kind': run_kind})
            for outcome, stop, error_type in OUTCOMES:
                self._ended.add(0, self._ended_attributes(run_kind, outcome, stop, error_type, ''))
        for outcome in ('completed', 'failed'):
            self._objects.add(0, {'rocketride.object.outcome': outcome})
        for reason in DROP_REASONS:
            self._dropped.add(0, {'rocketride.telemetry.reason': reason})

        # Pipeline traces are built off the event loop (see the module doc)
        self._traces: 'queue.Queue' = queue.Queue(maxsize=TRACE_QUEUE)
        # set at shutdown: the worker stops once the queue is empty (a full queue
        # needs no room for a stop signal, and no admitted trace is thrown away)
        self._traces_stop = threading.Event()
        self._trace_thread = threading.Thread(target=self._trace_worker, name='otel-traces', daemon=True)
        self._trace_thread.start()

        # Dependency calls are measured off the caller's path: recording into a
        # histogram costs more than the call's own bookkeeping (see _deps)
        self._measurements: 'deque' = deque()
        self._measure_stop = threading.Event()
        self._measure_thread = threading.Thread(target=self._measure_worker, name='otel-deps', daemon=True)
        self._measure_thread.start()

    # ── Guards and attribute hygiene ─────────────────────────────────────────

    def guard(self, hook: str, fn: Callable, *args) -> None:
        """Run a hook; count and swallow anything it raises."""
        try:
            fn(*args)
        except Exception:
            self.count_hook_error(hook)

    def count_hook_error(self, hook: str) -> None:
        try:
            self._hook_errors.add(1, {'rocketride.telemetry.hook': hook[:MAX_VALUE_LENGTH]})
        except Exception:
            pass

    def count_dropped(self, reason: str, count: int = 1) -> None:
        try:
            self._dropped.add(count, {'rocketride.telemetry.reason': reason})
        except Exception:
            pass

    def _org(self, org_id: Any) -> Optional[str]:
        """The org id to attach to a metric, or None: off, unknown, or past the cardinality cap ('_other')."""
        return self._capped_org(org_id) if self._org_attribute else None

    @staticmethod
    def _span_org(org_id: Any) -> Optional[str]:
        """The org id on a span or an error line: always, uncapped (a span or a line makes no series)."""
        return str(org_id)[:MAX_VALUE_LENGTH] if org_id else None

    def _with_span_org(self, attributes: Dict[str, Any], org_id: Any) -> Dict[str, Any]:
        org = self._span_org(org_id)
        if org is not None:
            attributes[ORG_KEY] = org
        return attributes

    def _capped_org(self, org_id: Any) -> Optional[str]:
        """
        The org id within the cardinality cap ('_other' past it), or None: the
        metrics' org label, and the key of the per-customer bounds (projects,
        servers) whether or not the label is on.
        """
        if not org_id:
            return None
        value = str(org_id)[:MAX_VALUE_LENGTH]
        with self._lock:
            if value in self._orgs:
                return value
            full = len(self._orgs) >= self._org_cap
            if not full:
                self._orgs.add(value)
        if full:
            self.count_dropped('org_overflow')
            return '_other'
        return value

    def _with_org(self, attributes: Dict[str, Any], org_id: Any, project: Any = None) -> Dict[str, Any]:
        """A metric's org id (bounded, when on), and with a project, its project id (MAX_PROJECTS_PER_ORG per org)."""
        org = self._capped_org(org_id)
        if org is not None and self._org_attribute:
            attributes[ORG_KEY] = org
        if project:
            attributes[PROJECT_KEY] = self._bounded_project(org or '', str(project)[:MAX_VALUE_LENGTH])
        return attributes

    def _bounded_project(self, org: str, project: str) -> str:
        """A pipeline's project id as metrics keep it: MAX_PROJECTS_PER_ORG per customer, then '_other'."""
        with self._lock:
            known = self._projects.get(org)
            if known is None:
                if len(self._projects) >= self._org_cap + 2:  # every org, '_other' and none
                    return '_other'
                known = self._projects[org] = set()
            if project in known:
                return project
            if len(known) < MAX_PROJECTS_PER_ORG:
                known.add(project)
                return project
        self.count_dropped('project_overflow')
        return '_other'

    def _ended_attributes(
        self, run_kind: str, outcome: str, stop: str, error_type: Optional[str], org_id: Any, project: Any = None
    ):
        attributes = {
            'rocketride.task.run_kind': run_kind,
            'rocketride.task.outcome': outcome,
            'rocketride.task.stop_reason': stop,
        }
        if error_type:
            attributes['error.type'] = error_type
        return self._with_org(attributes, org_id, project)

    def make_instrument(self, kind: str, name: str, unit: str, description: str) -> Any:
        """A counter or histogram for a module-declared metric (telemetry.Counter/Histogram)."""
        if kind == 'counter':
            return self._meter.create_counter(name, unit, description)
        if kind == 'histogram':
            return self._meter.create_histogram(name, unit, description)
        raise ValueError(f'unknown instrument kind {kind!r}')

    def make_gauge(
        self,
        name: str,
        unit: str,
        description: str,
        observe: Callable[[], List[Tuple[float, Dict]]],
        kind: str = 'gauge',
    ):
        """An observable gauge (or, kind 'updown', up-down counter) for a module-declared telemetry.Gauge."""

        def callback(options: CallbackOptions):
            try:
                return [Observation(value, attributes) for value, attributes in observe()]
            except Exception:
                self.count_hook_error(name)
                return []

        if kind == 'updown':
            return self._meter.create_observable_up_down_counter(name, [callback], unit, description)
        return self._meter.create_observable_gauge(name, [callback], unit, description)

    def size_attribute_sets(self, name: str, sets_per_org: int) -> None:
        """Bound a metric's lifetime attribute sets by the org cap: sets_per_org for each org, '_other' and none."""
        with self._lock:
            self._set_limits[name] = max(MAX_ATTRIBUTE_SETS, (self._org_cap + 2) * sets_per_org)

    def set_span_attributes(self, attributes: Mapping[str, Any]) -> None:
        """Attributes for the current request's span (set on it if it is kept)."""
        request = _deps.current_request()
        if request is None:
            return
        for key, value in attributes.items():
            if isinstance(value, (bool, int, float)):
                request.attributes[key[:MAX_VALUE_LENGTH]] = value
            elif value is not None:
                request.attributes[key[:MAX_VALUE_LENGTH]] = str(value)[:MAX_VALUE_LENGTH]

    def clean_attributes(self, name: str, keys: Mapping[str, str], values: Mapping[str, Any]) -> Dict[str, str]:
        """Declared keys only, short string values, a bounded number of distinct sets per metric."""
        attributes: Dict[str, str] = {}
        for argument, value in values.items():
            key = keys.get(argument)
            if key is None or value is None or value == '':
                continue
            if key == ORG_KEY:
                org = self._org(value)
                if org is not None:
                    attributes[key] = org
                continue
            attributes[key] = str(value)[:MAX_VALUE_LENGTH]
        return self._bounded(name, attributes)

    def attributed_orgs(self) -> frozenset:
        """The org ids this process has attributed so far (never '_other'); empty when the org id is off."""
        if not self._org_attribute:
            return frozenset()
        with self._lock:
            return frozenset(self._orgs)

    def clean_info_attributes(self, keys: Mapping[str, str], values: Mapping[str, Any]) -> Optional[Dict[str, str]]:
        """
        An info gauge's attributes: declared keys only, short string values.

        Never takes a slot of the org cap and never enters a metric's lifetime
        set of attribute combinations (a rename would fill it): an org id this
        process has not attributed drops the whole observation (None), and the
        caller bounds the observations of each collection instead.
        """
        attributes: Dict[str, str] = {}
        for argument, value in values.items():
            key = keys.get(argument)
            if key is None or value is None or value == '':
                continue
            value = str(value)[:MAX_VALUE_LENGTH]
            if key == ORG_KEY:
                if not self._org_attribute:
                    return None
                with self._lock:
                    if value not in self._orgs:
                        return None
            attributes[key] = value
        return attributes

    def _bounded(self, name: str, attributes: Dict[str, str]) -> Dict[str, str]:
        """The attributes, or '_other' for each once the metric has MAX_ATTRIBUTE_SETS distinct sets."""
        signature = frozenset(attributes.items())
        with self._lock:
            seen = self._attribute_sets.setdefault(name, set())
            full = signature not in seen and len(seen) >= self._set_limits.get(name, MAX_ATTRIBUTE_SETS)
            if signature not in seen and not full:
                seen.add(signature)
        if full:
            self.count_dropped('attribute_overflow')
            return {key: '_other' for key in attributes}
        return attributes

    # ── Hooks (on the event loop: cheap, never await, never raise) ────────────

    # ── Internals ────────────────────────────────────────────────────────────

    def _bounded_error_type(self, error_type: str, kind: str) -> str:
        """error.type within MAX_ERROR_TYPES distinct values per kind ('node', 'request'), then '_OTHER'."""
        with self._lock:
            seen = self._error_types[kind]
            if error_type in seen or error_type == '_OTHER':
                return error_type
            full = len(seen) >= MAX_ERROR_TYPES
            if not full:
                seen.add(error_type)
        if full:
            self.count_dropped('error_type_overflow')
            return '_OTHER'
        return error_type

    # ── Observable callbacks (on the metric reader thread) ───────────────────

    def _observe_up(self, options: CallbackOptions):
        return [Observation(1)]

    # ── Shutdown ─────────────────────────────────────────────────────────────

    def shutdown(self, timeout: float = 3.0) -> None:
        """Export what is open, then stop: metrics first (the SLIs), then traces, within `timeout`."""
        for recorder in self._recorders:
            try:
                recorder.uninstall()
            except Exception:
                pass
        self._recorders = []
        self._measure_stop.set()
        self.flush_measurements()
        now = time.time()
        with self._lock:
            runs = list(self._runs.items())
            self._runs.clear()
            self._buffered = 0
        for task_id, run in runs:
            for invocation in list(run.invocations.values()):
                self._emit_invocation(task_id, invocation, run, end=now, unclosed=True, why='shutdown')
            # The run's own trace, still open: its kept passes link to it
            reason = run.keep_reason or ('ratio' if self._keep_success() else '')
            if run.begin_mono is not None and run.trace_id and reason:
                try:
                    span = self._start_task_span(
                        task_id,
                        run,
                        run.org,
                        {
                            'rocketride.task.run_kind': run.run_kind or 'dev',
                            'rocketride.sampling.reason': reason,
                            'rocketride.task.closed_by': 'shutdown',
                        },
                    )
                    span.end(end_time=_ns(max(now, run.begin_wall)))
                except Exception:
                    self.count_hook_error('task_span_shutdown')
        # The queued traces get a quarter of the time; the rest is dropped
        self.flush_traces(timeout / 4)
        self._traces_stop.set()
        try:
            self._traces.put_nowait(None)  # wakes the worker now when there is room
        except queue.Full:
            pass  # it stops once it has built what was admitted
        half = max(1, int(timeout * 375))
        try:
            self._meter_provider.shutdown(timeout_millis=half)
        except Exception:
            pass
        try:
            self._tracer_provider.force_flush(timeout_millis=half)
            self._tracer_provider.shutdown()
        except Exception:
            pass


def _resource(service_version: str) -> Resource:
    """RocketRide defaults, overridden by OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES."""
    defaults = {
        'service.name': 'rocketride-eaas',
        'service.namespace': 'rocketride',
        # Unique per process (several EaaS can share a host); host.name is the pod
        'service.instance.id': str(uuid.uuid4()),
        'host.name': os.environ.get('HOSTNAME') or socket.gethostname(),
    }
    if service_version:
        defaults['service.version'] = service_version
    from_environment = Resource.create({})
    attributes = dict(from_environment.attributes)
    if str(attributes.get('service.name', '')).startswith('unknown_service'):
        attributes.pop('service.name')
    return Resource(defaults, SCHEMA_URL).merge(Resource(attributes))


def _check_transport(environ: Mapping[str, str]) -> str:
    """Refuse credential headers over a cleartext non-loopback endpoint; return the endpoint for display.

    Reuses the rocketride otel bridge's check (rocketride.otelbridge.config), so
    both OpenTelemetry paths behave alike. ROCKETRIDE_OTEL_ALLOW_INSECURE=1 opts out.
    """
    try:
        from rocketride.otelbridge import config as bridge
    except Exception:
        return environ.get('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4318')
    settings = SimpleNamespace(
        endpoint=None,
        protocol='http',
        headers=None,
        no_metrics=False,
        allow_insecure=_truthy(environ.get('ROCKETRIDE_OTEL_ALLOW_INSECURE')),
    )
    bridge.validate_transport_security(settings, environ)
    return bridge.redact_endpoint(bridge.effective_endpoint(settings, environ, 'traces'))
