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

import contextlib
import contextvars
import fnmatch
import json
import os
import queue
import random
import re
import socket
import sys
import threading
import time
import uuid
from collections import OrderedDict, deque
from types import SimpleNamespace
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import Link, SpanContext, SpanKind, Status, StatusCode, TraceFlags

from ai.common import logfmt

from . import _deps

# Semantic conventions version the attribute names follow
SCHEMA_URL = 'https://opentelemetry.io/schemas/1.39.0'
SCOPE = 'rocketride.eaas'

# TASK_STATE values (rocketride.types.task) → attribute values
STATE_NAMES = {0: 'none', 1: 'starting', 2: 'initializing', 3: 'running', 4: 'stopping', 5: 'completed', 6: 'cancelled'}
_STATE_RUNNING = 3
_LIVE_STATES = ('starting', 'initializing', 'running', 'stopping')

# Every label set the SLI counters can take, started at 0 (a series first seen
# at 1 loses that increment to rate()). Mirrors classify_run_outcome.
RUN_KINDS = ('dev', 'deploy')
OUTCOMES = (
    ('ok', 'none', None),
    ('ok', 'ttl', None),
    ('cancelled', 'user', None),
    ('cancelled', 'disconnect', None),
    ('cancelled', 'unregister', None),
    ('error', 'none', 'start_failed'),
    ('error', 'start_failed', 'start_rejected'),
    ('error', 'none', 'start_rejected'),
    ('error', 'start_failed', 'start_error'),
    ('error', 'none', 'start_error'),
    ('error', 'none', 'exit_code'),
    ('error', 'none', 'signal'),
    ('error', 'start_failed', 'start_failed'),
    ('error', 'none', 'engine_lost'),
)

# LLM token counters a task reports over >MET* (ai/common/llm_adapter.report_llm_tokens),
# as gen_ai.token.type values (semconv: input, output; the cache ones are ours)
_TOKEN_COUNTERS = {
    'llm_input_tokens': 'input',
    'llm_output_tokens': 'output',
    'llm_cache_read_tokens': 'cache_read',
    'llm_cache_creation_tokens': 'cache_creation',
}

# A Python node's error/warning location names its folder: .../nodes/<type>/...;
# a C++ node's names only its source file (<type>.cpp), matched to the pipeline
_NODE_PATH = re.compile(r'[\\/]nodes[\\/](?:src[\\/]nodes[\\/])?([A-Za-z0-9_]+)[\\/]')
_SOURCE_FILE = re.compile(r'([A-Za-z0-9_]+)\.(?:cpp|hpp|h)\b')
_ERROR_CODE = re.compile(r'^[A-Za-z][A-Za-z0-9_]{0,31}$')

# Bounds on tracked state, so no workload can grow memory without limit
MAX_LIVE_TASKS = 5000
MAX_OPEN_INVOCATIONS = 256  # per run (concurrent pipes)
MAX_STACK_DEPTH = 64  # nested/reentrant component calls on one pipe
MAX_COMPONENT_TYPES = 500
MAX_PROJECTS_PER_ORG = 50  # rocketride.project.id values kept per customer on metrics, then '_other'
MAX_MODELS_PER_PROVIDER = 50  # gen_ai.request.model values kept per provider on metrics, then '_other'
MAX_ATTRIBUTE_SETS = 2000  # per declared metric
# Attribute sets one org may take on each per-customer metric (outcome, cause, error
# type, server, model, project...), then '_other': bounds the series per tenant
SLI_SETS_PER_ORG = {
    'rocketride.turn': 100,
    'rocketride.task.ended': 60,
    'rocketride.task.duration': 20,
    'rocketride.component.errors': 60,
    'rocketride.component.dependency_errors': 40,
    'rocketride.ws.requests': 20,
}
NO_SOURCE = '_none'
# The run's first error on its span (status and exception event): the text the
# error log line already has, cut here
MAX_CAUSE_LENGTH = 300  # a run whose id names no source component of its pipeline
MAX_ERROR_TYPES = 200  # distinct error.type values, for node errors and for requests each
# Whose failure a failed pass was, by the error a node reported (relay.node_error,
# ai.common.llm_base.provider_error_type): the provider's (down: 5xx, a connection
# error, a timeout), the customer's (their key or quota), or the platform's (a
# request we built wrong, a node or engine error). Anything else: the platform's.
TURN_CAUSES = {
    'provider_unavailable': 'provider',
    'provider_unreachable': 'provider',
    'provider_timeout': 'provider',
    'provider_auth': 'customer',
    'provider_rate_limited': 'customer',
    'provider_error': 'platform',
}
TURN_CAUSE_ORDER = ('platform', 'provider', 'customer')  # the cause of a pass with several: the first
MAX_BUFFERED_CALLS = 200_000  # component calls held for traces, all runs together
TRACE_QUEUE = 1000  # finished pipeline traces waiting for the export thread
MAX_VALUE_LENGTH = 64
# The attributes a relayed call may carry (relay._KEEP)
_RELAY_ATTRIBUTES = frozenset(
    (
        'db.system.name',
        'db.operation.name',
        'server.address',
        'server.port',
        'http.request.method',
        'http.response.status_code',
        'url.scheme',
        'url.template',
        'rpc.system.name',
        'rpc.service',
        'rpc.method',
    )
)
# The attributes a relayed model call may carry (relay.model_call)
_GENAI_ATTRIBUTES = frozenset(('gen_ai.operation.name', 'gen_ai.provider.name', 'gen_ai.request.model'))
# A model call's usage (relay.model_call) as GenAI span attributes
_GENAI_USAGE = {
    'input': 'gen_ai.usage.input_tokens',
    'output': 'gen_ai.usage.output_tokens',
    'cache_read': 'gen_ai.usage.cache_read.input_tokens',
}
# rocketride.ws.client values (modules/task/connections.CLIENTS)
_WS_CLIENTS = frozenset(('user', 'run_key', 'unauthenticated'))
ORIGIN_KEY = 'rocketride.dependency.origin'  # who made a dependency call: 'eaas' or 'pipeline' (a node)
PROJECT_KEY = 'rocketride.project.id'  # the pipeline's project (opaque), bounded per customer on metrics
MAX_PIPELINE_SERVERS = 200  # distinct server.address values from pipelines; later ones are '_other'
MAX_MARKS_PER_PASS = 8  # a node's own error reports kept on one pass (relay.node_error)
MAX_INFLIGHT_REQUESTS = 10000  # WebSocket requests tracked as in flight; later ones are not
MAX_PASS_LINKS = 32  # a run's task span links to at most this many failed or slow passes
RAISED_MATCH_SECONDS = 10.0  # a node's raised exception names the engine's error report within this
MAX_SERVERS_PER_ORG = 20  # of those, per customer: one customer cannot take them all
IDLE_KEY_SECONDS = 900.0  # a (type, org) idle no more is reported 0 this long, then dropped
MAX_RELAYED_PER_PASS = 256  # relayed dependency calls kept for one pipeline trace
MATCH_TOLERANCE = 0.05  # seconds: a relayed call's clock vs the engine events' arrival
MAX_PENDING_MEASUREMENTS = 100_000  # dependency calls waiting to be recorded (otel-deps thread)
MEASURE_INTERVAL = 1.0  # seconds between recordings of the dependency calls
MAX_COMPONENT_SPANS = 1000  # ceiling for RR_OTEL_COMPONENT_SPANS

# Why telemetry was dropped to stay within its bounds (rocketride.telemetry.dropped)
DROP_REASONS = (
    'trace_queue_full',  # the export thread is behind: a sampled pipeline trace is lost
    'span_budget',  # MAX_BUFFERED_CALLS reached: a component call is left out of its trace
    'invocation_cap',  # MAX_OPEN_INVOCATIONS on one run: a call is neither traced nor timed
    'run_evicted',  # MAX_LIVE_TASKS reached: the least recently active run is forgotten
    'stale_call',  # a call open RR_OTEL_STALE_CALL_SECONDS: its invocation is closed unfinished
    'org_overflow',  # RR_OTEL_ORG_CARDINALITY reached: a value is attributed to '_other'
    'error_type_overflow',  # MAX_ERROR_TYPES reached (node errors or request codes): typed '_OTHER'
    'attribute_overflow',  # MAX_ATTRIBUTE_SETS reached on a declared metric
    'error_log_rate_limited',  # a task's error log lines past its rate (ai.modules.task.error_log)
    'error_log_duplicate',  # the same traceback again from the same run
    'slow_rate_limited',  # a slow trace past RR_OTEL_SLOW_TRACES_PER_MINUTE (errors are never limited)
    'dependency_span_cap',  # _deps.MAX_SPANS_PER_TRACE / MAX_HELD: a dependency call left out of its trace
    'dependency_undecided',  # a dependency call whose trace never decided (_deps.UNDECIDED_SECONDS)
    'dependency_metric_error',  # a dependency call that could not be measured
    'dependency_metric_overflow',  # MAX_PENDING_MEASUREMENTS reached: a dependency call is not measured
    'pass_tid_reused',  # the engine repeated a pass trace id on a pipe (its pass boundary drifted): ids not trusted
    'pass_unwound',  # a new pass began while a call of the open one had no leave (an exception): exported unfinished
    'server_overflow',  # a pipeline server past MAX_PIPELINE_SERVERS or a customer's MAX_SERVERS_PER_ORG: '_other'
    'relay_late',  # a relayed call whose pass already ended (or never began): measured, not traced
    'dependency_error_rate_limited',  # a pass kept for a failed dependency call, past its flood guard
    'relay_scope_cap',  # a relayed call past relay.MAX_CALLS_PER_SCOPE in one node call: measured, not traced
    'relay_overflow',  # relay.MAX_PENDING reached in the task process: a call neither measured nor traced
    'relay_unattributed',  # a relayed call made off the node's own call (its thread or loop): measured, not traced
    'dependency_span_error',  # a kept dependency call that could not be exported
    'relay_global',  # a node's call from its global handler (a key check at start): measured, in no pass
    'info_overflow',  # an info gauge (telemetry.InfoGauge) past its observations per collection
    'model_overflow',  # a provider's model past MAX_MODELS_PER_PROVIDER on the metrics: '_other'
    'project_overflow',  # a customer's project past MAX_PROJECTS_PER_ORG on the metrics: '_other'
)

ORG_KEY = 'rocketride.org.id'


def _truthy(value: Optional[str]) -> bool:
    return (value or '').strip().lower() in ('1', 'true', 'yes', 'on')


def _float(environ: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _ns(seconds: float) -> int:
    return int(seconds * 1e9)


# Ids are assigned when a run or a pass begins, so a log line written while it is
# still running can name the trace the run's span gets later. The SDK takes the
# ids from its IdGenerator: _preset_ids makes the next root span use given ones.
_PRESET: 'contextvars.ContextVar[Optional[List[int]]]' = contextvars.ContextVar('rocketride_preset_ids', default=None)


class _PresetIdGenerator(RandomIdGenerator):
    """Random ids, unless _preset_ids set the next root span's trace and span id (each used once)."""

    def generate_trace_id(self) -> int:
        preset = _PRESET.get()
        if preset and preset[0]:
            value, preset[0] = preset[0], 0
            return value
        return super().generate_trace_id()

    def generate_span_id(self) -> int:
        preset = _PRESET.get()
        if preset and preset[1]:
            value, preset[1] = preset[1], 0
            return value
        return super().generate_span_id()


@contextlib.contextmanager
def _preset_ids(trace_id: int, span_id: int):
    """Create the root span inside this block with these ids (only the first span gets them)."""
    token = _PRESET.set([trace_id, span_id])
    try:
        yield
    finally:
        _PRESET.reset(token)


class _SlowLimiter:
    """
    How many slow (not failed) traces are kept: a token bucket per process
    (per_minute), per run and per org (a tenth each, at least 1), so one slow
    deploy or one customer cannot flood the trace store. Errors never pass here.
    """

    def __init__(self, per_minute: float) -> None:
        self._rate = max(0.0, per_minute) / 60.0
        self._lock = threading.Lock()
        self._buckets: 'OrderedDict[str, List[float]]' = OrderedDict()

    def allow(self, task_id: str, org: str) -> bool:
        """Whether one more slow trace may be kept (takes the tokens when it may)."""
        if self._rate <= 0:
            return False
        now = time.monotonic()
        keys = [('', self._rate)]
        if task_id:
            keys.append(('t:' + task_id, max(self._rate / 10, 1 / 60)))
        if org:
            keys.append(('o:' + org, max(self._rate / 10, 1 / 60)))
        with self._lock:
            buckets = []
            for key, rate in keys:
                bucket = self._buckets.get(key)
                if bucket is None:
                    bucket = self._buckets[key] = [rate * 60, now]
                    if len(self._buckets) > 10000:
                        self._buckets.popitem(last=False)
                bucket[0] = min(rate * 60, bucket[0] + (now - bucket[1]) * rate)
                bucket[1] = now
                buckets.append(bucket)
            if all(bucket[0] >= 1 for bucket in buckets):
                for bucket in buckets:
                    bucket[0] -= 1
                return True
        return False


def _new_ids(rng: random.Random) -> Tuple[int, int]:
    """A valid (non-zero) trace id and span id."""
    return rng.getrandbits(128) or 1, rng.getrandbits(64) or 1


class _Invocation:
    """One pass of the pipeline over an object, on one pipe."""

    __slots__ = (
        'start',
        'stack',
        'components',
        'left',
        'calls',
        'dropped',
        'error',
        'call_error',
        'trace_id',
        'span_id',
        'engine_tid',
        'tid_rejected',
        'last_mono',
        'stall_logged',
        'marks',
        'models',
        'raised',
    )

    def __init__(self, start: float, ids: Tuple[int, int] = (0, 0)) -> None:
        self.start = start
        # The pass trace's ids, assigned when it begins (_preset_ids)
        self.trace_id, self.span_id = ids
        self.engine_tid = False  # the trace id is the engine's pass id (its nodes' requests carry it)
        self.tid_rejected = False  # the engine's pass id was not trusted (counted once for the pass)
        self.last_mono = time.monotonic()  # the pass's last enter or leave: its progress
        self.stall_logged = False  # its 'pass stalled' error line was written
        # (component id, wall start, monotonic start)
        self.stack: List[Tuple[str, float, float]] = []
        # (type, wall start, wall end, result, component id)
        self.components: List[Tuple[str, float, float, Optional[str], str]] = []
        # Up to two ids of components that left, kept past the span cap (_carried)
        self.left: List[str] = []
        # A node's own report that its call failed (relay.node_error):
        # (component type, wall time, error type, message)
        self.marks: List[Tuple[str, float, str, str]] = []
        # Logical model calls a node made (relay.model_call): (component type, start ns,
        # end ns, gen_ai attributes, error type, provider message, retries)
        self.models: List[Tuple[Any, ...]] = []
        # Exceptions its nodes' handlers raised (relay._raised), shared with the run's
        # record: [component type, class, message, location, monotonic, engine code, wall]
        self.raised: List[List[Any]] = []
        # Dependency calls the subprocess relayed (relay.py): (component type, name, start ns, end ns,
        # attributes, error type, span id: the one its traceparent carried, 0 if none)
        self.calls: List[Tuple[str, str, int, int, Dict[str, Any], Optional[str], int]] = []
        self.dropped = 0
        self.error = False
        self.call_error = False  # a relayed dependency call failed (the pass itself may not have)


def _exception_attributes(cause: Mapping[str, str], message: str) -> Dict[str, Any]:
    """An OTel 'exception' event's attributes (semantic conventions) from an error log record."""
    attributes: Dict[str, Any] = {'exception.message': message}
    if cause.get('type'):
        attributes['exception.type'] = str(cause['type'])[:MAX_VALUE_LENGTH]
    path, _, line = str(cause.get('location') or '').rpartition(':')
    if path and line.isdigit():
        attributes['code.file.path'] = path[-MAX_CAUSE_LENGTH:]
        attributes['code.line.number'] = int(line)
    elif cause.get('location'):
        attributes['code.file.path'] = str(cause['location'])[-MAX_CAUSE_LENGTH:]
    return attributes


def _carried(invocation: '_Invocation') -> bool:
    """
    The pass carried a question or a document: data went through two components
    or more (closed or still open). The engine also runs passes that enter one
    component alone (a component's own open or close step, under a millisecond
    to a few hundred): no question there, and no answer time.
    """
    ids = set(invocation.left) | {frame[0] for frame in invocation.stack}
    return len(ids) >= 2


class _Run:
    """What is tracked for one live run."""

    __slots__ = (
        'begin_wall',
        'begin_mono',
        'state',
        'startup_recorded',
        'completed',
        'failed',
        'tokens',
        'invocations',
        'types',
        'org',
        'project',
        'run_kind',
        'trace_id',
        'span_id',
        'keep_reason',
        'pass_tids',
        'debugger',
        'source_type',
        'raised',
        'pass_links',
    )

    def __init__(self) -> None:
        # the source component's type (chat, webhook, dropper...): '' until a
        # trace event brings the pipeline, NO_SOURCE when it names none
        self.source_type = ''
        # The last exception a node's handler raised (relay._raised): [component type,
        # class, message, location, monotonic time, the engine's error code]; the engine's
        # error report that follows is that node's
        self.raised: Optional[List[Any]] = None
        # The run's failed and slow passes that were kept: (trace id, span id, why), the
        # task span's links to them (MAX_PASS_LINKS)
        self.pass_links: List[Tuple[int, int, str]] = []
        self.begin_wall: Optional[float] = None
        self.begin_mono: Optional[float] = None
        self.state = 0
        self.startup_recorded = False
        self.completed = 0
        self.failed = 0
        self.tokens: Dict[str, int] = {}
        self.invocations: Dict[Any, _Invocation] = {}
        # component id → provider, from the run's pipeline
        self.types: Optional[Dict[str, str]] = None
        self.org = ''
        # The pipeline's project id (opaque, stable across runs and deploys of the
        # same pipeline): ties an incident to the pipeline a customer names
        self.project = ''
        self.run_kind = 'dev'
        # The run trace's ids (set by _admit), and why it must be kept whatever the
        # outcome: an error log named it ('error_log') or a kept pass links to it
        # ('pass_kept'). A trace id that appears anywhere must lead to a trace.
        self.trace_id = 0
        self.span_id = 0
        self.keep_reason = ''
        # pipe → the last pass trace id the engine gave it: a repeat means its
        # pass boundary drifted, and the trace id is not trusted (pass_tid_reused)
        self.pass_tids: Dict[Any, int] = {}
        # A debugger is attached (a dev run paused at a breakpoint makes no
        # progress on purpose): its passes are not counted as stalled
        self.debugger = False


class Telemetry:
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
            org_attribute=environ.get('RR_OTEL_ORG_ATTRIBUTE', 'true').strip().lower()
            not in ('0', 'false', 'no', 'off'),
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
        org_attribute: bool = True,
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
        self._span_cap = min(MAX_COMPONENT_SPANS, max(0, component_span_cap))
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
        """The org id to attach, or None: off, unknown, or past the cardinality cap ('_other')."""
        if not self._org_attribute or not org_id:
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
        """The org id (bounded), and with a project, its project id (MAX_PROJECTS_PER_ORG per org)."""
        org = self._org(org_id)
        if org is not None:
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

    def task_event(self, task_id: str, message: Dict[str, Any], identity: Callable[[], Mapping[str, Any]]) -> None:
        event = message.get('event')
        body = message.get('body')
        if not isinstance(body, dict):
            return
        if event == 'apaevt_status_update':
            self._status(task_id, body, identity())
        elif event == 'apaevt_task':
            if body.get('action') == 'begin':
                self._begin(task_id, identity())
        elif event == 'apaevt_status_metrics':
            self._llm_tokens(task_id, body.get('metrics'))
        elif event in ('apaevt_status_error', 'apaevt_status_warning'):
            self._node_report(task_id, body.get('message'), 'error' if event == 'apaevt_status_error' else 'warning')

    def component_trace(
        self,
        task_id: str,
        op: str,
        pipe_index: Any,
        component_id: str,
        event_time: Optional[float],
        trace_body: Any,
        pipeline: Optional[Dict[str, Any]],
    ) -> None:
        if op not in ('enter', 'leave') or not component_id:
            return
        wall = event_time or time.time()
        mono = time.monotonic()
        finished: Optional[Tuple[_Invocation, _Run]] = None
        reused = False
        record: Optional[Tuple[float, str, Optional[str]]] = None
        stale: Optional[Tuple[_Invocation, _Run]] = None
        stale_reason = ''
        over_budget = False

        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            if not run.source_type and pipeline:
                run.source_type = self._source_type_locked(task_id, pipeline)
            invocation = run.invocations.get(pipe_index)
            if op == 'enter' and invocation is not None and invocation.stack:
                if self._stale_call and mono - invocation.stack[0][2] >= self._stale_call:
                    stale_reason = 'stale_call'
                elif invocation.engine_tid and _other_pass(trace_body, invocation.trace_id):
                    # The engine began a new pass while one of this pass's calls
                    # is still open: an exception skipped its leave (the engine
                    # ended the level without an event)
                    stale_reason = 'pass_unwound'
                if stale_reason:
                    # Its leave never came: close it, and this enter starts a new pass
                    del run.invocations[pipe_index]
                    self._buffered -= len(invocation.components) + len(invocation.calls)
                    stale, invocation = (invocation, run), None
            if op == 'enter':
                if invocation is None and len(run.invocations) >= MAX_OPEN_INVOCATIONS:
                    over_budget = True
                else:
                    if invocation is None:
                        invocation = run.invocations[pipe_index] = _Invocation(wall, _new_ids(self._random))
                    reused = self._adopt_pass_id(run, pipe_index, invocation, trace_body)
                    if len(invocation.stack) >= MAX_STACK_DEPTH:
                        # A lost leave (killed process) must not grow the stack forever
                        invocation.stack.pop(0)
                    invocation.stack.append((component_id, wall, mono))
                    invocation.last_mono = mono
                    invocation.stall_logged = False  # progress: a later stall is logged again
                    if run.types is None:
                        # Known from the first enter: a call that never leaves is still named
                        self._component_type(run, component_id, pipeline)
            elif invocation is not None:
                invocation.last_mono = mono
                invocation.stall_logged = False
                # leave: pair by identity (reentrant calls interleave on one pipe)
                record, finished, over_budget = self._leave(
                    run, invocation, pipe_index, component_id, wall, mono, trace_body, pipeline
                )

        if reused:
            self.count_dropped('pass_tid_reused')
        if stale:
            self.count_dropped(stale_reason)
            self._emit_invocation(task_id, *stale, end=wall, unclosed=True, why=stale_reason)
        if op == 'enter':
            if over_budget:
                self.count_dropped('invocation_cap')
            return
        if record is None:
            return
        duration, component_type, result = record
        attributes = {'rocketride.component.type': component_type}
        if result:
            attributes['rocketride.component.result'] = result
        self._component.record(duration, attributes)
        if over_budget:
            self.count_dropped('span_budget')
        if finished:
            self._emit_invocation(task_id, *finished, end=wall)

    def _adopt_pass_id(self, run: _Run, pipe_index: Any, invocation: _Invocation, trace_body: Any) -> bool:
        """
        Make the engine's pass id ('tid' on an enter: the one its nodes' model
        server requests carry) the pass's trace id. Under the lock.

        A pass that began without one (an engine without it, a tool call's
        enter) takes the first it sees. A tid this pipe already had is not
        trusted: the engine's pass boundary drifted (a lost leave), and two
        passes must not share a trace. True when that happened (counted by the
        caller, outside the lock).
        """
        if invocation.engine_tid or invocation.tid_rejected:
            return False
        tid = trace_body.get('tid') if isinstance(trace_body, dict) else None
        if not (isinstance(tid, str) and _TRACE_ID.match(tid)) or not int(tid, 16):
            return False
        trace_id = int(tid, 16)
        if run.pass_tids.get(pipe_index) == trace_id:
            invocation.tid_rejected = True  # its other enters carry it too: one count for the pass
            return True
        run.pass_tids[pipe_index] = trace_id
        invocation.trace_id, invocation.engine_tid = trace_id, True
        return False

    def _leave(self, run, invocation, pipe_index, component_id, wall, mono, trace_body, pipeline):
        """A component's leave: (duration record, finished invocation, over budget). Under the lock."""
        for index in range(len(invocation.stack) - 1, -1, -1):
            if invocation.stack[index][0] == component_id:
                _, start_wall, start_mono = invocation.stack.pop(index)
                break
        else:
            return None, None, False
        component_type = self._component_type(run, component_id, pipeline)
        result = trace_body.get('result') if isinstance(trace_body, dict) else None
        result = result if result in ('continue', 'skip', 'error') else None
        if result == 'error':
            invocation.error = True
        if len(invocation.left) < 2 and str(component_id) not in invocation.left:
            invocation.left.append(str(component_id))
        over_budget = False
        if len(invocation.components) >= self._span_cap:
            invocation.dropped += 1
        elif self._buffered >= MAX_BUFFERED_CALLS:
            invocation.dropped += 1
            over_budget = True
        else:
            invocation.components.append((component_type, start_wall, max(wall, start_wall), result, str(component_id)))
            self._buffered += 1
        finished = None
        if not invocation.stack:
            del run.invocations[pipe_index]
            self._buffered -= len(invocation.components) + len(invocation.calls)
            finished = (invocation, run)
        return (max(0.0, mono - start_mono), component_type, result), finished, over_budget

    def task_restarted(self, task_id: str) -> None:
        """Same run, new subprocess: its pipeline may differ, open calls are gone, counts restart at 0."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            self._release(run)
            open_passes = list(run.invocations.values())
            run.invocations.clear()
        # A pass open across the restart ended with the old process, as at a
        # run's end: its turn (abandoned, unless it had failed) and its model calls count
        now = time.time()
        for invocation in open_passes:
            self._emit_invocation(task_id, invocation, run, end=now, unclosed=True, why='task_end')
        with self._lock:
            # The new process may run another pipeline: its source and raised errors are not the old one's
            run.types = None
            run.source_type = ''
            run.raised = None
            run.completed = run.failed = 0
            run.tokens = {}

    def task_ended(
        self,
        task_id: str,
        identity: Mapping[str, Any],
        outcome: str,
        stop_reason: Optional[str],
        error_type: Optional[str],
        error_code: Optional[str] = None,
        cause: Optional[Mapping[str, str]] = None,
        exit_cause: Optional[Mapping[str, Any]] = None,
    ) -> None:
        mono = time.monotonic()
        now = time.time()
        with self._lock:
            run = self._runs.pop(task_id, None)
            if run is not None:
                self._release(run)
            self._ended_ids[task_id] = None
            if len(self._ended_ids) > MAX_LIVE_TASKS:
                self._ended_ids.popitem(last=False)
        run_kind = (run.run_kind if run else None) or identity.get('run_kind') or 'dev'
        org_id = (run.org if run else None) or identity.get('org_id')
        project = run.project if run is not None else str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH]
        stop = stop_reason or 'none'
        error_type = error_type if outcome == 'error' else None

        ended = self._ended_attributes(run_kind, outcome, stop, error_type, org_id, project)
        exit_signal = str((exit_cause or {}).get('signal') or '')[:16]
        oom_suspected = bool((exit_cause or {}).get('oom_suspected'))
        if error_type and error_code:
            # why a start failed or a run ended in error (the engine's codes: a fixed set), bounded
            ended['rocketride.engine.error_code'] = self._bounded_error_type(
                str(error_code) if _ERROR_CODE.match(str(error_code)) else '_OTHER', 'engine'
            )
        if exit_signal:
            ended['rocketride.task.exit_signal'] = self._bounded_error_type(exit_signal, 'signal')
            ended['rocketride.task.oom_suspected'] = oom_suspected
        self._ended.add(1, self._bounded('rocketride.task.ended', ended))
        if run is None or run.begin_mono is None:
            return

        duration = max(0.0, mono - run.begin_mono)

        # Invocations still open died with the run
        for invocation in list(run.invocations.values()):
            invocation.error = invocation.error or outcome == 'error'
            self._emit_invocation(task_id, invocation, run, end=now, unclosed=True, why='task_end')

        # The run's trace, with the ids it was given when it began: always for a
        # run that ended in error or that an error log named, otherwise at the
        # sample ratio (0: errors only)
        trace_id = ''
        reason = 'error' if error_type else run.keep_reason
        if not reason and self._slow_run and run_kind != 'deploy' and duration >= self._slow_run:
            reason = 'slow' if self._slow_allowed(task_id, str(org_id or '')) else ''
        if not reason and self._keep_success():
            reason = 'ratio'
        # Measured whatever the sampling; the exemplar names the run's trace when it is kept
        self._duration.record(
            duration,
            self._bounded(
                'rocketride.task.duration',
                self._with_org(
                    {'rocketride.task.run_kind': run_kind, 'rocketride.task.outcome': outcome}, org_id, project
                ),
            ),
            context=_exemplar(run.trace_id, run.span_id, bool(reason)),
        )
        if reason:
            with _preset_ids(run.trace_id, run.span_id):
                span = self._tracer.start_span(
                    'task',
                    kind=SpanKind.INTERNAL,
                    start_time=_ns(run.begin_wall),
                    attributes=self._with_org(
                        {
                            'rocketride.task.id': task_id,
                            'rocketride.task.run_kind': run_kind,
                            'rocketride.task.outcome': outcome,
                            'rocketride.task.stop_reason': stop,
                            'rocketride.sampling.reason': reason,
                            **({'rocketride.project.id': run.project} if run.project else {}),
                        },
                        org_id,
                    ),
                    # its failed and slow passes, each a trace of its own
                    links=[
                        Link(
                            SpanContext(tid, sid, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED)),
                            {'rocketride.sampling.reason': why},
                        )
                        for tid, sid, why in run.pass_links
                    ]
                    or None,
                )
            if error_type:
                span.set_attribute('error.type', error_type)
                if error_code:
                    span.set_attribute('rocketride.engine.error_code', str(error_code)[:MAX_VALUE_LENGTH])
                if exit_signal:
                    span.set_attribute('rocketride.task.exit_signal', exit_signal)
                    span.set_attribute('rocketride.task.oom_suspected', oom_suspected)
                status = f'run ended in error ({error_type})'
                if exit_signal:
                    status += f' by {exit_signal}' + (' (out of memory suspected)' if oom_suspected else '')
                message = str((cause or {}).get('message') or '')[:MAX_CAUSE_LENGTH]
                if message:
                    # What failed, on the trace itself: the run's first error
                    status += f': {message}'
                    span.add_event('exception', _exception_attributes(cause, message))
                span.set_status(Status(StatusCode.ERROR, status))
            span.end(end_time=_ns(max(now, run.begin_wall)))
            trace_id = format(span.get_span_context().trace_id, '032x')

        # One structured line per run: the link between logs, traces and metrics
        try:
            line = {
                # A standard level field, so log pipelines can keep errors only
                'level': 'error' if outcome == 'error' else 'info',
                'event': 'rocketride.task.ended',
                'task_id': task_id,
                'trace_id': trace_id,
                'outcome': outcome,
                'stop_reason': stop,
                'error_type': error_type,
                'error_code': error_code if error_type else None,
                'run_kind': run_kind,
                'duration_s': round(duration, 3),
            }
            if project:
                line['project_id'] = project  # the pipeline the customer names
            if exit_signal:
                line['exit_signal'] = exit_signal
                line['oom_suspected'] = oom_suspected
            org = self._org(org_id)
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('run_ended_log')

    def error_log_trace(self, task_id: str) -> str:
        """The run's trace id for an error log line, and keep that trace (the log must lead to it)."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None or not run.trace_id:
                return ''
            if not run.keep_reason:
                run.keep_reason = 'error_log'
            return format(run.trace_id, '032x')

    def runs_lost(self, identities: List[Mapping[str, Any]]) -> None:
        """Runs that were in progress when a previous EaaS process died."""
        for identity in identities:
            self._ended.add(
                1,
                self._ended_attributes(
                    identity.get('run_kind') or 'dev',
                    'error',
                    'none',
                    'engine_lost',
                    identity.get('org_id'),
                    str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH],
                ),
            )

    def request_scope(
        self, method: str, traceparent: Any = None, identity: Optional[Mapping[str, Any]] = None
    ) -> Tuple[Any, Any]:
        """
        A DAP request about to be handled: (its _deps.Request, the context to make current).

        No span is started: the request's ids are set now, so its trace is
        current while the handler runs (a log line's trace_id, the HTTP
        instrumentations' parent), and its SERVER span is built at
        request_done, only when kept.
        """
        parent = _parent_context(traceparent)
        if parent is not None:
            trace_id = trace.get_current_span(parent).get_span_context().trace_id
            span_id = self._random.getrandbits(64) or 1
        else:
            trace_id, span_id = _new_ids(self._random)
        request = _deps.Request(trace_id, span_id, parent)
        if not any(fnmatch.fnmatchcase(method, pattern) for pattern in self._slow_exclude):
            with self._lock:
                if len(self._inflight) < MAX_INFLIGHT_REQUESTS:
                    self._inflight[span_id] = [
                        method[:MAX_VALUE_LENGTH],
                        time.monotonic(),
                        dict(identity or {}),
                        request,
                        False,
                    ]
        current = SpanContext(trace_id, span_id, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
        return request, trace.set_span_in_context(trace.NonRecordingSpan(current))

    def request_done(
        self,
        method: str,
        error_type: Optional[str],
        duration_s: float,
        traceparent: Any = None,
        span: Any = None,
        identity: Optional[Mapping[str, Any]] = None,
    ) -> None:
        attributes = {'rpc.system.name': 'rocketride_dap', 'rpc.method': method[:MAX_VALUE_LENGTH]}
        if error_type:
            # A response code comes from any exception's .code: bounded like node codes
            code = error_type[:MAX_VALUE_LENGTH]
            attributes['error.type'] = self._bounded_error_type(
                code if _ERROR_CODE.match(code) else '_OTHER', 'request'
            )
        identity = identity or {}
        client = identity.get('client') if identity.get('client') in _WS_CLIENTS else None
        if client:
            attributes['rocketride.ws.client'] = client  # how the caller signed in: three values
        duration = max(0.0, duration_s)
        request = span if isinstance(span, _deps.Request) else None
        if request is not None:
            with self._lock:
                self._inflight.pop(request.span_id, None)

        # Kept: failed, named by an ERROR log, slow (not a long-lived method),
        # or, for a caller that traces, at the ratio (the caller's sampling
        # decision is respected: only a sampled parent counts)
        parent = request.parent if request is not None else _parent_context(traceparent)
        slow = (
            self._slow_request
            and duration >= self._slow_request
            and not any(fnmatch.fnmatchcase(method, pattern) for pattern in self._slow_exclude)
        )
        keep = 'error' if error_type else ''
        if not keep and request is not None and request.error_logged:
            keep = 'error_log'
        if not keep and slow and (request is not None or parent is not None) and self._slow_allowed('', ''):
            keep = 'slow'
        if not keep and parent is not None and self._keep_success():
            keep = 'ratio'

        # Measured whatever the sampling; the exemplar names the request's span when it is kept
        exemplar = _exemplar(request.trace_id, request.span_id, bool(keep)) if request is not None else None
        self._request.record(duration, attributes, context=exemplar)
        # Per customer: whether their requests fail, and how (no method: bounded series;
        # which method is on the exemplar's trace)
        org = identity.get('org_id')
        if org:
            counted = {'rocketride.ws.client': client or '_unknown'}
            if error_type:
                counted['error.type'] = attributes['error.type']
            self._requests.add(
                1, self._bounded('rocketride.ws.requests', self._with_org(counted, org)), context=exemplar
            )

        span_attributes = dict(attributes)
        if org and self._org(org):
            span_attributes[ORG_KEY] = self._org(org)
        if identity.get('connection_id'):
            span_attributes['rocketride.connection.id'] = str(identity['connection_id'])[:MAX_VALUE_LENGTH]
        links = []
        task_id = identity.get('task_id')
        if task_id:
            span_attributes['rocketride.task.id'] = str(task_id)[:MAX_VALUE_LENGTH]
            run_link = self._run_link(str(task_id), bool(keep))
            if run_link is not None:
                links.append(run_link)

        if request is not None:
            # Its HTTP/AWS spans wait for this decision; its database and Redis calls too
            if keep:
                self._export_request(
                    request, dict(span_attributes, **{'rocketride.sampling.reason': keep}), error_type, links
                )
            self._spans.decide(request.span_id, bool(keep))
            return
        if parent is not None and keep:
            attributes = dict(span_attributes, **{'rocketride.sampling.reason': keep})
            end = time.time()
            # semconv rpc-spans: the span is named after the method
            span = self._tracer.start_span(
                attributes['rpc.method'],
                context=parent,
                kind=SpanKind.SERVER,
                start_time=_ns(end - duration),
                attributes=attributes,
                links=links,
            )
            if error_type:
                span.set_status(Status(StatusCode.ERROR, attributes['error.type']))
            span.end(end_time=_ns(end))

    def _run_link(self, task_id: str, kept: bool) -> Optional[Link]:
        """A link to a live run's trace; a kept request keeps the run's trace too (the link must lead somewhere)."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None or not run.trace_id:
                return None
            if kept and not run.keep_reason:
                run.keep_reason = 'request_kept'
            ids = (run.trace_id, run.span_id)
        return Link(SpanContext(ids[0], ids[1], is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED)))

    def _export_request(
        self, request: Any, attributes: Dict[str, Any], error_type: Optional[str], links: Optional[List[Any]] = None
    ) -> None:
        """The kept request's SERVER span (with its preset ids) and a CLIENT span per database/Redis call."""
        with _preset_ids(request.trace_id, request.span_id):
            span = self._tracer.start_span(
                attributes['rpc.method'],
                context=request.parent,
                kind=SpanKind.SERVER,
                start_time=request.start_ns,
                attributes=dict(request.attributes, **attributes),
                links=links or None,
            )
        if error_type:
            span.set_status(Status(StatusCode.ERROR, attributes['error.type']))
        if request.dropped:
            span.set_attribute('rocketride.calls.dropped', request.dropped)
        span.end()
        parent = trace.set_span_in_context(trace.NonRecordingSpan(span.get_span_context()))
        for call in request.calls:
            self._export_call(call, parent)

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
                self._relayed(task_id, record, pipeline)

    def _relayed(self, task_id: str, body: Mapping[str, Any], pipeline: Optional[Dict[str, Any]]) -> None:
        if body.get('dropped'):
            reason = body.get('reason') if body.get('reason') in ('relay_overflow',) else 'dependency_span_cap'
            self.count_dropped(reason, int(body['dropped']))
            return
        if body.get('k') == 'err':
            self._node_error_mark(task_id, body, pipeline)
            return
        if body.get('k') == 'gen':
            self._model_call(task_id, body, pipeline)
            return
        if body.get('k') == 'exc':
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
                invocation.marks.append((component_type, wall, error_type, message))
                invocation.call_error = True  # the pass is kept, as for a failed call
        if reason:
            self.count_dropped(reason)

    def error_logged(self) -> None:
        """An ERROR log line carries the current trace id: that request's trace must be kept."""
        request = _deps.current_request()
        if request is not None:
            request.error_logged = True

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
        org = self._org(attributes.get('_org')) or ''  # who made the call (relayed, bounded): never a label
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
            if org:
                failed[ORG_KEY] = org
            if attributes.get('_project'):
                failed[PROJECT_KEY] = self._bounded_project(org, str(attributes['_project']))
            self._dependency_errors.add(1, self._bounded('rocketride.component.dependency_errors', failed))

    def observe_connections(self, count: Callable[[], int]) -> None:
        self._connections = count

    # ── Internals ────────────────────────────────────────────────────────────

    def _begin(self, task_id: str, identity: Mapping[str, Any]) -> None:
        mono = time.monotonic()
        wall = time.time()
        run_kind = identity.get('run_kind') or 'dev'
        with self._lock:
            self._ended_ids.pop(task_id, None)  # a new run of the same task
            run = self._runs.get(task_id)
            if run is not None and run.begin_mono is not None:
                return  # a duplicate begin: the run is already counted
            if run is None:
                run = self._admit(task_id)
            else:
                self._runs.move_to_end(task_id)
            run.begin_wall, run.begin_mono = wall, mono
            run.run_kind = run_kind
            run.org = str(identity.get('org_id') or '')
            run.project = str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH]
        self._count_evictions()
        self._started.add(1, self._with_org({'rocketride.task.run_kind': run_kind}, identity.get('org_id')))

    def _admit(self, task_id: str) -> _Run:
        """Track a run; past the cap the least recently active run is forgotten (never a refusal). Under the lock."""
        if len(self._runs) >= MAX_LIVE_TASKS:
            _, evicted = self._runs.popitem(last=False)
            self._release(evicted)
            self._evicted += 1  # counted by _count_evictions, outside the lock
        run = self._runs[task_id] = _Run()
        run.trace_id, run.span_id = _new_ids(self._random)
        return run

    def _count_evictions(self) -> None:
        with self._lock:
            evicted, self._evicted = self._evicted, 0
        if evicted:
            self.count_dropped('run_evicted', evicted)

    def _release(self, run: _Run) -> None:
        """Give back the buffered-call budget of a run's open invocations. Under the lock."""
        for invocation in run.invocations.values():
            self._buffered -= len(invocation.components) + len(invocation.calls)

    def _status(self, task_id: str, body: Dict[str, Any], identity: Mapping[str, Any]) -> None:
        state = body.get('state')
        completed = body.get('completedCount') or 0
        failed = body.get('failedCount') or 0
        startup = None
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                if task_id in self._ended_ids:
                    return  # late status of a run that ended: already counted
                run = self._admit(task_id)  # status before begin (starting)
                run.run_kind = identity.get('run_kind') or 'dev'
                run.org = str(identity.get('org_id') or '')
            else:
                # Least recently active first out: a live run reports status
                self._runs.move_to_end(task_id)
            if 'debuggerAttached' in body:
                run.debugger = bool(body.get('debuggerAttached'))
            if isinstance(state, int):
                run.state = state
                if state == _STATE_RUNNING and not run.startup_recorded and run.begin_mono is not None:
                    run.startup_recorded = True
                    startup = max(0.0, time.monotonic() - run.begin_mono)
            # Counts restart at zero after a restart: a decrease is a new baseline
            done = completed - run.completed if completed >= run.completed else completed
            lost = failed - run.failed if failed >= run.failed else failed
            run.completed, run.failed = completed, failed
            run_kind, org = run.run_kind, run.org

        self._count_evictions()
        if startup is not None:
            self._startup.record(startup, {'rocketride.task.run_kind': run_kind})
        if done > 0:
            self._objects.add(done, self._with_org({'rocketride.object.outcome': 'completed'}, org))
        if lost > 0:
            self._objects.add(lost, self._with_org({'rocketride.object.outcome': 'failed'}, org))

    def _llm_tokens(self, task_id: str, metrics: Any) -> None:
        """Token deltas from the task's cumulative >MET* snapshot (every LLM node reports it)."""
        counters = metrics.get('counters') if isinstance(metrics, dict) else None
        if not isinstance(counters, dict):
            return
        deltas: List[Tuple[str, int]] = []
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            for name, token_type in _TOKEN_COUNTERS.items():
                value = counters.get(name)
                if not isinstance(value, (int, float)):
                    continue
                value = int(value)
                previous = run.tokens.get(name, 0)
                delta = value - previous if value >= previous else value
                run.tokens[name] = value
                if delta > 0:
                    deltas.append((token_type, delta))
            org = run.org
        for token_type, delta in deltas:
            self._tokens.add(delta, self._with_org({'gen_ai.token.type': token_type}, org))

    def _node_report(self, task_id: str, message: Any, severity: str) -> None:
        """A node's error or warning: '<code>*<text>*<location>...', attributed to the node type.

        'Cancelled' is a requested stop winding down (every terminate emits a burst), not an error.
        """
        if not isinstance(message, str):
            return
        code = message.split('*', 1)[0]
        if code == 'Cancelled':
            return
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            known = set((run.types or {}).values())
            org, project = run.org, run.project
            # The node whose handler just raised (relay._raised): named, not guessed from the text
            raised = run.raised
            if severity == 'error' and raised is not None:
                run.raised = None
                if time.monotonic() - raised[4] <= RAISED_MATCH_SECONDS:
                    raised[5] = code if _ERROR_CODE.match(code) else '_OTHER'
                else:
                    raised = None
            else:
                raised = None
        error_type = None
        if severity == 'error':
            error_type = self._bounded_error_type(code if _ERROR_CODE.match(code) else '_OTHER', 'node')
        match = _NODE_PATH.search(message)
        node = match.group(1) if match else None
        if raised is not None:
            component_type = raised[0]
        elif node is None:
            # A C++ node's own source file (parse.cpp → parse), only if the pipeline has it
            node = next((stem for stem in _SOURCE_FILE.findall(message) if stem in known), None)
            component_type = self._bounded_type(node) if node else '_engine'
        elif not known or node in known:
            component_type = self._bounded_type(node)
        else:
            component_type = '_other'
        attributes = {'rocketride.component.type': component_type, 'rocketride.component.severity': severity}
        if error_type:
            # error.type on errors only; a warning is not an error (semconv)
            attributes['error.type'] = error_type
        else:
            # a warning's code (the engine's Ec name), bounded as error types are: the
            # logs keep errors only, so without it a warning would be a bare count
            attributes['rocketride.component.code'] = self._bounded_error_type(
                code if _ERROR_CODE.match(code) else '_OTHER', 'warning'
            )
        self._component_errors.add(
            1, self._bounded('rocketride.component.errors', self._with_org(attributes, org, project))
        )

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

    def _bounded_type(self, component_type: str) -> str:
        with self._lock:
            return self._bounded_type_locked(component_type)

    def _bounded_type_locked(self, component_type: str) -> str:
        if component_type in self._component_types:
            return component_type
        if len(self._component_types) >= MAX_COMPONENT_TYPES:
            return '_other'
        self._component_types.add(component_type)
        return component_type

    def _component_type(self, run: _Run, component_id: str, pipeline: Optional[Dict[str, Any]]) -> str:
        """The component's provider (its node type), bounded; never the user-chosen id. Under the lock."""
        if run.types is None:
            run.types = {}
            for component in (pipeline or {}).get('components') or []:
                if isinstance(component, dict) and component.get('id'):
                    run.types[str(component['id'])] = str(component.get('provider') or '_unknown')[:MAX_VALUE_LENGTH]
        provider = run.types.get(component_id)
        return self._bounded_type_locked(provider) if provider else '_unknown'

    def _turn_outcome(self, invocation: _Invocation, why: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        (outcome, cause, error type) of a pass that ended. why: '' when it finished,
        'task_end' when its run ended with it open, 'shutdown', or the reason a
        stuck or unwound pass was closed.
        """
        if invocation.marks:
            kinds = [kind for _, _, kind, _ in invocation.marks]
            causes = [TURN_CAUSES.get(kind, 'platform') for kind in kinds]
            cause = next(c for c in TURN_CAUSE_ORDER if c in causes)
            return 'error', cause, kinds[causes.index(cause)]
        if invocation.error:
            return 'error', 'platform', 'component_error' if not why else why
        if why == 'task_end':
            return 'abandoned', None, None  # the user stopped the run, or it ended, with the pass open
        if why:
            return 'error', 'platform', why  # stuck, unwound, or the engine shutting down
        return 'ok', None, None

    def _turn_server(self, invocation: _Invocation, run: _Run, failed: bool) -> Optional[str]:
        """
        The LLM server a pass called (an LLM node's or an agent's call): the one
        whose call failed for a failed pass, else the first. Bounded as the
        dependency metrics' servers.
        """
        # HTTP calls of LLM nodes and agents: not the model server's DAP calls (ours,
        # with alerts of their own: a customer's model it is not)
        calls = [
            c
            for c in invocation.calls
            if c[0].startswith(('llm_', 'agent_')) and (c[4] or {}).get('rpc.system.name') != 'rocketride_dap'
        ]
        if failed:
            calls = [c for c in calls if c[5]] or calls
        if not calls:
            return None
        server = (calls[0][4] or {}).get('server.address')
        if not server:
            return None
        return self._bounded_pipeline_server(self._org(run.org) or '', str(server)[:MAX_VALUE_LENGTH])

    def _source_type_locked(self, task_id: str, pipeline: Mapping[str, Any]) -> str:
        """The run's source component type (its provider), bounded like component types; under the lock."""
        source = task_id.partition('.')[2]
        for component in pipeline.get('components') or []:
            if isinstance(component, dict) and component.get('id') == source and component.get('provider'):
                return self._bounded_type_locked(str(component['provider'])[:MAX_VALUE_LENGTH])
        return NO_SOURCE

    def _count_turn(
        self, invocation: _Invocation, run: _Run, why: str, end: Optional[float] = None, kept: bool = False
    ) -> None:
        outcome, cause, error_type = self._turn_outcome(invocation, why)
        attributes = {'rocketride.turn.outcome': outcome, 'rocketride.task.run_kind': run.run_kind}
        if cause:
            attributes['rocketride.turn.cause'] = cause
            attributes['error.type'] = self._bounded_error_type(error_type, 'node')
        # The LLM provider a question used: whether a provider is down is the
        # share of its questions failing (a failed question makes many calls)
        server = self._turn_server(invocation, run, outcome == 'error')
        if server:
            attributes['server.address'] = server
        model = self._turn_model(invocation, outcome == 'error')
        attributes.update(model)
        # Whether it carried a question or a document (data through two components or
        # more), not only a component's own open or close step: the turns SLO counts these
        carried = _carried(invocation)
        attributes['rocketride.turn.carried'] = 'true' if carried else 'false'
        exemplar = _exemplar(invocation.trace_id, invocation.span_id, kept)
        self._turns.add(
            1, self._bounded('rocketride.turn', self._with_org(attributes, run.org, run.project)), context=exemplar
        )
        # Timed: a pass that carried a question or a document (answered, failed, or
        # stopped by the user while open: how long they waited before giving up)
        if end is not None and outcome in ('ok', 'error', 'abandoned') and carried:
            timed = {'rocketride.turn.outcome': outcome, 'rocketride.task.run_kind': run.run_kind}
            if server:
                timed['server.address'] = server
            timed.update(model)
            # the answer time a chat user waits is not a document's: the latency SLO is per source type
            if run.source_type and run.source_type != NO_SOURCE:
                timed['rocketride.source.type'] = run.source_type
            self._turn_duration.record(
                max(0.0, end - invocation.start), self._bounded('rocketride.turn.duration', timed), context=exemplar
            )
        for _type, start, finish, gen_attributes, gen_error, _m, _r, usage, _a, span_id in invocation.models:
            self._measure_model_call(
                gen_attributes,
                (finish - start) / 1e9,
                gen_error,
                usage,
                _exemplar(invocation.trace_id, span_id, kept),
            )

    def _turn_model(self, invocation: _Invocation, failed: bool) -> Dict[str, str]:
        """The provider and model a pass asked (the failed call's for a failed pass, else the last), bounded."""
        models = invocation.models
        if failed:
            models = [m for m in models if m[4]] or models
        if not models:
            return {}
        attributes = models[-1][3]
        provider = attributes.get('gen_ai.provider.name') or '_unknown'
        return {
            'gen_ai.provider.name': provider,
            'gen_ai.request.model': self._bounded_model(provider, attributes.get('gen_ai.request.model') or ''),
        }

    def _emit_invocation(
        self, task_id: str, invocation: _Invocation, run: _Run, end: float, unclosed=False, why: str = ''
    ) -> None:
        """Decide whether the pass's trace is kept (failed and slow ones always), count its outcome and queue it."""
        duration = max(0.0, end - invocation.start)
        dropped = ''
        if invocation.error:
            reason = 'error'
        elif self._slow and duration >= self._slow and self._slow_allowed(task_id, run.org):
            reason = 'slow'
        elif invocation.call_error and self._dependency_error_limit.allow(task_id, run.org):
            # A dependency failed inside a pass that went on (a retried 429, a
            # 401 the node reported as a warning): kept, under a flood guard of
            # its own (a provider's outage must not spend the slow traces' budget)
            reason = 'dependency_error'
        elif invocation.call_error and not self._keep_success():
            reason, dropped = '', 'dependency_error_rate_limited'
        elif self._keep_success():
            reason = 'ratio'
        else:
            reason = ''
        if dropped:
            self.count_dropped(dropped)
        # Queued first: only a trace that will be exported is named by an exemplar or a link
        kept = False
        if reason:
            try:
                self._traces.put_nowait(
                    (
                        task_id,
                        invocation,
                        run.run_kind,
                        run.org,
                        end,
                        unclosed,
                        reason,
                        run.trace_id,
                        run.span_id,
                        run.project,
                        why,
                    )
                )
                kept = True
            except queue.Full:
                self.count_dropped('trace_queue_full')
        if kept:
            if not run.keep_reason:
                run.keep_reason = 'pass_kept'  # the pass links to the run's trace: keep it too
            if reason in ('error', 'slow', 'dependency_error') and len(run.pass_links) < MAX_PASS_LINKS:
                # the run's trace leads to its failed and slow passes (a pass already leads to its run)
                run.pass_links.append((invocation.trace_id, invocation.span_id, reason))
        # Measured whatever the sampling; the exemplars name the pass's spans when they are kept
        try:
            self._count_turn(invocation, run, why, end, kept)
        except Exception:
            self.count_hook_error('turn')

    def _slow_allowed(self, task_id: str, org: str) -> bool:
        """A slow trace may be kept (flood guard); counted when it may not."""
        if self._slow_limit.allow(task_id, org):
            return True
        self.count_dropped('slow_rate_limited')
        return False

    def _keep_success(self) -> bool:
        """Whether a trace that did not fail is kept (RR_OTEL_TRACES_SAMPLE_RATIO; 0 keeps errors only)."""
        return self._ratio >= 1.0 or (self._ratio > 0.0 and self._random.random() < self._ratio)

    def _trace_worker(self) -> None:
        """The 'otel-traces' thread: builds the queued pipeline traces; None stops it."""
        while True:
            item = self._traces.get()
            try:
                if item is None:
                    return
                self._export_invocation(*item)
            except Exception:
                self.count_hook_error('pipeline_trace')
            finally:
                self._traces.task_done()

    def flush_traces(self, timeout: float = 1.0) -> bool:
        """Wait up to `timeout` seconds for the queued pipeline traces to be built; True when none are left."""
        deadline = time.monotonic() + max(0.0, timeout)
        while self._traces.unfinished_tasks:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.005)
        return True

    def _export_invocation(
        self,
        task_id: str,
        invocation: _Invocation,
        run_kind: str,
        org: str,
        end: float,
        unclosed: bool,
        reason: str,
        run_trace_id: int = 0,
        run_span_id: int = 0,
        project: str = '',
        why: str = '',
    ) -> None:
        attributes = self._with_org(
            {
                'rocketride.task.id': task_id,
                'rocketride.task.run_kind': run_kind,
                'rocketride.sampling.reason': reason,
                'rocketride.sampling.ratio': self._ratio,
                'rocketride.pipeline.component_calls': len(invocation.components) + invocation.dropped,
            },
            org,
        )
        # The run id is '<pipeline token>.<source>' (a chat, a webhook, a folder)
        pipeline_id, _, source = task_id.partition('.')
        if source:
            attributes['rocketride.pipeline.id'] = pipeline_id
            attributes['rocketride.source'] = source
        if project:
            attributes['rocketride.project.id'] = project
        if unclosed:
            attributes['rocketride.span.unclosed'] = True
        # The pass's own trace (ids from when it began), linked to its run's trace
        links = []
        if run_trace_id and run_span_id:
            links.append(
                Link(
                    SpanContext(run_trace_id, run_span_id, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
                )
            )
        with _preset_ids(invocation.trace_id, invocation.span_id):
            root = self._tracer.start_span(
                'pipeline',
                kind=SpanKind.INTERNAL,
                start_time=_ns(invocation.start),
                attributes=attributes,
                links=links,
            )
        if invocation.error:
            first = invocation.raised[0] if invocation.raised else None
            if first is not None:
                # which node raised what: the pass's cause, readable from the trace list
                root.set_status(Status(StatusCode.ERROR, f'{first[0]}: {first[1]}: {first[2]}'[:MAX_CAUSE_LENGTH]))
                root.set_attribute('error.type', first[5] or first[1])
                root.set_attribute('rocketride.component.type', first[0])
            elif unclosed:
                root.set_status(Status(StatusCode.ERROR, _cut_cause(invocation, why)))
            else:
                root.set_status(Status(StatusCode.ERROR, 'a component failed'))
        elif invocation.marks:
            # A node reported its call failed and went on (an LLM answering with the
            # provider's error): the pass failed for that question
            component_type, _, error_type, _ = invocation.marks[0]
            root.set_status(Status(StatusCode.ERROR, f'{component_type}: {error_type}'))
            root.set_attribute('error.type', error_type)
            root.set_attribute('rocketride.component.type', component_type)
        elif unclosed and invocation.call_error:
            # Cut open (the run ended, a stop, a shutdown) after a call failed: that failure
            # is the likely cause, not a success
            root.set_status(Status(StatusCode.ERROR, _cut_cause(invocation, why)))
        if unclosed and why:
            root.set_attribute('rocketride.pass.closed_by', why[:MAX_VALUE_LENGTH])
        context = trace.set_span_in_context(root)
        spans = []  # (type, start, end, span) for the relayed calls' parents
        marks = list(invocation.marks)
        raised = list(invocation.raised)
        for component_type, start, finish, result, component_id in invocation.components:
            child = self._tracer.start_span(
                f'component {component_type}',
                context=context,
                kind=SpanKind.INTERNAL,
                start_time=_ns(start),
                attributes={
                    'rocketride.component.type': component_type,
                    'rocketride.component.id': component_id,
                    **({'rocketride.component.result': result} if result else {}),
                },
            )
            if result == 'error':
                child.set_status(Status(StatusCode.ERROR, 'component returned an error'))
            for mark in [
                m
                for m in marks
                if m[0] == component_type and start - MATCH_TOLERANCE <= m[1] <= finish + MATCH_TOLERANCE
            ]:
                marks.remove(mark)
                child.set_status(Status(StatusCode.ERROR, mark[2]))
                child.set_attribute('error.type', mark[2])
                child.add_event(
                    'exception',
                    {'exception.type': mark[2], 'exception.message': mark[3]},
                    timestamp=_ns(mark[1]),
                )
            for error in [
                r
                for r in raised
                if r[0] == component_type and start - MATCH_TOLERANCE <= r[6] <= finish + MATCH_TOLERANCE
            ]:
                raised.remove(error)
                _component_type, cls, message, location, _mono, code, wall = error
                child.set_status(Status(StatusCode.ERROR, f'{cls}: {message}'[:MAX_CAUSE_LENGTH]))
                # error.type: the engine's code for it (bounded); exception.type: the class
                child.set_attribute('error.type', code or cls)
                child.add_event('exception', _raised_attributes(cls, message, location), timestamp=_ns(wall))
            child.end(end_time=_ns(max(finish, start)))
            spans.append((component_type, start, max(finish, start), child))
        # A node's logical model calls (GenAI conventions): their HTTP tries go under them
        models = []
        for (
            component_type,
            start_ns,
            end_ns,
            gen_attributes,
            error_type,
            message,
            retries,
            usage,
            answered_by,
            model_span_id,
        ) in sorted(invocation.models, key=lambda m: m[1]):
            parent = _call_parent(spans, component_type, start_ns / 1e9, end_ns / 1e9)
            name = ' '.join(
                part
                for part in (gen_attributes.get('gen_ai.operation.name'), gen_attributes.get('gen_ai.request.model'))
                if part
            )
            span_attributes = dict(gen_attributes, **{'rocketride.component.type': component_type})
            for key, attribute in _GENAI_USAGE.items():
                if usage.get(key):
                    span_attributes[attribute] = usage[key]
            if answered_by:
                span_attributes['gen_ai.response.model'] = answered_by
            # the span id its metrics' exemplars name
            with _preset_ids(0, model_span_id):
                model = self._tracer.start_span(
                    name or 'model call',
                    context=trace.set_span_in_context(parent) if parent is not None else context,
                    kind=SpanKind.CLIENT,
                    start_time=start_ns,
                    attributes=span_attributes,
                )
            for at, kind, wait in retries:
                model.add_event(
                    'rocketride.model.retry',
                    {'error.type': str(kind)[:MAX_VALUE_LENGTH], 'rocketride.retry.delay_s': float(wait)},
                    timestamp=int(at),
                )
            if error_type:
                model.set_attribute('error.type', error_type)
                model.set_status(Status(StatusCode.ERROR, f'{error_type}: {message}' if message else error_type))
                if message:
                    model.add_event('exception', {'exception.message': message}, timestamp=end_ns)
            model.end(end_time=end_ns)
            models.append([component_type, start_ns, end_ns, model, 0])
        tolerance = int(MATCH_TOLERANCE * 1e9)
        for component_type, name, start_ns, end_ns, call_attributes, error_type, span_id in sorted(
            invocation.calls, key=lambda c: c[2]
        ):
            holder = next(
                (
                    m
                    for m in models
                    if m[0] == component_type and m[1] - tolerance <= start_ns and end_ns <= m[2] + tolerance
                ),
                None,
            )
            if holder is not None:
                # a try of a model call: the second and later are resends (HTTP conventions)
                parent = holder[3]
                if holder[4]:
                    call_attributes = dict(call_attributes, **{'http.request.resend_count': holder[4]})
                holder[4] += 1
            else:
                parent = _call_parent(spans, component_type, start_ns / 1e9, end_ns / 1e9)
            # A call that sent a traceparent (the model server) keeps the span id it sent:
            # the server's span is its child
            with _preset_ids(0, span_id) if span_id else contextlib.nullcontext():
                call = self._tracer.start_span(
                    name,
                    context=trace.set_span_in_context(parent) if parent is not None else context,
                    kind=SpanKind.CLIENT,
                    start_time=start_ns,
                    attributes=dict(call_attributes, **{'rocketride.component.type': component_type}),
                )
            if error_type:
                call.set_attribute('error.type', error_type)
                call.set_status(Status(StatusCode.ERROR, error_type))
            call.end(end_time=end_ns)
        root.end(end_time=_ns(max(end, invocation.start)))

    # ── Observable callbacks (on the metric reader thread) ───────────────────

    def _observe_inflight(self, options: CallbackOptions):
        counts: Dict[str, int] = {}
        with self._lock:
            for method, *_rest in self._inflight.values():
                counts[method] = counts.get(method, 0) + 1
        return [Observation(count, {'rpc.method': method}) for method, count in counts.items()]

    def _observe_oldest(self, options: CallbackOptions):
        """The oldest open request per method; one error line for each request past the overdue age."""
        now = time.monotonic()
        oldest: Dict[str, float] = {}
        overdue = []
        with self._lock:
            for entry in self._inflight.values():
                method, started, identity, request, logged = entry
                age = now - started
                oldest[method] = max(oldest.get(method, 0.0), age)
                if self._overdue and age >= self._overdue and not logged:
                    entry[4] = True
                    overdue.append((method, age, identity, request))
        for method, age, identity, request in overdue:
            self._overdue_line(method, age, identity, request)
        return [Observation(round(age), {'rpc.method': method}) for method, age in oldest.items()]

    def _overdue_line(self, method: str, age: float, identity: Mapping[str, Any], request: Any) -> None:
        """One error line for a request open past RR_OTEL_OVERDUE_REQUEST_SECONDS; its trace is kept when it ends."""
        try:
            request.error_logged = True
            line = {
                'level': 'error',
                'event': 'rocketride.request.overdue',
                'method': method,
                'age_s': round(age),
                'trace_id': f'{request.trace_id:032x}',
            }
            for key in ('task_id', 'connection_id'):
                if identity.get(key):
                    line[key] = str(identity[key])[:MAX_VALUE_LENGTH]
            org = self._org(identity.get('org_id'))
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('request_overdue_log')

    def _observe_runs(self, options: CallbackOptions):
        counts = {state: 0 for state in _LIVE_STATES}
        with self._lock:
            for run in self._runs.values():
                state = STATE_NAMES.get(run.state)
                if state in counts:
                    counts[state] += 1
        return [Observation(value, {'rocketride.task.state': state}) for state, value in counts.items()]

    def _observe_connections(self, options: CallbackOptions):
        count = self._connections
        if count is None:
            return []
        try:
            return [Observation(int(count()))]
        except Exception:
            return []

    def _observe_up(self, options: CallbackOptions):
        return [Observation(1)]

    def _observe_idle(self, options: CallbackOptions):
        """
        rocketride.pass.idle: for each innermost open component type and org, the
        longest a pass has gone without an enter or leave while a call is still
        open. A hung node, a provider that never answers, a lost leave. A working
        agent keeps emitting its tools' enters and leaves, so it stays near 0;
        an idle source has no pass open, so it is not counted. 0 for a type and
        org reported before and idle no more (dropped IDLE_KEY_SECONDS later),
        so the series do not go stale nor stay forever.
        """
        now = time.monotonic()
        passes: List[Tuple[str, str, str, float]] = []  # (task, component type, raw org, idle seconds)
        stalled: List[Tuple[str, str, str, float, int]] = []
        with self._lock:
            for task_id, run in self._runs.items():
                if run.debugger:
                    continue
                for invocation in run.invocations.values():
                    if not invocation.stack:
                        continue
                    provider = (run.types or {}).get(invocation.stack[-1][0])
                    component_type = self._bounded_type_locked(provider) if provider else '_unknown'
                    if any(fnmatch.fnmatchcase(component_type, pattern) for pattern in self._stall_exempt):
                        continue
                    seconds = max(0.0, now - invocation.last_mono)
                    passes.append((task_id, component_type, run.org, seconds))
                    if self._stall_log and seconds >= self._stall_log and not invocation.stall_logged:
                        invocation.stall_logged = True
                        stalled.append((task_id, component_type, run.org, seconds, invocation.trace_id, run.project))
        # The org's cap takes the lock: outside it
        idle: Dict[Tuple[str, str], float] = {}
        for _task_id, component_type, org_id, seconds in passes:
            key = (component_type, self._org(org_id) or '')
            idle[key] = max(idle.get(key, 0.0), seconds)
        with self._lock:
            for key in idle:
                if key in self._idle_keys or len(self._idle_keys) < MAX_ATTRIBUTE_SETS:
                    self._idle_keys[key] = now
            for key, seen in list(self._idle_keys.items()):
                if key not in idle and now - seen > IDLE_KEY_SECONDS:
                    del self._idle_keys[key]
            keys = set(self._idle_keys)
        for task_id, component_type, org_id, seconds, trace_id, project in stalled:
            self._stall_line(task_id, component_type, org_id, seconds, trace_id, project)
        observations = []
        for component_type, org in keys | set(idle):
            attributes = {'rocketride.component.type': component_type}
            if org:
                attributes[ORG_KEY] = org
            observations.append(Observation(round(idle.get((component_type, org), 0.0)), attributes))
        return observations

    def _stall_line(
        self, task_id: str, component_type: str, org_id: str, seconds: float, trace_id: int, project: str = ''
    ) -> None:
        """One error line on stdout for a stalled pass: the run to look at (logs keep errors only)."""
        try:
            line = {
                'level': 'error',
                'event': 'rocketride.pass.stalled',
                'task_id': task_id,
                # The pass's trace: kept if the pass then fails or is slow
                'trace_id': f'{trace_id:032x}' if trace_id else '',
                'component_type': component_type,
                'idle_s': round(seconds),
            }
            if project:
                line['project_id'] = project
            org = self._org(org_id)
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('pass_stalled_log')

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
        # The queued traces get a quarter of the time; the rest is dropped
        self.flush_traces(timeout / 4)
        try:
            self._traces.put_nowait(None)
        except queue.Full:
            pass
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


_CUT_BY = {'task_end': 'the run ending', 'shutdown': 'the server shutting down'}


def _cut_cause(invocation: '_Invocation', why: str) -> str:
    """Why an open pass was closed, and the last dependency failure in it (its likely cause)."""
    text = f'pass cut by {_CUT_BY.get(why, why or "its end")}'
    failed_models = [m for m in invocation.models if m[4]]
    failed_calls = [c for c in invocation.calls if c[5]]
    if failed_models:
        model = max(failed_models, key=lambda m: m[2])
        attributes = model[3]
        text += f'; last model error: {model[4]} ({attributes.get("gen_ai.request.model") or "?"})'
        if model[5]:
            text += f': {model[5]}'
    elif failed_calls:
        call = max(failed_calls, key=lambda c: c[3])
        text += f'; last dependency error: {call[5]} ({call[4].get("server.address") or call[1]})'
    return text[:MAX_CAUSE_LENGTH]


def _raised_attributes(cls: str, message: str, location: str) -> Dict[str, Any]:
    """An exception event's attributes (semconv): class, message, and where it was raised."""
    attributes: Dict[str, Any] = {'exception.type': cls, 'exception.message': message}
    path, _, line = location.rpartition(':')
    if path and line.isdigit():
        attributes['code.file.path'] = path
        attributes['code.line.number'] = int(line)
    return attributes


def _exemplar(trace_id: int, span_id: int, kept: bool) -> Optional[Any]:
    """
    The context a measurement is recorded in for its exemplar: the span it names,
    sampled only when its trace is kept (the SDK keeps exemplars of sampled spans only,
    so a metric never links to a trace that was not exported).
    """
    if not (trace_id and span_id):
        return None
    flags = TraceFlags(TraceFlags.SAMPLED if kept else TraceFlags.DEFAULT)
    return trace.set_span_in_context(trace.NonRecordingSpan(SpanContext(trace_id, span_id, False, flags)))


def _call_parent(spans: List[Tuple[str, float, float, Any]], component_type: str, start: float, end: float) -> Any:
    """The component span a relayed call belongs to: same type, covering it (within MATCH_TOLERANCE); else the
    nearest earlier one of that type; None (the pass root) when the type never ran.
    """
    best = None
    for span_type, span_start, span_end, span in spans:
        if span_type != component_type or span_start > end + MATCH_TOLERANCE:
            continue
        if span_start - MATCH_TOLERANCE <= start and end <= span_end + MATCH_TOLERANCE:
            return span
        best = span
    return best


_TRACE_ID = re.compile(r'^[0-9a-f]{32}$')
_SPAN_ID = re.compile(r'^[0-9a-f]{16}$')


def _other_pass(trace_body: Any, trace_id: int) -> bool:
    """An enter whose engine pass id ('tid') is valid and not trace_id: another pass than the open one."""
    tid = trace_body.get('tid') if isinstance(trace_body, dict) else None
    return isinstance(tid, str) and bool(_TRACE_ID.match(tid)) and int(tid, 16) not in (0, trace_id)


_TRACEPARENT = re.compile(r'^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$')


def _parent_context(traceparent: Any) -> Any:
    """A context for the caller's sampled W3C traceparent, or None (absent, malformed, unsampled)."""
    if not isinstance(traceparent, str):
        return None
    match = _TRACEPARENT.match(traceparent.strip())
    if not match:
        return None
    trace_id, span_id, flags = (int(group, 16) for group in match.groups())
    if not trace_id or not span_id or not flags & 0x01:
        return None
    parent = trace.SpanContext(trace_id, span_id, is_remote=True, trace_flags=trace.TraceFlags(flags))
    return trace.set_span_in_context(trace.NonRecordingSpan(parent))


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
