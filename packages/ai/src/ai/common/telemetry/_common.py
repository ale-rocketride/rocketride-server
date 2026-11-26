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

"""Constants, per-run state and helpers shared by the parts of the telemetry sink (_otel)."""

import contextlib
import contextvars
import random
import re
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Mapping, Optional, Tuple

from opentelemetry import trace
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import SpanContext, TraceFlags


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
# The relay's other records (relay.node_error, _raised, model_call), by kind: the keys
# and value types it writes, required then optional; a record with any other is ignored
_RELAY_RECORDS: Dict[str, Tuple[Dict[str, type], Dict[str, type]]] = {
    'err': ({'k': str, 'x': str, 'g': str, 'w': int, 'p': int, 'c': str}, {'t': str}),
    'exc': ({'k': str, 'x': str, 'g': str, 'l': str, 'w': int, 'p': int, 'c': str}, {'t': str}),
    'gen': (
        {'k': str, 's': int, 'e': int, 'p': int, 'c': str, 'a': dict, 'r': list},
        {'x': str, 'g': str, 'u': dict, 'rm': str, 't': str},
    ),
}
MAX_RELAYED_DROPPED = 10000  # calls one relay record may say were dropped (relay.MAX_PENDING is far below)
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
# Requests kept only because their caller sampled its trace: per process (a tenth per org)
CALLER_TRACES_PER_MINUTE = 100.0

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
    'caller_rate_limited',  # a request kept for its caller's sampled trace, past CALLER_TRACES_PER_MINUTE
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


def _relay_record_valid(record: Mapping[str, Any]) -> bool:
    """Whether a relay record of a known kind has exactly the keys and value types the relay writes."""
    shape = _RELAY_RECORDS.get(record.get('k'))  # type: ignore[arg-type]
    if shape is None:
        return False
    required, optional = shape
    if not required.keys() <= record.keys():
        return False
    for key, value in record.items():
        expected = required.get(key) or optional.get(key)
        if expected is None or not isinstance(value, expected):
            return False
        if expected is int and isinstance(value, bool):
            return False
    return True


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
        'stall_noted',
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
        # A stall line named this pass's trace at some point: kept (never cleared by progress)
        self.stall_noted = False
        # (component id, wall start, monotonic start)
        self.stack: List[Tuple[str, float, float]] = []
        # (type, wall start, wall end, result, component id)
        self.components: List[Tuple[str, float, float, Optional[str], str]] = []
        # Up to two ids of components that left, kept past the span cap (_carried)
        self.left: List[str] = []
        # A node's own report that its call failed (relay.node_error):
        # (component type, wall time, error type, message, component id)
        self.marks: List[Tuple[str, float, str, str, str]] = []
        # Logical model calls a node made (relay.model_call): (component type, start ns,
        # end ns, gen_ai attributes, error type, provider message, retries)
        self.models: List[Tuple[Any, ...]] = []
        # Exceptions its nodes' handlers raised (relay._raised), shared with the run's
        # record: [component type, class, message, location, monotonic, engine code, wall, component id]
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
