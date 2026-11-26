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
A run's dependency calls, relayed to EaaS (the task subprocess's side).

The calls a node makes (an LLM provider's API, a database, Redis, AWS, the
model server) happen in the task subprocess, which exports no telemetry of its
own (no OTEL_* reaches it). With --otel_relay=<flags> (EaaS adds it when its
telemetry is on) each call is recorded here (_deps recorders and library
instrumentations) and written to EaaS in batches, as control lines
'>SPN*<json array>', through the engine's monitor: when the node's handler
returns (so they arrive before its component's leave), else every
FLUSH_SECONDS. A node's call past MAX_CALLS_PER_SCOPE, or made off its handler,
is still sent (measured) without its component.
EaaS measures it and attaches it to the pass and component that made it
(Telemetry.dependency_relayed), kept or dropped with that pass's trace.

The component is the node instance the engine is calling into: every
IInstanceBase subclass's handlers are wrapped to make (pipe, component id)
current for the call. The wrapping is done when the class is defined, so only
classes defined after install() get it (ai/node.py installs before loading any
node); one defined earlier is named in a warning and its calls carry no
component. A call made on a thread or event loop the node started itself has
no component; EaaS still measures it.

What is written: the call's name, times, error type and a few bounded
attributes (system, operation, server, method, status, scheme). Never a URL, a
statement, a payload or a header.
"""

import collections
import functools
import json
import sys
import threading
import time
from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

# Only the standard library at import: node.py imports this in every run,
# with or without the OpenTelemetry SDK. _deps (and the SDK) load in install().

FLAG = '--otel_relay'
FLAGS = ('db', 'redis', 'http', 'aws', 'grpc', 'model')  # as _deps.FLAGS
MAX_CALLS_PER_SCOPE = 64  # calls placed in the trace for one handler call; the rest are measured only
BATCH = 32  # calls per line
MAX_PENDING = 5000  # calls waiting to be written; past it they are counted (relay_overflow)
FLUSH_SECONDS = 0.2  # calls made off a handler are written at least this often
MAX_STARTED = 10_000  # client spans open at once whose node call is remembered (a stream read later)
MAX_ERROR_MESSAGE = 256  # characters of a node's error message on its span
_KEEP = (
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


class _Scope:
    __slots__ = ('pipe', 'component', 'trace_id', 'calls', 'dropped', 'component_type')

    def __init__(self, pipe: Any, component: Any, trace_id: int = 0, component_type: Optional[str] = None) -> None:
        self.pipe = pipe
        self.component = component
        # A node's global handler (validateConfig, beginGlobal): no pass, only its type
        self.component_type = component_type
        self.trace_id = trace_id  # the pass's (engine: instance.passTraceId), 0 when unknown
        self.calls = 0
        self.dropped = 0


# The scope lives in the OpenTelemetry context, which the threading
# instrumentation copies into the work a node hands to a thread or an executor
# (at submit, per task): a pooled thread carries the pass that gave it the work.
# Imported on first use: this module is imported with telemetry off too.
_ctx: Any = None  # opentelemetry.context
_SCOPE_KEY: Any = None


def _context() -> Any:
    global _ctx, _SCOPE_KEY
    if _ctx is None:
        from opentelemetry import context

        _SCOPE_KEY = context.create_key('rocketride_relay_scope')
        _ctx = context
    return _ctx


def _current() -> Optional[_Scope]:
    """The scope of the node call this code runs for (its thread's or its task's), or None."""
    return _ctx.get_value(_SCOPE_KEY) if _ctx is not None else None


_write: Optional[Callable[[str], None]] = None
_lock = threading.Lock()
# Held across taking AND writing a batch: a handler's flush waits for a write
# already in progress, so its calls are out before its component's leave
_writing = threading.Lock()
_pending: List[dict] = []
_overflow = 0


def active() -> bool:
    """Whether this process relays its calls to EaaS (installed with telemetry on); cheap."""
    return _write is not None


def flags_from_argv(argv: Sequence[str]) -> List[str]:
    """The relay flags EaaS passed (--otel_relay=db,http,...), or [] (off)."""
    for arg in argv:
        if arg.startswith(FLAG + '='):
            names = {name.strip().lower() for name in arg.split('=', 1)[1].split(',')}
            return [name for name in FLAGS if name in names]
    return []


def install(argv: Sequence[str] = sys.argv) -> List[str]:
    """Start relaying when EaaS asked for it; returns what is recorded. Never raises."""
    global _write
    flags = flags_from_argv(argv)
    if not flags or _write is not None:
        return []
    try:
        from rocketlib import IGlobalBase, IInstanceBase, monitorOther

        from . import _deps

        _write = functools.partial(monitorOther, 'SPN')
        _context()
        _warn_untraced([(IInstanceBase, None), (IGlobalBase, GLOBAL_HANDLERS)])
        _wrap_subclasses(IInstanceBase)
        try:
            _wrap_subclasses(IGlobalBase, _scoped_global, GLOBAL_HANDLERS)
        except Exception:
            pass  # the node calls keep their place; global calls stay unattributed
        threading.Thread(target=_flush_loop, name='otel-relay', daemon=True).start()

        tracer_provider = None
        if {'http', 'aws', 'grpc', 'db'} & set(flags):  # the instrumentations that make spans
            from opentelemetry.sdk.trace import TracerProvider

            # Not the global provider: a node may set its own (crewai), and a
            # library adding its exporter to the global one would receive these
            # spans unsanitized. The instrumentations are given this one.
            tracer_provider = TracerProvider(shutdown_on_exit=False)
            tracer_provider.add_span_processor(relay_spans())
        done, _skipped, _recorders = _deps.instrument(tracer_provider, flags, emit, current_pass_trace_id)
        _watch_depends()
        _propagate_to_threads()
        return done
    except Exception:
        return []


def _watch_depends() -> None:
    """A node installs its driver with depends() and imports it after: record it right then (depends' hook)."""
    try:
        import depends as module  # type: ignore

        hooks = module.after_install_hooks
        if _depends_installed not in hooks:
            hooks.append(_depends_installed)
    except Exception:
        pass


def _depends_installed(_requirements: Optional[str]) -> None:
    """depends() finished an install: record the libraries that became importable."""
    from . import _deps

    _deps.instrument_new()


def _propagate_to_threads() -> None:
    """The pass's scope follows the work a node hands to a thread or an executor (threading instrumentation)."""
    try:
        from opentelemetry.instrumentation.threading import ThreadingInstrumentor

        instrumentor = ThreadingInstrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument()
    except Exception:
        pass  # not installed: those calls are measured, not placed (relay_unattributed)


GLOBAL_HANDLERS = ('validateConfig', 'beginGlobal', 'endGlobal')  # a node's global calls (key checks)


MAX_UNTRACED_NAMES = 10  # class names in the warning about node classes defined before install()
# Node classes that existed before install(): their handlers are not wrapped (no component on their calls)
untraced_classes = 0


def _handlers(base: Any, handlers: Optional[Sequence[str]]) -> Sequence[str]:
    """The handler names _wrap_subclasses wraps on base's subclasses: the given ones, else base's public methods."""
    if handlers is not None:
        return handlers
    return [name for name, value in vars(base).items() if callable(value) and not name.startswith('_')]


def _existing_subclasses(base: Any) -> List[Any]:
    """Every subclass of base defined so far, at any depth, once."""
    found: List[Any] = []
    stack = list(base.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in found:
            continue
        found.append(cls)
        stack.extend(cls.__subclasses__())
    return found


def _warn_untraced(bases: Sequence[Tuple[Any, Optional[Sequence[str]]]]) -> int:
    """
    Node classes defined before install() keep their handlers unwrapped (only
    classes defined later are): their calls are measured but not placed on
    their component. Counted (untraced_classes) and named in one warning line.
    """
    global untraced_classes
    try:
        names: List[str] = []
        for base, handlers in bases:
            wanted = _handlers(base, handlers)
            for cls in _existing_subclasses(base):
                if any(
                    callable(cls.__dict__.get(name)) and not getattr(cls.__dict__[name], '__relay_scoped__', False)
                    for name in wanted
                ):
                    label = f'{cls.__module__}.{cls.__qualname__}'
                    if label not in names:
                        names.append(label)
        untraced_classes = len(names)
        if names:
            shown = ', '.join(names[:MAX_UNTRACED_NAMES])
            more = f' (+{len(names) - MAX_UNTRACED_NAMES} more)' if len(names) > MAX_UNTRACED_NAMES else ''
            from rocketlib import warning

            warning(
                f'telemetry: {len(names)} node classes defined before telemetry started are not traced: {shown}{more}'
            )
        return len(names)
    except Exception:
        return 0


def _wrap_subclasses(base: Any, scoped: Optional[Callable] = None, handlers: Optional[Sequence[str]] = None) -> None:
    """Make every node's handlers set (pipe, component) for their call, now and for classes defined later."""
    scoped = scoped or _scoped
    handlers = _handlers(base, handlers)
    own = vars(base).get('__init_subclass__')
    original = own.__func__ if own is not None else None

    def init_subclass(cls, **kwargs):
        if original is not None:
            original(cls, **kwargs)
        else:
            super(base, cls).__init_subclass__(**kwargs)
        for name in handlers:
            fn = cls.__dict__.get(name)
            if callable(fn) and not getattr(fn, '__relay_scoped__', False):
                setattr(cls, name, scoped(fn))

    base.__init_subclass__ = classmethod(init_subclass)


def _scoped(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        ids = getattr(self, '_relay_ids', None)
        if ids is None:
            instance = getattr(self, 'instance', None)
            ids = (getattr(instance, 'pipeId', None), _component_id(getattr(instance, 'pipeType', None)))
            try:
                self._relay_ids = ids
            except Exception:
                pass
        context = _context()
        scope = _Scope(*ids, _pass_trace_id(getattr(self, 'instance', None)))
        token = context.attach(context.set_value(_SCOPE_KEY, scope))
        try:
            return fn(self, *args, **kwargs)
        except BaseException as error:
            _raised(scope, error)
            raise
        finally:
            context.detach(token)
            if scope.calls:
                flush()  # before the engine's leave for this component

    wrapper.__relay_scoped__ = True
    return wrapper


def _location(error: BaseException) -> str:
    """'<file>:<line>' of the frame that raised, the path from the node's or the package's folder."""
    tb = error.__traceback__
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    if tb is None:
        return ''
    path = tb.tb_frame.f_code.co_filename.replace('\\', '/')
    for marker in ('/nodes/', '/site-packages/', '/ai/', '/rocketlib/'):
        if marker in path:
            path = marker.strip('/') + '/' + path.rsplit(marker, 1)[1]
            break
    else:
        path = path.rsplit('/', 1)[-1]
    return f'{path}:{tb.tb_lineno}'[:MAX_ERROR_MESSAGE]


def _raised(scope: _Scope, error: BaseException) -> None:
    """
    A node's handler raised: EaaS names that node for the engine's error report that
    follows (instead of guessing it from the text), with the exception's class,
    message (redacted) and where it was raised. Once per exception (handlers nest).
    """
    if _write is None or scope.pipe is None or not scope.component or getattr(error, '_relay_reported', False):
        return
    try:
        error._relay_reported = True  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        from ai.common import logfmt

        from . import _deps

        cls = type(error).__name__
        record = {
            'k': 'exc',
            'x': cls[:64],
            'g': _deps._clean_text(str(error), logfmt.redact)[:MAX_ERROR_MESSAGE],
            'l': _location(error),
            'w': time.time_ns(),
            'p': scope.pipe,
            'c': scope.component,
        }
        if scope.trace_id:
            record['t'] = f'{scope.trace_id:032x}'
    except Exception:
        return
    global _overflow
    scope.calls += 1  # written before the engine reports the error
    with _lock:
        if len(_pending) < MAX_PENDING:
            _pending.append(record)
        else:
            _overflow += 1


def _scoped_global(fn: Callable) -> Callable:
    """A node's global handler: its calls (a key check at start) carry the node's type, in no pass."""

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if _current() is not None:
            return fn(self, *args, **kwargs)
        try:
            component_type = str(getattr(getattr(self, 'glb', None), 'logicalType', '') or '') or None
        except Exception:
            component_type = None
        if component_type is None:
            return fn(self, *args, **kwargs)
        context = _context()
        scope = _Scope(None, None, 0, component_type)
        token = context.attach(context.set_value(_SCOPE_KEY, scope))
        try:
            return fn(self, *args, **kwargs)
        finally:
            context.detach(token)
            if scope.calls:
                flush()

    wrapper.__relay_scoped__ = True
    return wrapper


def _pass_trace_id(instance: Any) -> int:
    """The trace id of the pass the engine is running on this instance's pipe (0: unknown, an older engine)."""
    try:
        text = getattr(instance, 'passTraceId', '')
        return int(text, 16) if text and len(text) == 32 else 0
    except Exception:
        return 0


def current_pass_trace_id() -> int:
    """The pass of the node call being handled on this thread (0: none)."""
    scope = _current()
    return scope.trace_id if scope is not None else 0


def _component_id(pipe_type: Any) -> Optional[str]:
    """The component id of an engine pipe type (engLib.IPipeType has .id; a dict, ['id'])."""
    value = pipe_type.get('id') if isinstance(pipe_type, dict) else getattr(pipe_type, 'id', None)
    return str(value) if value else None


def emit(call: Any, scope: Optional[_Scope] = None) -> None:
    """Queue one call for EaaS, with the component it was made for (when it can be placed).

    scope: the node call it was made for, when it ends off that call (a
    streamed response read on another thread or after the handler returned).
    """
    global _overflow
    scope = _current() or scope
    record = {
        'm': call.metric,
        'n': call.name,
        's': call.start_ns,
        'e': call.end_ns,
        'a': {key: call.attributes[key] for key in _KEEP if call.attributes.get(key) not in (None, '')},
    }
    if call.error_type:
        record['x'] = call.error_type
    if getattr(call, 'span_id', 0):
        record['i'] = f'{call.span_id:016x}'  # the span id its traceparent carried
    if scope is not None and scope.pipe is None and scope.component_type:
        record['ct'] = scope.component_type[:64]  # a global handler's call: the node's type, no pass
        scope.calls += 1
    elif scope is not None:
        if scope.trace_id:
            record['t'] = f'{scope.trace_id:032x}'  # its pass: EaaS places it only in that one
        # Not atomic across a node's threads: the cap is approximate there
        if scope.calls < MAX_CALLS_PER_SCOPE:
            record['p'], record['c'] = scope.pipe, scope.component
        else:
            record['o'] = 1  # measured, not placed: past the cap for this handler call
        scope.calls += 1
    with _lock:
        if len(_pending) >= MAX_PENDING:
            _overflow += 1
            return
        _pending.append(record)
        full = len(_pending) >= BATCH
    if full:
        flush()


def node_error(error_type: str, message: str) -> None:
    """
    The node call being handled failed (a provider refused it, say): EaaS marks
    its component span, and the pass, as failed with this bounded type and the
    (already redacted) message. Nothing else changes: not the metrics, not the
    answer. A no-op off a node call or with the relay off.
    """
    scope = _current()
    if scope is None or _write is None or scope.pipe is None or not scope.component:
        return
    record = {
        'k': 'err',
        'x': str(error_type)[:64],
        'g': str(message)[:MAX_ERROR_MESSAGE],
        'w': time.time_ns(),
        'p': scope.pipe,
        'c': scope.component,
    }
    if scope.trace_id:
        record['t'] = f'{scope.trace_id:032x}'
    global _overflow
    scope.calls += 1  # written before the engine's leave for this component
    with _lock:
        if len(_pending) < MAX_PENDING:
            _pending.append(record)
        else:
            _overflow += 1


# The semantic conventions' provider names, by LLM node type (gen_ai.provider.name);
# another node type is named after itself without its 'llm_' prefix
GENAI_PROVIDERS = {
    'llm_openai': 'openai',
    'llm_openai_api': 'openai',
    'llm_anthropic': 'anthropic',
    'llm_bedrock': 'aws.bedrock',
    'llm_gemini': 'gcp.gemini',
    'llm_vision_gemini': 'gcp.gemini',
    'llm_mistral': 'mistral_ai',
    'llm_vision_mistral': 'mistral_ai',
    'llm_deepseek': 'deepseek',
    'llm_perplexity': 'perplexity',
    'llm_xai': 'x_ai',
    'llm_ibm_watson': 'ibm.watsonx.ai',
    'llm_vision_openai': 'openai',
}
MAX_RETRIES_RECORDED = 16  # retry events on one model call


def genai_provider(node_type: str) -> str:
    """gen_ai.provider.name for an LLM node type."""
    node_type = str(node_type or '')
    if node_type.startswith('llm_vision_'):
        node_type = 'llm_' + node_type[len('llm_vision_') :]  # a vision node calls its base provider
    return (
        GENAI_PROVIDERS.get(node_type) or (node_type[4:] if node_type.startswith('llm_') else node_type) or '_unknown'
    )


def model_call(
    operation: str,
    provider: str,
    model: str,
    start_ns: int,
    end_ns: int,
    error_type: Optional[str] = None,
    message: str = '',
    retries: Sequence[Tuple[int, str, float]] = (),
    usage: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    One logical call to a model (its tries and the waits between them) made for
    the node call being handled: EaaS makes it a span ('chat <model>', the GenAI
    conventions) and places the HTTP tries under it. message: the provider's
    error, already redacted. retries: (time ns, error type, wait in seconds) of
    each failed try that was retried. usage: input, output and cache_read tokens and
    the model that answered (llm_adapter.end_call_usage). A no-op off a node call or
    with the relay off.
    """
    scope = _current()
    if scope is None or _write is None or scope.pipe is None or not scope.component:
        return
    record = {
        'k': 'gen',
        's': int(start_ns),
        'e': int(end_ns),
        'p': scope.pipe,
        'c': scope.component,
        'a': {
            'gen_ai.operation.name': str(operation)[:32],
            'gen_ai.provider.name': str(provider)[:64],
            'gen_ai.request.model': str(model or '')[:64],
        },
        'r': [
            [int(t), str(kind)[:64], round(float(wait), 3)] for t, kind, wait in list(retries)[:MAX_RETRIES_RECORDED]
        ],
    }
    if error_type:
        record['x'] = str(error_type)[:64]
        record['g'] = str(message)[:MAX_ERROR_MESSAGE]
    tokens = {k: int(usage[k]) for k in ('input', 'output', 'cache_read') if usage and isinstance(usage.get(k), int)}
    if tokens:
        record['u'] = tokens
    if usage and usage.get('model'):
        record['rm'] = str(usage['model'])[:64]
    if scope.trace_id:
        record['t'] = f'{scope.trace_id:032x}'
    global _overflow
    scope.calls += 1
    with _lock:
        if len(_pending) < MAX_PENDING:
            _pending.append(record)
        else:
            _overflow += 1


def flush() -> None:
    """Write the queued calls (and how many did not fit) to EaaS."""
    global _overflow
    with _writing:
        with _lock:
            records, _pending[:] = list(_pending), []
            overflow, _overflow = _overflow, 0
        if overflow:
            records.append({'dropped': overflow, 'reason': 'relay_overflow'})
        write = _write
        for start in range(0, len(records), BATCH):
            try:
                if write is not None:
                    write(json.dumps(records[start : start + BATCH], separators=(',', ':'), default=str))
            except Exception:
                pass


def _flush_loop() -> None:
    event = threading.Event()
    while not event.wait(FLUSH_SECONDS):
        if _pending or _overflow:
            flush()


def relay_spans() -> Any:
    """A span processor writing the library instrumentations' CLIENT spans as calls; nothing is exported."""
    from opentelemetry.sdk.trace import SpanProcessor
    from opentelemetry.trace import SpanKind

    from . import _deps

    # The node call each open client span was started for: a streamed response
    # ends later, on another thread or after the handler returned
    started: 'collections.OrderedDict[int, _Scope]' = collections.OrderedDict()
    started_lock = threading.Lock()

    class RelaySpans(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            _deps.client_started(span)
            scope = _current()
            if scope is not None and span.kind == SpanKind.CLIENT:
                with started_lock:
                    started[span.context.span_id] = scope
                    if len(started) > MAX_STARTED:
                        started.popitem(last=False)

        def on_end(self, span: Any) -> None:
            try:
                with started_lock:
                    scope = started.pop(span.context.span_id, None)
                if not _deps.client_ended(span):
                    return  # inside another client span: that one is the call
                measured = _deps.measurement(span)
                if measured is None:
                    return
                metric, _, attributes = measured
                error_type = attributes.pop('error.type', None)
                # Never the URL (a path can carry a token): its scheme and its known
                # route (url.template, from an allowlist) only
                url = (span.attributes or {}).get('url.full') or (span.attributes or {}).get('http.url')
                if url and _deps._scheme(url):
                    attributes['url.scheme'] = _deps._scheme(url)
                # Never the library's span name: it can carry a database or collection name
                name = _deps.kept_call_name(attributes, metric)
                emit(_deps.Call(metric, name, span.start_time, span.end_time, attributes, error_type), scope)
            except Exception:
                pass

    return RelaySpans()
