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
EaaS's own dependency calls: databases (SQLAlchemy), Redis, HTTP clients
(httpx, requests) and AWS (botocore). Every call is measured; a call is traced
when its request is kept (failed, slow, or named by an ERROR log).

What it costs decides how each is recorded (measured per call, sqlite in
memory: a library instrumentation adds ~130 µs, these recorders ~30 µs):

- Databases and Redis are fast and frequent: our own recorders time them
  (SQLAlchemy Engine events, Redis execute_command) into a Call. A Call becomes
  a span only when its request is kept; otherwise it is only measured.
- HTTP and AWS calls take milliseconds: the libraries' OpenTelemetry
  instrumentations record their spans, which DependencyBuffer holds until the
  request decides.

A DAP request (Request) has its ids from the start: its trace is current while
the handler runs (log lines and the HTTP instrumentations see it), and its
SERVER span is built when it ends, only when kept. A call outside any request
decides itself: kept when it failed or was slow. Statement text, URL query
strings and credentials never leave the process.
"""

import contextvars
import importlib
import importlib.util
import os
import random
import re
import threading
import time
import weakref
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from opentelemetry.sdk.trace import Event, ReadableSpan, SpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

# Library instrumentations, by flag (RR_OTEL_INSTRUMENT):
# (module, instrumentor class, library it patches, pip package)
INSTRUMENTATIONS: Tuple[Tuple[str, str, str, str, str], ...] = (
    (
        'http',
        'opentelemetry.instrumentation.httpx',
        'HTTPXClientInstrumentor',
        'httpx',
        'opentelemetry-instrumentation-httpx',
    ),
    (
        'http',
        'opentelemetry.instrumentation.requests',
        'RequestsInstrumentor',
        'requests',
        'opentelemetry-instrumentation-requests',
    ),
    (
        'http',
        'opentelemetry.instrumentation.aiohttp_client',
        'AioHttpClientInstrumentor',
        'aiohttp',
        'opentelemetry-instrumentation-aiohttp-client',
    ),
    (
        'aws',
        'opentelemetry.instrumentation.botocore',
        'BotocoreInstrumentor',
        'botocore',
        'opentelemetry-instrumentation-botocore',
    ),
    # Under requests and botocore (they suppress it); alone for the libraries
    # that use it directly (Elasticsearch, Pinecone). Only the outermost client
    # span is measured (client_ended), so a library's own span over it counts once.
    (
        'http',
        'opentelemetry.instrumentation.urllib3',
        'URLLib3Instrumentor',
        'urllib3',
        'opentelemetry-instrumentation-urllib3',
    ),
    # gRPC clients (Qdrant, Weaviate, Milvus, Google Cloud): a flag of its own,
    # high in volume. Channels made before instrument() are not seen: nodes make
    # theirs after depends(), when instrument_new() has run.
    (
        'grpc',
        'ai.common.telemetry._grpc',  # the contrib client, given its tracer provider (see _grpc)
        'GrpcClient',
        'grpc',
        'opentelemetry-instrumentation-grpc',
    ),
    (
        'grpc',
        'opentelemetry.instrumentation.grpc',
        'GrpcAioInstrumentorClient',
        'grpc',
        'opentelemetry-instrumentation-grpc',
    ),
    # Databases a node talks to without SQLAlchemy (SqlRecorder has those)
    (
        'db',
        'opentelemetry.instrumentation.pymongo',
        'PymongoInstrumentor',
        'pymongo',
        'opentelemetry-instrumentation-pymongo',
    ),
    (
        'db',
        'opentelemetry.instrumentation.psycopg',
        'PsycopgInstrumentor',
        'psycopg',
        'opentelemetry-instrumentation-psycopg',
    ),
    (
        'db',
        'opentelemetry.instrumentation.psycopg2',
        'Psycopg2Instrumentor',
        'psycopg2',
        'opentelemetry-instrumentation-psycopg2',
    ),
)
FLAGS = ('db', 'redis', 'http', 'aws', 'grpc', 'model')
# Installed with the instrumentations, used by the relay: the pass's scope
# follows the work a node hands to a thread or an executor
EXTRA_PACKAGES = ('opentelemetry-instrumentation-threading',)

MAX_CALLS_PER_REQUEST = 128  # Calls held for one request
MAX_SPANS_PER_TRACE = 128  # library spans held for one undecided trace
MAX_HELD = 20_000  # all undecided traces together
MAX_DECIDED = 10_000  # decisions remembered for late spans
DECIDED_SECONDS = 30.0  # how long a late span still follows its trace's decision
UNDECIDED_SECONDS = 600.0  # a trace whose request never ends is dropped after this

_OPERATION = re.compile(r'^[A-Za-z][A-Za-z_]{0,31}$')
_SQL_DETAIL = re.compile(r'\s*\[(?:SQL|parameters): .*?\](?=\s*(?:\[|\(Background|$))', re.DOTALL)


# ── A request and its calls ───────────────────────────────────────────────────


class Request:
    """A DAP request being handled: its ids, its calls, and what decides whether it is kept."""

    __slots__ = ('trace_id', 'span_id', 'parent', 'start_ns', 'calls', 'error_logged', 'attributes', 'dropped')

    def __init__(self, trace_id: int, span_id: int, parent: Any) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent = parent  # the caller's context (a sampled traceparent), or None
        self.start_ns = time.time_ns()
        self.calls: List['Call'] = []
        self.error_logged = False
        self.attributes: Dict[str, Any] = {}
        self.dropped = 0  # calls past MAX_CALLS_PER_REQUEST


class Call:
    """One database or Redis call (a span only if its request is kept)."""

    __slots__ = ('metric', 'name', 'start_ns', 'end_ns', 'attributes', 'error_type', 'span_id')

    def __init__(
        self,
        metric: str,
        name: str,
        start_ns: int,
        end_ns: int,
        attributes: Dict[str, Any],
        error_type: Optional[str],
        span_id: int = 0,
    ) -> None:
        self.metric = metric
        self.name = name
        self.start_ns = start_ns
        self.end_ns = end_ns
        self.attributes = attributes
        self.error_type = error_type
        self.span_id = span_id  # the id the call's span must have (a traceparent it sent); 0: any


_REQUEST: 'contextvars.ContextVar[Optional[Request]]' = contextvars.ContextVar('rocketride_request', default=None)


def current_request() -> Optional[Request]:
    """The request being handled in this context, or None."""
    return _REQUEST.get()


def set_request(request: Optional[Request]) -> contextvars.Token:
    return _REQUEST.set(request)


def reset_request(token: contextvars.Token) -> None:
    _REQUEST.reset(token)


def _operation(statement: Any) -> str:
    words = str(statement or '').split(None, 1)
    operation = words[0].upper() if words else ''
    return operation if _OPERATION.match(operation) else '_OTHER'


# ── Our recorders (databases, Redis) ─────────────────────────────────────────


class SqlRecorder:
    """
    Times every SQLAlchemy execution (any engine, made before or after this:
    the listeners are on the Engine class) into a Call.
    """

    _SYSTEMS = {'postgresql': 'postgresql', 'mysql': 'mysql', 'mariadb': 'mariadb', 'sqlite': 'sqlite'}

    def __init__(self, on_call: Callable[[Call], None]) -> None:
        self._on_call = on_call
        self._engines: 'weakref.WeakKeyDictionary[Any, Dict[str, Any]]' = weakref.WeakKeyDictionary()
        self._installed = False

    def install(self) -> bool:
        from sqlalchemy import event
        from sqlalchemy.engine import Engine
        from sqlalchemy.exc import TimeoutError as PoolTimeout

        if self._installed:
            return False
        # A connection that cannot be opened (the database is down: a refused
        # connection is not a DBAPI error, so handle_error never sees it).
        # Looked up first: nothing is installed if it is not there.
        original = Engine.__dict__['raw_connection']
        recorder = self
        event.listen(Engine, 'before_cursor_execute', self._before)
        event.listen(Engine, 'after_cursor_execute', self._after)
        event.listen(Engine, 'handle_error', self._failed)

        def raw_connection(engine, *args, **kwargs):
            started = time.time_ns()  # a pool timeout or a slow refusal is the call's duration
            try:
                return original(engine, *args, **kwargs)
            except Exception as e:
                # The pool's own timeout (EaaS ran out of connections) apart
                # from a driver's or socket's TimeoutError (the database)
                pool = isinstance(e, PoolTimeout)
                recorder._record(
                    engine, 'CONNECT', started, 'sqlalchemy.exc.TimeoutError' if pool else type(e).__name__
                )
                raise

        raw_connection.__wrapped__ = original
        self._raw_connection = original
        Engine.raw_connection = raw_connection
        self._installed = True
        return True

    def uninstall(self) -> None:
        from sqlalchemy import event
        from sqlalchemy.engine import Engine

        if self._installed:
            event.remove(Engine, 'before_cursor_execute', self._before)
            event.remove(Engine, 'after_cursor_execute', self._after)
            event.remove(Engine, 'handle_error', self._failed)
            if getattr(Engine.__dict__.get('raw_connection'), '__wrapped__', None) is self._raw_connection:
                Engine.raw_connection = self._raw_connection
            self._installed = False

    def _before(self, conn, cursor, statement, parameters, context, executemany) -> None:
        if context is not None:
            context._rocketride_started = time.time_ns()
            context._rocketride_token = _IN_SQLALCHEMY.set(True)  # the driver's own span is this call

    @staticmethod
    def _end_statement(context: Any) -> None:
        token = getattr(context, '_rocketride_token', None)
        if token is not None:
            context._rocketride_token = None
            try:
                _IN_SQLALCHEMY.reset(token)
            except ValueError:
                _IN_SQLALCHEMY.set(False)  # ended in another context than it began

    def _after(self, conn, cursor, statement, parameters, context, executemany) -> None:
        self._end_statement(context)
        started = getattr(context, '_rocketride_started', None)
        if started is not None:
            context._rocketride_started = None  # recorded: an error while fetching is not a second call
            self._record(conn.engine, statement, started, None)

    def _failed(self, exception_context) -> None:
        if getattr(exception_context, 'is_pre_ping', False):
            return  # a stale pooled connection failing its ping: the pool reconnects, no call failed
        context = exception_context.execution_context
        error = type(exception_context.original_exception).__name__
        if context is None:
            return  # no statement ran: a failed connection, recorded by the raw_connection wrapper
        self._end_statement(context)
        started = getattr(context, '_rocketride_started', None)
        if started is not None:
            context._rocketride_started = None
            self._record(exception_context.engine, exception_context.statement, started, error)

    def _record(self, engine: Any, statement: Any, started: int, error_type: Optional[str]) -> None:
        try:
            base = self._engines.get(engine)
            if base is None:
                base = self._engines[engine] = self._describe(engine)
            operation = _operation(statement)
            attributes = dict(base, **{'db.operation.name': operation})
            self._on_call(
                Call(
                    'db.client.operation.duration',
                    f'{operation} {base.get("db.namespace") or base["db.system.name"]}',
                    started,
                    time.time_ns(),
                    attributes,
                    error_type,
                )
            )
        except Exception:
            pass  # a measurement must never break a query

    def _describe(self, engine: Any) -> Dict[str, Any]:
        url = engine.url
        system = self._SYSTEMS.get(engine.dialect.name, engine.dialect.name)
        attributes: Dict[str, Any] = {'db.system.name': system}
        if system != 'sqlite':  # a sqlite database is a file path, not a name
            if url.database:
                attributes['db.namespace'] = url.database
            if url.host:
                attributes['server.address'] = url.host
            if url.port:
                attributes['server.port'] = url.port
        return attributes


class RedisRecorder:
    """Times every Redis command and pipeline (redis-py, sync and asyncio) into a Call."""

    def __init__(self, on_call: Callable[[Call], None]) -> None:
        self._on_call = on_call
        self._originals: List[Tuple[Any, str, Any]] = []

    def install(self) -> bool:
        if self._originals:
            return False
        import redis.asyncio.client as async_client
        import redis.client as sync_client

        for cls in (sync_client.Redis, sync_client.Pipeline):
            self._wrap_sync(cls, 'execute_command' if cls is sync_client.Redis else 'execute')
        for cls in (async_client.Redis, async_client.Pipeline):
            self._wrap_async(cls, 'execute_command' if cls is async_client.Redis else 'execute')
        return True

    def uninstall(self) -> None:
        for cls, name, original in self._originals:
            # Only our own wrapper: one installed after it stays
            if getattr(cls.__dict__.get(name), '__wrapped__', None) is original:
                setattr(cls, name, original)
        self._originals = []

    def _wrap_sync(self, cls: Any, name: str) -> None:
        original = cls.__dict__[name]
        recorder = self

        def wrapper(client, *args, **kwargs):
            started = time.time_ns()
            try:
                result = original(client, *args, **kwargs)
            except Exception as e:
                recorder._record(client, args, name, started, type(e).__name__)
                raise
            recorder._record(client, args, name, started, None)
            return result

        wrapper.__wrapped__ = original
        self._originals.append((cls, name, original))
        setattr(cls, name, wrapper)

    def _wrap_async(self, cls: Any, name: str) -> None:
        original = cls.__dict__[name]
        recorder = self

        async def wrapper(client, *args, **kwargs):
            started = time.time_ns()
            try:
                result = await original(client, *args, **kwargs)
            except Exception as e:
                recorder._record(client, args, name, started, type(e).__name__)
                raise
            recorder._record(client, args, name, started, None)
            return result

        wrapper.__wrapped__ = original
        self._originals.append((cls, name, original))
        setattr(cls, name, wrapper)

    def _record(self, client: Any, args: Tuple, name: str, started: int, error_type: Optional[str]) -> None:
        try:
            operation = 'PIPELINE' if name == 'execute' else _operation(args[0] if args else '')
            attributes: Dict[str, Any] = {'db.system.name': 'redis', 'db.operation.name': operation}
            pool = getattr(client, 'connection_pool', None)
            kwargs = getattr(pool, 'connection_kwargs', None) or {}
            if kwargs.get('host'):
                attributes['server.address'] = kwargs['host']
            if kwargs.get('port'):
                attributes['server.port'] = kwargs['port']
            self._on_call(
                Call('db.client.operation.duration', operation, started, time.time_ns(), attributes, error_type)
            )
        except Exception:
            pass


class ModelRecorder:
    """
    Times every model server request a ModelClient makes (a node's GPU inference) into a Call.

    ModelClient.send_command calls telemetry.model_server_call() around each
    request; install() makes this recorder the one it uses (no-op otherwise).
    The synchronous send_command is timed, on the node's thread (it hands the
    request to the subprocess's event loop and waits): the latency as the
    pipeline sees it, retries included, and the node's component is current.

    With pass_trace_id (the relay's: the pass the node is running), the request
    carries a traceparent in that pass's trace and a span id of its own, which
    the call's span gets: the model server's span is then its child, in the
    pipeline's trace.
    """

    def __init__(self, on_call: Callable[[Call], None], pass_trace_id: Optional[Callable[[], int]] = None) -> None:
        self._on_call = on_call
        self._pass_trace_id = pass_trace_id
        self._installed = False

    def install(self) -> bool:
        if self._installed:
            return False
        try:
            from opentelemetry import context, trace  # noqa: F401 (_in_trace sets the traceparent with them)
        except ImportError:
            return False  # a model call must never fail for telemetry: not instrumented
        from ai.common import telemetry

        telemetry._model_recorder = self
        self._installed = True
        return True

    def uninstall(self) -> None:
        from ai.common import telemetry

        if telemetry._model_recorder is self:
            telemetry._model_recorder = None
        self._installed = False

    def call(self, client: Any, command: Any) -> '_ModelCall':
        """One request (telemetry.model_server_call): ids taken now, on the node's thread."""
        trace_id = self._pass_trace_id() if self._pass_trace_id else 0
        span_id = (random.getrandbits(64) or 1) if trace_id else 0
        return _ModelCall(self, client, command, trace_id, span_id)

    def _record(self, client: Any, command: Any, started: int, error_type: Optional[str], span_id: int) -> None:
        try:
            method = str(command) if _OPERATION.match(str(command)) else '_OTHER'
            attributes: Dict[str, Any] = {'rpc.system.name': 'rocketride_dap', 'rpc.method': method}
            address = _host(getattr(client, '_uri', ''))
            if address:
                attributes['server.address'] = address
            end = time.time_ns()
            self._on_call(
                Call('rpc.client.call.duration', f'model {method}', started, end, attributes, error_type, span_id)
            )
        except Exception:
            pass


class _ModelCall:
    """One model server request, as `with` (ModelRecorder.call); an Exception is the call's error type."""

    __slots__ = ('_recorder', '_client', '_command', '_trace_id', '_span_id', '_started')

    def __init__(self, recorder: ModelRecorder, client: Any, command: Any, trace_id: int, span_id: int) -> None:
        self._recorder, self._client, self._command = recorder, client, command
        self._trace_id, self._span_id = trace_id, span_id
        self._started = 0

    def __enter__(self) -> '_ModelCall':
        self._started = time.time_ns()
        return self

    def __exit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        if kind is None:
            self._recorder._record(self._client, self._command, self._started, None, self._span_id)
        elif issubclass(kind, Exception):
            self._recorder._record(self._client, self._command, self._started, kind.__name__, self._span_id)
        return False

    def traced(self, coroutine: Any) -> Any:
        """The request's coroutine, run in the pass's trace with this call's span id (its traceparent)."""
        return _in_trace(coroutine, self._trace_id, self._span_id) if self._trace_id else coroutine


async def _in_trace(coroutine: Any, trace_id: int, span_id: int) -> Any:
    """Run the coroutine with (trace_id, span_id) current: the client's requests carry it as traceparent."""
    from opentelemetry import context, trace

    current = trace.SpanContext(
        trace_id, span_id, is_remote=False, trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED)
    )
    token = context.attach(trace.set_span_in_context(trace.NonRecordingSpan(current)))
    try:
        return await coroutine
    finally:
        context.detach(token)


class DependencyCall:
    """
    Times one call into a Call, as `with` or `async with`; the exception, if
    any, goes on and its class is the call's error type. For the recorder of a
    library that has no OpenTelemetry instrumentation (Neo4jRecorder): one
    place per library, never in a node.
    """

    __slots__ = ('_on_call', '_metric', '_name', '_attributes', '_started')

    def __init__(self, on_call: Callable[[Call], None], metric: str, name: str, attributes: Dict[str, Any]) -> None:
        self._on_call, self._metric, self._name, self._attributes = on_call, metric, name, attributes
        self._started = 0

    def __enter__(self) -> 'DependencyCall':
        self._started = time.time_ns()
        return self

    def __exit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        try:
            error_type = kind.__name__ if kind is not None else None
            self._on_call(Call(self._metric, self._name, self._started, time.time_ns(), self._attributes, error_type))
        except Exception:
            pass
        return False

    async def __aenter__(self) -> 'DependencyCall':
        return self.__enter__()

    async def __aexit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        return self.__exit__(kind, value, traceback)


class Neo4jRecorder:
    """
    Times every Cypher query a node runs (neo4j driver: sessions and
    transactions, sync and async) into a Call. run() returns a lazy result: the
    time is to the server's first answer, and an error raised while the records
    are read is not this call's.
    """

    def __init__(self, on_call: Callable[[Call], None]) -> None:
        self._on_call = on_call
        self._originals: List[Tuple[Any, str, Any]] = []

    def install(self) -> bool:
        if self._originals:
            return False
        import inspect

        import neo4j

        names = ('Session', 'AsyncSession', 'Transaction', 'AsyncTransaction', 'ManagedTransaction')
        names += ('AsyncManagedTransaction',)
        owners = []
        for name in names:
            cls = getattr(neo4j, name, None)
            # The class that defines run(): a subclass sharing it is not wrapped twice
            owner = next((c for c in getattr(cls, '__mro__', ()) if 'run' in c.__dict__), None)
            if owner is not None and owner not in owners:
                owners.append(owner)
        for owner in owners:
            self._wrap(owner, inspect.iscoroutinefunction(owner.__dict__['run']))
        return bool(owners)

    def uninstall(self) -> None:
        for cls, name, original in self._originals:
            if getattr(cls.__dict__.get(name), '__wrapped__', None) is original:
                setattr(cls, name, original)
        self._originals = []

    def _call(self, query: Any) -> DependencyCall:
        operation = _operation(getattr(query, 'text', query))  # a Query object or the Cypher text
        attributes = {'db.system.name': 'neo4j', 'db.operation.name': operation}
        return DependencyCall(self._on_call, 'db.client.operation.duration', operation, attributes)

    def _wrap(self, cls: Any, is_async: bool) -> None:
        original = cls.__dict__['run']
        recorder = self
        if is_async:

            async def wrapper(session, query, *args, **kwargs):
                async with recorder._call(query):
                    return await original(session, query, *args, **kwargs)

        else:

            def wrapper(session, query, *args, **kwargs):
                with recorder._call(query):
                    return original(session, query, *args, **kwargs)

        wrapper.__wrapped__ = original
        self._originals.append((cls, 'run', original))
        setattr(cls, 'run', wrapper)


# Our recorders, by flag: (flag, library, recorder class)
RECORDERS: Tuple[Tuple[str, str, Any], ...] = (
    ('db', 'sqlalchemy', SqlRecorder),
    ('redis', 'redis', RedisRecorder),
    ('model', 'ai.common.models', ModelRecorder),
    ('db', 'neo4j', Neo4jRecorder),
)


# ── Installing the library instrumentations (HTTP, AWS) ──────────────────────


def contrib_version() -> str:
    """The instrumentation release that matches the installed SDK: SDK 1.N pairs with contrib 0.(N+21)."""
    from opentelemetry.sdk.version import __version__

    minor = int(__version__.split('.')[1])
    return f'0.{minor + 21}'


def check_installed() -> List[str]:
    """
    Every instrumentation package of the table installed, at the SDK's release
    (the image build fails otherwise); their names. Not imported: an
    instrumentation imports its library, and the drivers come with the nodes.
    """
    from importlib.metadata import version

    release = contrib_version()
    names = sorted({package for *_, package in INSTRUMENTATIONS} | set(EXTRA_PACKAGES))
    wrong = [f'{name} {version(name)}' for name in names if not version(name).startswith(release + 'b')]
    if wrong:
        raise RuntimeError(f'instrumentations not at {release}.x: {", ".join(wrong)}')
    return names


def install() -> None:
    """Install the instrumentations: the telemetry requirements.txt pins them (RR_OTEL_AUTO_INSTALL at run time)."""
    from . import install_sdk

    install_sdk()


def parse_flags(value: Optional[str]) -> Tuple[List[str], List[str]]:
    """RR_OTEL_INSTRUMENT ('db,redis,http,aws' when unset; 'none' or '' for none) → (flags, unknown names)."""
    if value is None:
        return list(FLAGS), []
    names = [name.strip().lower() for name in value.replace(' ', ',').split(',') if name.strip()]
    if names in ([], ['none']):
        return [], []
    return [name for name in FLAGS if name in names], [name for name in names if name not in FLAGS]


# The last instrument()'s arguments: instrument_new() records, with them, the
# libraries that appear later (a node installs its driver with depends())
_STATE: Dict[str, Any] = {}
# depends() may run on several threads at once: one install at a time
_install_lock = threading.Lock()


def instrument(
    tracer_provider: Any,
    flags: Iterable[str],
    on_call: Callable[[Call], None],
    pass_trace_id: Optional[Callable[[], int]] = None,
) -> Tuple[List[str], List[str], List[Any]]:
    """
    Record the dependency calls this process makes, for the flags given.

    Returns (recorded libraries, skipped with the reason, recorders installed).
    A library that is not installed is not used here and is neither: it is
    tried again by instrument_new() once it is. A library instrumentation is
    skipped when it is missing, of another release than the SDK, or the
    library is already instrumented by someone else.
    """
    _STATE.clear()
    _STATE.update(
        provider=tracer_provider,
        wanted=set(flags),
        on_call=on_call,
        pass_trace_id=pass_trace_id,
        attempted=set(),
        missing=set(),
        recorders=[],
    )
    # Stable semconv names (http.request.method, url.full, ...): before any
    # instrumentation reads it. Never reaches a run (task_engine passes an
    # allowlisted environment).
    os.environ.setdefault('OTEL_SEMCONV_STABILITY_OPT_IN', 'http')
    with _install_lock:
        done, skipped = _instrument_available()
    return done, skipped, list(_STATE['recorders'])


def instrument_new() -> List[str]:
    """Record the libraries installed since instrument() (a node's depends()); the newly recorded ones."""
    if not _STATE:
        return []
    importlib.invalidate_caches()
    with _install_lock:
        done, _skipped = _instrument_available()
    return done


def instrument_missing() -> Tuple[List[str], List[str]]:
    """
    Try again the instrumentations whose package was not installed (after
    RR_OTEL_AUTO_INSTALL installed them): (recorded libraries, skipped with reasons).
    Only those: the recorders and instrumentations already in place stay as they are.
    """
    if not _STATE:
        return [], []
    importlib.invalidate_caches()
    with _install_lock:
        _STATE['attempted'] -= _STATE['missing']
        _STATE['missing'].clear()
        return _instrument_available()


def _instrument_available() -> Tuple[List[str], List[str]]:
    """Install a recorder or an instrumentation for each wanted library now importable, once. Under _install_lock."""
    wanted, attempted = _STATE['wanted'], _STATE['attempted']
    done: List[str] = []
    skipped: List[str] = []
    for flag, library, recorder_class in RECORDERS:
        key = ('recorder', library)
        if flag not in wanted or key in attempted or importlib.util.find_spec(library) is None:
            continue
        attempted.add(key)
        try:
            recorder = (
                recorder_class(_STATE['on_call'], _STATE['pass_trace_id'])
                if recorder_class is ModelRecorder
                else recorder_class(_STATE['on_call'])
            )
            if recorder.install():
                _STATE['recorders'].append(recorder)
                done.append(library)
        except Exception as e:
            skipped.append(f'{library}: {type(e).__name__}: {e}')

    from opentelemetry.metrics import NoOpMeterProvider

    release = contrib_version()
    for flag, module, name, library, package in INSTRUMENTATIONS:
        key = (module, name)
        if flag not in wanted or key in attempted or importlib.util.find_spec(library) is None:
            continue
        attempted.add(key)
        try:
            from importlib.metadata import PackageNotFoundError, version

            installed = version(package)
        except PackageNotFoundError:
            _STATE['missing'].add(key)
            skipped.append(f'{library}: {package} not installed')
            continue
        except Exception as e:
            skipped.append(f'{library}: {package}: {type(e).__name__}: {e}')
            continue
        if not installed.startswith(release + 'b') and installed != release:
            skipped.append(f'{library}: instrumentation {installed} does not match the SDK (needs {release}.x)')
            continue
        try:
            instrumentor = getattr(importlib.import_module(module), name)()
            if instrumentor.is_instrumented_by_opentelemetry:
                skipped.append(f'{library}: already instrumented')
                continue
            # The instrumentations' own meters stay off: DependencyBuffer measures their spans
            instrumentor.instrument(tracer_provider=_STATE['provider'], meter_provider=NoOpMeterProvider())
        except Exception as e:
            skipped.append(f'{library}: {type(e).__name__}: {e}')
            continue
        if instrumentor.is_instrumented_by_opentelemetry:
            done.append(library)
        else:
            skipped.append(f'{library}: version not supported by the instrumentation')
    return done, skipped


# ── Measuring a library span (every call, kept or not) ───────────────────────

# A database span inside a SQLAlchemy statement (psycopg2 under the engine):
# SqlRecorder already has that call
_IN_SQLALCHEMY: 'contextvars.ContextVar[bool]' = contextvars.ContextVar('rocketride_in_sqlalchemy', default=False)

# gRPC status codes, by number (rpc.grpc.status_code): the error type of a call
_GRPC_STATUS = (
    'OK',
    'CANCELLED',
    'UNKNOWN',
    'INVALID_ARGUMENT',
    'DEADLINE_EXCEEDED',
    'NOT_FOUND',
    'ALREADY_EXISTS',
    'PERMISSION_DENIED',
    'RESOURCE_EXHAUSTED',
    'FAILED_PRECONDITION',
    'ABORTED',
    'OUT_OF_RANGE',
    'UNIMPLEMENTED',
    'INTERNAL',
    'UNAVAILABLE',
    'DATA_LOSS',
    'UNAUTHENTICATED',
)

# Client spans open in this process. One inside another (urllib3 under
# Elasticsearch's own span) is not measured: the outermost is the call.
MAX_OPEN_CLIENTS = 10_000
_OPEN_CLIENTS: 'OrderedDict[int, None]' = OrderedDict()
_open_lock = threading.Lock()


def client_started(span: Any) -> None:
    """A span processor's on_start: remember an open client span."""
    if span.kind == SpanKind.CLIENT:
        with _open_lock:
            _OPEN_CLIENTS[span.context.span_id] = None
            if len(_OPEN_CLIENTS) > MAX_OPEN_CLIENTS:
                _OPEN_CLIENTS.popitem(last=False)  # one that never ended


def client_ended(span: Any) -> bool:
    """A span processor's on_end: forget it; True when it is to be measured (not inside another client span)."""
    if span.kind != SpanKind.CLIENT:
        return True
    parent = span.parent.span_id if span.parent is not None else None
    with _open_lock:
        _OPEN_CLIENTS.pop(span.context.span_id, None)
        return parent is None or parent not in _OPEN_CLIENTS


def _peer(attributes: Dict[str, Any]) -> Optional[str]:
    """The server's host (server.address, or the older net.peer.name), without a port."""
    value = attributes.get('server.address') or attributes.get('net.peer.name')
    if not value:
        return None
    host = str(value)
    if host.count(':') == 1:  # host:port (an IPv6 address has more)
        host = host.split(':', 1)[0]
    return host[:253]


def _name(value: Any) -> Optional[str]:
    """A gRPC service or method name: the proto's, bounded."""
    return str(value)[:128] if value else None


def measurement(span: ReadableSpan) -> Optional[Tuple[str, float, Dict[str, Any]]]:
    """(metric, seconds, attributes) for a client span (HTTP, AWS, gRPC, a database), or None when it is not one."""
    if span.start_time is None or span.end_time is None or span.kind != SpanKind.CLIENT:
        return None
    # One copy: lookups on the span's read-only mapping are several times slower
    attributes = dict(span.attributes or ())
    seconds = max(0.0, (span.end_time - span.start_time) / 1e9)
    error_type = _error_type(span, attributes)
    rpc_system = attributes.get('rpc.system.name') or attributes.get('rpc.system')
    if rpc_system == 'aws-api':
        return (
            'rpc.client.call.duration',
            seconds,
            {
                'rpc.system.name': 'aws-api',
                'rpc.service': attributes.get('rpc.service'),
                'rpc.method': attributes.get('rpc.method'),
                'error.type': error_type,
            },
        )
    if rpc_system == 'grpc':
        code = attributes.get('rpc.grpc.status_code')
        if isinstance(code, int) and 0 < code < len(_GRPC_STATUS):
            # The status (UNAVAILABLE, DEADLINE_EXCEEDED) says what happened; the
            # exception's class (grpc's _InactiveRpcError) does not
            error_type = _GRPC_STATUS[code]
        return (
            'rpc.client.call.duration',
            seconds,
            {
                'rpc.system.name': 'grpc',
                'rpc.service': _name(attributes.get('rpc.service')),
                'rpc.method': _name(attributes.get('rpc.method')),
                'server.address': _peer(attributes),
                'error.type': error_type,
            },
        )
    # Before HTTP: a database client over HTTP (Elasticsearch) is a database call.
    # Never the statement, the collection or the database's name: the operation
    db_system = attributes.get('db.system.name') or attributes.get('db.system')
    if db_system:
        if _IN_SQLALCHEMY.get():
            return None
        operation = attributes.get('db.operation.name') or attributes.get('db.operation')
        operation = operation or attributes.get('db.statement') or attributes.get('db.query.text')
        if db_system == 'mongodb':
            # A command name as MongoDB spells it (find, findAndModify), as semconv has it
            words = str(operation or '').split(None, 1)
            operation = words[0] if words and _OPERATION.match(words[0]) else '_OTHER'
        else:
            operation = _operation(operation)
        return (
            'db.client.operation.duration',
            seconds,
            {
                'db.system.name': str(db_system)[:32],
                'db.operation.name': operation,
                'server.address': _peer(attributes),
                'error.type': error_type,
            },
        )
    method = attributes.get('http.request.method') or attributes.get('http.method')
    if method:
        status = attributes.get('http.response.status_code') or attributes.get('http.status_code')
        address = attributes.get('server.address') or attributes.get('net.peer.name')
        url = attributes.get('url.full') or attributes.get('http.url')
        return (
            'http.client.request.duration',
            seconds,
            {
                'http.request.method': method,
                'server.address': address or _host(url),
                'server.port': attributes.get('server.port') or attributes.get('net.peer.port') or _port(url),
                'url.template': attributes.get('url.template') or url_template(url),
                'http.response.status_code': status,
                'error.type': error_type,
            },
        )
    return None


def _error_type(span: ReadableSpan, attributes: Dict[str, Any]) -> Optional[str]:
    if attributes.get('error.type'):
        return str(attributes['error.type'])
    if span.status.status_code != StatusCode.ERROR:
        return None
    for event in span.events:
        if event.name == 'exception' and event.attributes and event.attributes.get('exception.type'):
            return str(event.attributes['exception.type']).rsplit('.', 1)[-1]
    return '_OTHER'


def _host(url: Any) -> Optional[str]:
    match = re.match(r'^[a-z][a-z0-9+.-]*://(?:[^@/]*@)?([^/:?#]+)', str(url or ''), re.IGNORECASE)
    return match.group(1) if match else None


def _port(url: Any) -> Optional[int]:
    """The URL's port, or its scheme's default (http 80, https 443)."""
    match = re.match(r'^([a-z][a-z0-9+.-]*)://(?:[^@/]*@)?[^/:?#]+(?::(\d+))?', str(url or ''), re.IGNORECASE)
    if not match:
        return None
    if match.group(2):
        return int(match.group(2))
    return {'http': 80, 'https': 443, 'ws': 80, 'wss': 443}.get(match.group(1).lower())


# ── What leaves the process ──────────────────────────────────────────────────


_URL = re.compile(
    r'\b([a-z][a-z0-9+.-]*)://(?:[^@/\s]*@)?(\[[0-9a-f:.]{2,45}\]|[^/:?#\s\'"]+)(:\d{1,5})?[^\s\'"]*', re.IGNORECASE
)


def _scheme(url: Any) -> Optional[str]:
    match = _URL.match(str(url or ''))
    return match.group(1).lower() if match else None


# The routes an HTTP call's url.template may name: the LLM, embedding and vector
# store APIs the nodes call. A path is never exported as it is (it can carry a
# token: api.telegram.org/bot<token>/...); a known route is, with its variable
# segments (a model, a deployment, a collection) as placeholders. Anything else
# is _OTHER. (pattern matched at the end of the path, template)
_ROUTES = tuple(
    (re.compile(pattern + r'$'), template)
    for pattern, template in (
        (
            r'/openai/deployments/[^/]+/(chat/completions|completions|embeddings|responses)',
            r'/openai/deployments/{deployment}/\1',
        ),
        (
            r'/models/[^/:]+:(generateContent|streamGenerateContent|embedContent|batchEmbedContents|countTokens)',
            r'/models/{model}:\1',
        ),
        (r'/model/[^/]+/(invoke|converse|invoke-with-response-stream|converse-stream)', r'/model/{model}/\1'),
        (
            r'/collections/[^/]+/points(/search|/query|/scroll|/delete|/payload|/count|/recommend)?',
            r'/collections/{collection}/points\1',
        ),
        (r'/collections/[^/]+', r'/collections/{collection}'),
        (r'/(chat/completions|completions|embeddings|responses|messages|moderations|models|rerank)', r'/\1'),
        (r'/(audio/transcriptions|audio/speech|images/generations)', r'/\1'),
        (r'/api/(chat|generate|embed|embeddings|tags|show|ps)', r'/api/\1'),
        (r'/(chat|embed|generate)', r'/\1'),
    )
)
# A path prefix kept in the template: at most 3 short segments of letters (a
# version such as v1 or v1beta), never an id or a token
_SAFE_SEGMENT = re.compile(r'^(?:v\d+(?:beta|alpha)?\d*|[a-z][a-z-]{1,15})$')


def url_template(url: Any) -> Optional[str]:
    """The call's route as a template (url.template), '_OTHER' for an unknown path, None without a URL."""
    match = re.match(r'^[a-z][a-z0-9+.-]*://[^/?#]*([^?#]*)', str(url or ''), re.IGNORECASE)
    if not match:
        return None
    path = match.group(1) or '/'
    for pattern, template in _ROUTES:
        found = pattern.search(path)
        if found is None:
            continue
        segments = [segment for segment in path[: found.start()].split('/') if segment]
        if len(segments) > 3 or not all(_SAFE_SEGMENT.match(segment) for segment in segments):
            return '_OTHER'
        return ''.join(f'/{segment}' for segment in segments) + found.expand(template)
    return '_OTHER'


def call_name(method: Any, template: Optional[str]) -> Optional[str]:
    """An HTTP client span's name: '{method} {url.template}' for a known route, the method alone otherwise."""
    if not method:
        return None
    return f'{method} {template}' if template and template != '_OTHER' else str(method)


def _clean_text(text: Any, redact: Callable[[str], str]) -> str:
    """An error text without SQL statement or parameters (SQLAlchemy appends both), URLs cut to
    scheme://host[:port] (a path can carry a token: api.telegram.org/bot<token>/...), redacted.
    """
    return redact(_URL.sub(r'\1://\2\3', _SQL_DETAIL.sub('', str(text))))


# The attributes a dependency span keeps (current and older semantic-convention names)
SPAN_ATTRIBUTES = frozenset(
    (
        'server.address',
        'server.port',
        'net.peer.name',
        'net.peer.port',
        'url.scheme',
        'url.template',
        'http.request.method',
        'http.method',
        'http.response.status_code',
        'http.status_code',
        'http.request.resend_count',
        'error.type',
        'db.system',
        'db.system.name',
        'db.operation',
        'db.operation.name',
        'db.response.status_code',
        'rpc.system',
        'rpc.system.name',
        'rpc.service',
        'rpc.method',
        'rpc.grpc.status_code',
        'aws.region',
        'cloud.region',
        'rocketride.component.type',
        'rocketride.dependency.origin',
    )
)


def sanitized(span: ReadableSpan, redact: Callable[[str], str]) -> ReadableSpan:
    """
    A copy of a dependency span with only the attributes known to be safe
    (SPAN_ATTRIBUTES: where the call went and how it ended), never a statement,
    a URL path or query, a database or user name, a header or a user agent.
    """
    raw = dict(span.attributes or {})
    # Never a URL: its path can carry a secret (a bot token, a webhook key);
    # the server and the scheme say where the call went
    url = raw.get('url.full') or raw.get('http.url')
    attributes = {key: value for key, value in raw.items() if key in SPAN_ATTRIBUTES}
    if url and _scheme(url):
        attributes.setdefault('url.scheme', _scheme(url))
    template = attributes.get('url.template') or url_template(url) if url else attributes.get('url.template')
    if template:
        attributes['url.template'] = template
    if url and not attributes.get('server.port') and _port(url):
        attributes['server.port'] = _port(url)
    events = []
    for event in span.events:
        values = dict(event.attributes or {})
        for key in ('exception.message', 'exception.stacktrace'):
            if key in values:
                values[key] = _clean_text(values[key], redact)
        events.append(Event(event.name, values, event.timestamp))
    status = span.status
    if status.description:
        status = Status(status.status_code, _clean_text(status.description, redact))
    method = attributes.get('http.request.method') or attributes.get('http.method')
    return ReadableSpan(
        # an HTTP call named by its route ('POST /v1/chat/completions'), as the conventions name it
        name=call_name(method, attributes.get('url.template')) or span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=attributes,
        events=events,
        links=span.links,
        kind=span.kind,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


# ── Holding spans until their trace decides ──────────────────────────────────


class DependencyBuffer(SpanProcessor):
    """
    The span processor in front of the exporter.

    Spans of our own scope pass through: they are built only once kept. The
    HTTP and AWS instrumentations' spans are measured, then held per request
    (the one current when the span started: several requests can share a
    caller's trace) until it decides (decide): dropped with it or kept with it.
    One started outside any request is its own root and decides itself.
    """

    def __init__(
        self,
        delegate: SpanProcessor,
        own_scope: str,
        keep_root: Callable[[ReadableSpan], str],
        measure: Callable[[str, float, Dict[str, Any]], None],
        count_dropped: Callable[[str, int], None],
        redact: Callable[[str], str],
    ) -> None:
        self._delegate = delegate
        self._own_scope = own_scope
        self._keep_root = keep_root
        self._measure = measure
        self._count_dropped = count_dropped
        self._redact = redact
        self._lock = threading.Lock()
        # span id → (the request it started under, when): stamped in on_start
        self._owner: 'OrderedDict[int, Tuple[int, float]]' = OrderedDict()
        # request span id → [first seen (monotonic), held spans]
        self._pending: 'OrderedDict[int, List[Any]]' = OrderedDict()
        self._held = 0
        # request span id → (kept, decided at (monotonic))
        self._decided: 'OrderedDict[int, Tuple[bool, float]]' = OrderedDict()

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        client_started(span)
        request = current_request()  # on_start runs in the caller's context
        if request is not None and getattr(span.instrumentation_scope, 'name', '') != self._own_scope:
            with self._lock:
                if len(self._owner) < MAX_HELD * 2:
                    self._owner[span.context.span_id] = (request.span_id, time.monotonic())
        self._delegate.on_start(span, parent_context=parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        if (span.instrumentation_scope.name if span.instrumentation_scope else '') == self._own_scope:
            self._delegate.on_end(span)
            return
        try:
            measured = measurement(span) if client_ended(span) else None
            if measured is not None:
                self._measure(*measured)
        except Exception:
            self._count_dropped('dependency_metric_error', 1)
        now = time.monotonic()
        keep: Optional[bool] = None
        full = False
        with self._lock:
            expired = self._expire(now)
            owned = self._owner.pop(span.context.span_id, None)
            owner = owned[0] if owned is not None else None
            if owner is not None:
                decided = self._decided.get(owner)
                if decided is not None:
                    keep = decided[0]
                else:
                    pending = self._pending.get(owner)
                    full = self._held >= MAX_HELD or (pending is not None and len(pending[1]) >= MAX_SPANS_PER_TRACE)
                    if not full:
                        if pending is None:
                            pending = self._pending[owner] = [now, []]
                        pending[1].append(span)
                        self._held += 1
        if expired:
            self._count_dropped('dependency_undecided', expired)
        if full:
            self._count_dropped('dependency_span_cap', 1)
        if keep:
            self._release(span)
        if owner is None and self._keep_root(span):
            # A call outside any request decides itself: kept when it failed or was slow
            self._release(span)

    def decide(self, request_id: int, keep: bool) -> None:
        """A request decided: release its held spans (kept) or drop them; later ones follow."""
        now = time.monotonic()
        with self._lock:
            pending = self._pending.pop(request_id, None)
            if pending is not None:
                self._held -= len(pending[1])
            self._decided[request_id] = (keep, now)
            self._decided.move_to_end(request_id)
            while len(self._decided) > MAX_DECIDED:
                self._decided.popitem(last=False)
        if keep and pending is not None:
            for span in pending[1]:
                self._release(span)

    def _release(self, span: ReadableSpan) -> None:
        try:
            self._delegate.on_end(sanitized(span, self._redact))
        except Exception:
            self._count_dropped('dependency_span_error', 1)

    def _expire(self, now: float) -> int:
        """Forget old decisions and drop requests never decided (under the lock); the count dropped."""
        while self._decided:
            request_id, (_, at) = next(iter(self._decided.items()))
            if now - at < DECIDED_SECONDS:
                break
            self._decided.popitem(last=False)
        # A span that started and never ended (a stream nobody closed) leaves its stamp
        while self._owner:
            span_id, (_, at) = next(iter(self._owner.items()))
            if now - at < UNDECIDED_SECONDS:
                break
            self._owner.popitem(last=False)
        dropped = 0
        while self._pending:
            request_id, (first, spans) = next(iter(self._pending.items()))
            if now - first < UNDECIDED_SECONDS:
                break
            self._pending.popitem(last=False)
            self._held -= len(spans)
            dropped += len(spans)
        return dropped

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._delegate.force_flush(timeout_millis)
