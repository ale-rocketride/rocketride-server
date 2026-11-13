# Copyright (c) 2026 Aparavi Software AG

import inspect
import re
from typing import Callable, List, Optional

from rocketlib import IInstanceBase, error, invoke_function
from ai.common import logfmt
from ai.common.telemetry import relay
from ai.common.schema import Question, Answer, AnswerError
from ai.common.llm_native_stream import STOP_SEQUENCES_VAR
from ai.common.llm_adapter import turn_usage
from ai.common.llm_tools import InvalidTurn, ToolsUnsupported, is_turn


def _turn_error(e: Exception) -> AnswerError:
    """Type a failed turn, so a client can tell its own mistake from the provider failing.

    Only our own messages go back. The provider's text can hold URLs, request ids and org
    names, so a provider error names just the exception type; the full text is logged.
    """
    if isinstance(e, ToolsUnsupported):
        return AnswerError(code='tools_unsupported', message=str(e))
    if isinstance(e, InvalidTurn):
        return AnswerError(code='invalid_request', message=str(e))
    return AnswerError(code='provider_error', message=f'the model provider call failed ({type(e).__name__})')


# Exception class names (and AWS error codes) across the SDKs the LLM nodes use:
# openai and anthropic (httpx), google-genai / google-api-core, botocore (Bedrock),
# requests
_AUTH_NAMES = (
    'AuthenticationError',
    'PermissionDeniedError',
    'Unauthorized',
    'Forbidden',
    'AccessDeniedException',
    'UnrecognizedClientException',
)
_RATE_NAMES = ('RateLimitError', 'ResourceExhausted', 'TooManyRequests', 'ThrottlingException')
# Three ways a provider fails, told apart because they mean different things:
# its answer says it failed (5xx: the provider's state, whatever was asked), it
# could not be reached (DNS, TCP, TLS: our egress or its edge), or it did not
# answer in time (a slow provider, or a very large request: ambiguous)
_UNAVAILABLE_NAMES = (
    'InternalServerError',
    'ServiceUnavailableError',
    'ServerError',
    'ServiceUnavailable',
    'ServiceUnavailableException',
    'InternalServerException',
)
# Failing to connect at all: these win over the SDK's own class wrapped around
# them (openai and anthropic raise APITimeoutError from httpx.ConnectTimeout: a
# NAT dropping packets is unreachable, not slow)
_CONNECT_NAMES = (
    'ConnectError',
    'ConnectTimeout',
    'ConnectTimeoutError',
    'ConnectionRefusedError',
    'EndpointConnectionError',
    'NameResolutionError',
    'NewConnectionError',
    'SSLError',
    'gaierror',
)
# Connected, then cut or not answered in time (a proxy closing a long request, a
# reset mid-answer): ambiguous like a timeout, and not our egress. These win over
# the SDK's own class too (openai's APIConnectionError around a RemoteProtocolError)
_MID_REQUEST_NAMES = (
    'ConnectionResetError',
    'ReadError',
    'ReadTimeout',
    'ReadTimeoutError',
    'RemoteProtocolError',
    'WriteError',
    'WriteTimeout',
)
_UNREACHABLE_NAMES = _CONNECT_NAMES + ('APIConnectionError', 'ConnectionError')
_TIMEOUT_NAMES = _MID_REQUEST_NAMES + (
    'APITimeoutError',
    'TimeoutError',
    'PoolTimeout',
    'DeadlineExceeded',
    'ModelTimeoutException',
)
# An HTTP status in an exception's text, when a wrapper kept only the text
# (e.g. "Exception: Error code: 503 - {...}")
_STATUS_IN_TEXT = re.compile(r'\b(?:Error code|status(?: code)?)\s*[:=]?\s*(\d{3})\b', re.IGNORECASE)


def _kind_of(status: Optional[int], name: str) -> Optional[str]:
    if status in (401, 403) or name in _AUTH_NAMES:
        return 'provider_auth'
    if status == 429 or name in _RATE_NAMES:
        return 'provider_rate_limited'
    # 504, 408 (the provider's edge timed the request out), 499 (it closed it) and
    # 524 (a CDN's origin timeout): did not answer in time, as a client timeout.
    # Timeout names first: an SDK's timeout error is a kind of its connection error
    if status in (504, 408, 499, 524) or name in _TIMEOUT_NAMES:
        return 'provider_timeout'
    if (isinstance(status, int) and status >= 500) or name in _UNAVAILABLE_NAMES:
        return 'provider_unavailable'
    if name in _UNREACHABLE_NAMES:
        return 'provider_unreachable'
    if isinstance(status, int) and 400 <= status < 500:
        return 'provider_error'
    return None


def _status_and_name(exc: BaseException):
    """The HTTP status and the name an exception tells: its class, or AWS's error code."""
    name = type(exc).__name__
    status = None
    for attr in ('status_code', 'status', 'code'):  # httpx / openai, aiohttp, google-genai
        value = getattr(exc, attr, None)
        if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
            status = value
            break
    response = getattr(exc, 'response', None)
    if isinstance(response, dict):  # botocore ClientError
        meta = response.get('ResponseMetadata') or {}
        if status is None and isinstance(meta.get('HTTPStatusCode'), int):
            status = meta['HTTPStatusCode']
        code = (response.get('Error') or {}).get('Code')
        if isinstance(code, str) and code:
            name = code
    elif status is None and response is not None:
        value = getattr(response, 'status_code', None)
        status = value if isinstance(value, int) else None
    return status, name


def provider_error_type(exc: BaseException, default: str = 'provider_error') -> str:
    """
    A bounded name for why a provider refused an LLM call, for logs and telemetry.

    provider_auth (401/403: a rejected or unauthorized key, usually the
    customer's configuration), provider_rate_limited (429: a quota or rate
    limit), provider_unavailable (5xx: the provider says it failed),
    provider_unreachable (no connection at all: DNS, a refused or timed-out
    connect, TLS), provider_timeout (connected but no answer in time, or cut
    mid-answer: 504/408/499/524 or a client timeout, a slow provider or a very
    large request), provider_error (another 4xx: a request we built wrong).
    The exception and the ones it was raised from: when none of them carries an
    HTTP status, a connect failure or a mid-request cut anywhere in the chain
    decides (the SDK's outer class says less); otherwise the outermost that
    tells, then a status in its text; `default` when none of them tells
    (agent_error for an agent's own failure).
    """
    chain = []
    current: Optional[BaseException] = exc
    while current is not None and all(current is not seen for seen in chain):
        chain.append(current)
        current = current.__cause__ or current.__context__
    told = [_status_and_name(link) for link in chain]
    if all(status is None for status, _ in told):
        names = [name for _, name in told]
        if any(name in _CONNECT_NAMES for name in names):
            return 'provider_unreachable'
        if any(name in _MID_REQUEST_NAMES for name in names):
            return 'provider_timeout'
    for status, name in told:
        kind = _kind_of(status, name)
        if kind:
            return kind
    match = _STATUS_IN_TEXT.search(str(exc))
    if match:
        return _kind_of(int(match.group(1)), '') or default
    return default


def report_llm_failure(
    where: str, exc: BaseException, default: str = 'provider_error', instance: object = None, mark: bool = True
) -> str:
    """
    A failed LLM call (or agent run) a node answers with an error text and goes
    on from: one ERROR line, with why and the question's own trace
    (pass_trace_id: the line's trace_id is the run's), and the node's call marked
    failed for EaaS (its span, the pass, and the turn's outcome and cause).
    instance: the node's engine instance (or its Python instance, whose
    .instance is the engine's), whose passTraceId names the question when the
    call is not in a relay scope. mark=False: the line only (the node
    still answers, e.g. an agent from the work it gathered). Returns the kind
    (provider_error_type).
    """
    kind = provider_error_type(exc, default)
    trace_id = relay.current_pass_trace_id()
    pass_trace = f'{trace_id:032x}' if trace_id else ''
    if not pass_trace:
        # the engine instance, or a node's Python instance holding it (agents pass that)
        text = getattr(instance, 'passTraceId', None)
        if text is None:
            text = getattr(getattr(instance, 'instance', None), 'passTraceId', '')
        pass_trace = text if isinstance(text, str) and len(text) == 32 else ''
    error(
        f'{where}: LLM call failed ({kind}): {type(exc).__name__}: {exc}'
        + (f' pass_trace_id={pass_trace}' if pass_trace else '')
    )
    if mark:
        relay.node_error(kind, logfmt.redact(f'{type(exc).__name__}: {exc}'))
    return kind


class LLMBase(IInstanceBase):
    """Shared base instance for LLM-style nodes.

    This class is the canonical node-level base for LLM providers and adapters.
    Provider-specific request/retry behavior remains in ai.common.chat.ChatBase.
    """

    def _question(
        self,
        question: Question,
        on_chunk: Optional[Callable[[str], None]] = None,
        on_finish: Optional[Callable[[Optional[str]], None]] = None,
        on_reasoning_chunk: Optional[Callable[[str], None]] = None,
        stop: Optional[List[str]] = None,
    ) -> Answer:
        chat = self.IGlobal._chat
        # Every driver passes here, including those that override chat(), so the refusal
        # happens before any provider call and costs no tokens.
        if is_turn(question) and not getattr(chat, 'supports_tools', False):
            raise ToolsUnsupported(getattr(chat, '_model', None) or type(chat).__name__)
        # Publish the stop sequences on the per-call contextvar so ChatBase's model sinks
        # forward them to the provider API. A contextvar (not a chat() kwarg) leaves the
        # many chat()/_chat() provider overrides untouched. Reset in finally so a stop from
        # one request never bleeds into the next on a reused chat instance.
        token = STOP_SEQUENCES_VAR.set(stop or None)
        try:
            # Legacy drivers override chat(self, question) without streaming callbacks.
            try:
                accepts_stream = 'on_chunk' in inspect.signature(chat.chat).parameters
            except (TypeError, ValueError):
                accepts_stream = True
            if not accepts_stream:
                return chat.chat(question)
            return chat.chat(
                question,
                on_chunk=on_chunk,
                on_finish=on_finish,
                on_reasoning_chunk=on_reasoning_chunk,
            )
        finally:
            STOP_SEQUENCES_VAR.reset(token)

    def writeQuestions(self, question: Question):
        # Stream the model's reasoning live, one line at a time, on the chat-ui
        # 'thinking' lane (same channel as agents). The answer is written normally.
        buf: list = []

        def _noop(_text: str) -> None:
            pass

        def _emit_thinking(message: str) -> None:
            if message:
                try:
                    self.instance.sendSSE('thinking', message=message)
                except Exception:
                    pass

        def _flush_reasoning(force: bool = False) -> None:
            # Emit complete lines; keep the trailing partial in buf so the next
            # delta can continue it — unless forced (long paragraph or stream end).
            lines = ''.join(buf).splitlines(keepends=True)
            remainder = lines.pop() if (lines and not force and not lines[-1].endswith('\n')) else ''
            buf[:] = [remainder] if remainder else []
            for line in lines:
                _emit_thinking(line.strip())

        def on_reasoning_chunk(text: str) -> None:
            if not text:
                return
            buf.append(text)
            # Stream live: emit complete lines, or force a flush once a newline-less
            # paragraph grows enough to show progress.
            if '\n' in text:
                _flush_reasoning()
            elif sum(len(p) for p in buf) >= 120:
                _flush_reasoning(force=True)

        # Scope this turn's model calls so a prior call's usage can't bleed into this
        # answer and the collector stays bounded to one turn.
        with turn_usage() as read_usage:
            try:
                answer = self._question(
                    question,
                    on_chunk=_noop,
                    on_reasoning_chunk=on_reasoning_chunk,
                )
            except Exception as e:
                # An error, not a warning: the run's error log (and its trace)
                # must show a question the provider refused, e.g. a customer's
                # rejected API key; the server redacts secrets from the line
                report_llm_failure('writeQuestions', e, instance=self.instance)
                if is_turn(question):
                    answer = Answer(error=_turn_error(e))
                else:
                    err_msg = f'**LLM error** — {type(e).__name__}: {e}'
                    answer = Answer()
                    answer.setAnswer(err_msg)
                # A turn that failed after burning tokens still spent them: the adapters
                # report from their own finally, so the scope holds what was billed —
                # hang it on the error answer or the Trace shows nothing for exactly the
                # turn the user opens.
                failed_usage = read_usage()
                if failed_usage:
                    answer.tokens = failed_usage
                self.instance.writeAnswers(answer)
                return

            _flush_reasoning(force=True)  # emit any trailing partial line
            usage = read_usage()  # turn total: summed across every model call this turn
            if usage:
                answer.tokens = usage  # shown in the Trace "Tokens" grid on the answers lane
            self.instance.writeAnswers(answer)

    @invoke_function
    def getContextLength(self, _param):
        return self.IGlobal._chat.getTotalTokens()

    @invoke_function
    def getOutputLength(self, _param):
        return self.IGlobal._chat.getOutputTokens()

    @invoke_function
    def getTokenCounter(self, _param):
        return self.IGlobal._chat.getTokens

    @invoke_function
    def ask(self, param):
        # Each invoke shows its own cost: the scope reads only the calls made while it
        # was open (its own, plus any nested sub-agent calls it drove). When an agent
        # drives this invoke, the agent's outer scope still sees the same calls for the
        # turn total; the outermost scope bounds and clears the collector.
        with turn_usage() as read_usage:
            answer = self._question(param.question, stop=getattr(param, 'stop', None))
            usage = read_usage()
            if usage:
                answer.tokens = usage  # shown in the Trace "Tokens" grid on this invoke's row
        return answer
