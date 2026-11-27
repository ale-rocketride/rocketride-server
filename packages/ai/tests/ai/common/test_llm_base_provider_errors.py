# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""
A provider error in an LLM node (writeQuestions): the user gets the error as
the answer, and the failure is visible as an ERROR log line naming why. The
question is not marked failed (no completion code): that comes with the cause
of each failure (customer, provider, platform), so a customer's key does not
count against the platform.
"""

from __future__ import annotations

from typing import Any, List

import pytest

import ai.common.llm_base as llm_base
from ai.common.llm_base import LLMBase, provider_error_type
from ai.common.telemetry import relay


class _Object:
    def __init__(self) -> None:
        self.codes: List[Any] = []

    def completionCode(self, *args: Any) -> None:
        self.codes.append(args)


class _Inner:
    def __init__(self) -> None:
        self.written: List[Any] = []
        self.currentObject = _Object()

    def writeAnswers(self, answer: Any) -> None:
        self.written.append(answer)

    def sendSSE(self, *args: Any, **kwargs: Any) -> None:
        pass


class _Question:
    question = 'q'


class _StatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f'status {status}')
        self.status_code = status


class AuthenticationError(Exception):
    pass


class APIConnectionError(Exception):
    pass


def _node(exc: BaseException) -> LLMBase:
    class _Stub(LLMBase):
        def _question(self, question, **kwargs):
            raise exc

    node = _Stub.__new__(_Stub)
    node.instance = _Inner()
    return node


@pytest.mark.parametrize(
    'exc, expected',
    [
        (_StatusError(401), 'provider_auth'),
        (_StatusError(403), 'provider_auth'),
        (AuthenticationError('Incorrect API key provided'), 'provider_auth'),
        (_StatusError(429), 'provider_rate_limited'),
        (_StatusError(503), 'provider_unavailable'),
        (APIConnectionError('connect'), 'provider_unreachable'),
        (ValueError('bad'), 'provider_error'),
    ],
)
def test_provider_error_type_is_bounded(exc, expected):
    assert provider_error_type(exc) == expected


def test_a_rejected_key_is_an_error_line_and_the_same_answer(monkeypatch):
    lines: List[str] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: lines.append(' '.join(map(str, args))))
    node = _node(_StatusError(401))

    node.writeQuestions(_Question())

    # The user still gets the error as the answer
    assert '**LLM error**' in node.instance.written[0].getText()
    # The run's error log names why
    assert len(lines) == 1 and '(provider_auth)' in lines[0]
    # Not marked failed: the API response and the run's warnings stay as they were
    assert node.instance.currentObject.codes == []


def test_the_error_line_names_the_question_trace(monkeypatch):
    lines: List[str] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: lines.append(' '.join(map(str, args))))
    node = _node(_StatusError(401))
    node.instance.passTraceId = 'ab' * 16

    node.writeQuestions(_Question())

    assert lines[0].endswith('pass_trace_id=' + 'ab' * 16)


def test_the_failure_marks_the_question_trace_redacted(monkeypatch):
    marks: List[Any] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: None)
    monkeypatch.setattr(relay, 'node_error', lambda kind, message: marks.append((kind, message)))
    node = _node(AuthenticationError('Incorrect API key provided: sk-proj-abcdefghijklmnopqrstuvwxyz123456'))

    node.writeQuestions(_Question())

    ((kind, message),) = marks
    assert kind == 'provider_auth'
    assert 'abcdefghijklmnopqrstuvwxyz123456' not in message  # the key never reaches the trace


@pytest.mark.parametrize(
    'text, secret',
    [
        ('Incorrect API key provided: sk-proj-abcdefghijklmnop1234', 'abcdefghijklmnop1234'),  # gitleaks:allow
        (
            '400 https://generativelanguage.googleapis.com/v1/models?key=AIzaSyAbcdefghijkl123',  # gitleaks:allow
            'AIzaSyAbcdefghijkl123',
        ),
        ('echo: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abcdefgh', 'eyJhbGciOiJIUzI1NiJ9'),
        ('invalid x-api-key: sk-ant-api03-abcdefghijklmnop', 'abcdefghijklmnop'),
    ],
)
def test_provider_messages_reach_the_trace_without_the_key(monkeypatch, text, secret):
    marks: List[Any] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: None)
    monkeypatch.setattr(relay, 'node_error', lambda kind, message: marks.append(message))

    _node(AuthenticationError(text)).writeQuestions(_Question())

    assert secret not in marks[0]


class _Wrapped(Exception):
    """An engine or SDK wrapper that kept only the provider error's text."""


@pytest.mark.parametrize(
    'exc, expected',
    [
        # a 4xx other than auth and rate limits: a request we built wrong
        (_StatusError(400), 'provider_error'),
        (_StatusError(404), 'provider_error'),
        (_StatusError(422), 'provider_error'),
        # only the text kept the status (the agent's planner, through the engine)
        (_Wrapped("Error code: 503 - {'error': {'message': 'simulated provider error'}}"), 'provider_unavailable'),
        (_Wrapped('Error code: 401 - invalid key'), 'provider_auth'),
        (_Wrapped('Error code: 400 - bad request'), 'provider_error'),
    ],
)
def test_provider_error_type_reads_a_status_from_the_text(exc, expected):
    assert provider_error_type(exc) == expected


def test_provider_error_type_follows_the_exception_it_was_raised_from():
    try:
        try:
            raise _StatusError(502)
        except _StatusError as inner:
            raise RuntimeError('the agent could not plan') from inner
    except RuntimeError as outer:
        assert provider_error_type(outer) == 'provider_unavailable'


def test_an_unknown_failure_takes_the_default():
    assert provider_error_type(ValueError('bad')) == 'provider_error'
    assert provider_error_type(ValueError('bad'), default='agent_error') == 'agent_error'


def test_report_llm_failure_logs_why_and_marks_the_question(monkeypatch):
    lines: List[str] = []
    marks: List[Any] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: lines.append(' '.join(map(str, args))))
    monkeypatch.setattr(relay, 'node_error', lambda kind, message: marks.append((kind, message)))
    instance = _Inner()
    instance.passTraceId = 'cd' * 16

    kind = llm_base.report_llm_failure(
        'rocketride wave plan failed run_id=r1', _Wrapped('Error code: 503 - down'), instance=instance
    )

    assert kind == 'provider_unavailable'
    assert lines == [
        'rocketride wave plan failed run_id=r1: LLM call failed (provider_unavailable): _Wrapped: Error code: 503 - down'
        ' pass_trace_id=' + 'cd' * 16
    ]
    assert marks == [('provider_unavailable', '_Wrapped: Error code: 503 - down')]


@pytest.mark.parametrize('status', [504, 408, 499, 524])
def test_a_request_the_provider_timed_out_or_closed_is_a_timeout(status):
    assert provider_error_type(_StatusError(status)) == 'provider_timeout'


class APITimeoutError(APIConnectionError):
    """As the OpenAI SDK: its timeout error is a kind of its connection error."""


class ReadTimeout(Exception):
    pass


class ConnectError(Exception):
    pass


class gaierror(Exception):  # noqa: N801 (socket.gaierror's name)
    pass


@pytest.mark.parametrize(
    'exc, expected',
    [
        # the provider says it failed: its state, whatever was asked
        (_StatusError(500), 'provider_unavailable'),
        (_StatusError(503), 'provider_unavailable'),
        (_StatusError(529), 'provider_unavailable'),
        # no answer at all: DNS, a refused connection
        (ConnectError('refused'), 'provider_unreachable'),
        (gaierror('Name or service not known'), 'provider_unreachable'),
        (ConnectionRefusedError('refused'), 'provider_unreachable'),
        # no answer in time: a slow provider or a very large request
        (APITimeoutError('Request timed out.'), 'provider_timeout'),
        (ReadTimeout('read timed out'), 'provider_timeout'),
        (_Wrapped('Error code: 504 - gateway timeout'), 'provider_timeout'),
    ],
)
def test_the_three_ways_a_provider_fails_are_told_apart(exc, expected):
    assert provider_error_type(exc) == expected


def test_report_without_mark_logs_but_does_not_fail_the_question(monkeypatch):
    lines: List[str] = []
    marks: List[Any] = []
    monkeypatch.setattr(llm_base, 'error', lambda *args: lines.append(' '.join(map(str, args))))
    monkeypatch.setattr(relay, 'node_error', lambda kind, message: marks.append((kind, message)))

    llm_base.report_llm_failure('rocketride wave plan failed run_id=r2', _StatusError(503), mark=False)

    assert len(lines) == 1 and '(provider_unavailable)' in lines[0]
    assert marks == []  # the agent answered from the work it gathered


def _raised_from(inner: BaseException, outer: BaseException) -> BaseException:
    try:
        try:
            raise inner
        except BaseException as e:
            raise outer from e
    except BaseException as e:
        return e


def test_an_sdk_timeout_around_a_connect_timeout_is_unreachable():
    # openai and anthropic raise APITimeoutError from every httpx timeout: a NAT
    # dropping packets (the connect times out) is our egress, not a slow provider
    httpx = pytest.importorskip('httpx')
    openai = pytest.importorskip('openai')
    request = httpx.Request('POST', 'https://api.openai.com/v1/chat/completions')
    connect = _raised_from(httpx.ConnectTimeout('timed out'), openai.APITimeoutError(request=request))
    read = _raised_from(httpx.ReadTimeout('timed out'), openai.APITimeoutError(request=request))
    assert provider_error_type(connect) == 'provider_unreachable'
    assert provider_error_type(read) == 'provider_timeout'


def test_a_cut_mid_answer_is_a_timeout_not_unreachable():
    # a proxy closing a long request: ambiguous like a timeout, not our egress
    httpx = pytest.importorskip('httpx')
    openai = pytest.importorskip('openai')
    request = httpx.Request('POST', 'https://api.openai.com/v1/chat/completions')
    cut = _raised_from(httpx.RemoteProtocolError('peer closed connection'), openai.APIConnectionError(request=request))
    assert provider_error_type(cut) == 'provider_timeout'
    assert provider_error_type(ConnectionResetError()) == 'provider_timeout'
    assert provider_error_type(httpx.ReadError('reset')) == 'provider_timeout'


def test_a_status_in_the_chain_wins_over_a_connect_failure_before_it():
    # an earlier attempt's connect failure, then the provider answered 503
    assert provider_error_type(_raised_from(ConnectionRefusedError(), _StatusError(503))) == 'provider_unavailable'


def test_tls_failures_are_unreachable():
    import ssl

    assert provider_error_type(ssl.SSLError('handshake')) == 'provider_unreachable'


def test_bedrock_errors_are_told_by_aws_error_code_and_status():
    exceptions = pytest.importorskip('botocore.exceptions')

    def client_error(code, status):
        return exceptions.ClientError(
            {'Error': {'Code': code, 'Message': 'm'}, 'ResponseMetadata': {'HTTPStatusCode': status}}, 'Converse'
        )

    assert provider_error_type(client_error('ThrottlingException', 429)) == 'provider_rate_limited'
    assert provider_error_type(client_error('ServiceUnavailableException', 503)) == 'provider_unavailable'
    assert provider_error_type(client_error('ModelTimeoutException', 408)) == 'provider_timeout'
    assert provider_error_type(client_error('AccessDeniedException', 403)) == 'provider_auth'
    assert provider_error_type(client_error('ValidationException', 400)) == 'provider_error'
    url = 'https://bedrock-runtime.us-east-1.amazonaws.com'
    assert provider_error_type(exceptions.EndpointConnectionError(endpoint_url=url)) == 'provider_unreachable'
    assert provider_error_type(exceptions.ConnectTimeoutError(endpoint_url=url)) == 'provider_unreachable'
    assert provider_error_type(exceptions.ReadTimeoutError(endpoint_url=url)) == 'provider_timeout'


class _GenaiError(Exception):
    """As google-genai's APIError: the HTTP status in .code, the gRPC status name in .status."""

    def __init__(self, code: int, status: str) -> None:
        super().__init__(f'{code} {status}. {{}}')
        self.code = code
        self.status = status
        self.response = None


class ServerError(_GenaiError):
    pass


class ClientError(_GenaiError):
    pass


@pytest.mark.parametrize(
    'exc, expected',
    [
        (ServerError(503, 'UNAVAILABLE'), 'provider_unavailable'),
        (ServerError(500, 'INTERNAL'), 'provider_unavailable'),
        (ServerError(504, 'DEADLINE_EXCEEDED'), 'provider_timeout'),
        (ClientError(429, 'RESOURCE_EXHAUSTED'), 'provider_rate_limited'),
        (ClientError(403, 'PERMISSION_DENIED'), 'provider_auth'),
        (ClientError(400, 'INVALID_ARGUMENT'), 'provider_error'),
    ],
)
def test_gemini_errors_are_told_by_their_code(exc, expected):
    assert provider_error_type(exc) == expected


def test_an_agents_python_instance_still_names_the_questions_trace(monkeypatch):
    """Agents pass their Python IInstance: its .instance (the engine's) holds passTraceId."""
    from types import SimpleNamespace

    from ai.common import llm_base

    lines = []
    monkeypatch.setattr(llm_base, 'error', lines.append)
    monkeypatch.setattr(relay, 'current_pass_trace_id', lambda: 0)
    python_instance = SimpleNamespace(instance=SimpleNamespace(passTraceId='ab' * 16))
    llm_base.report_llm_failure('agent failed', RuntimeError('x'), instance=python_instance, mark=False)
    assert lines and lines[0].endswith('pass_trace_id=' + 'ab' * 16)


def test_a_local_error_raised_from_none_is_not_the_providers():
    # `raise ... from None` hides the connect failure it was handling
    try:
        try:
            raise ConnectionRefusedError()
        except ConnectionRefusedError:
            raise RuntimeError('local') from None
    except RuntimeError as e:
        hidden = e
    assert provider_error_type(hidden) == 'provider_error'
    # an implicit context still decides
    try:
        try:
            raise ConnectionRefusedError()
        except ConnectionRefusedError:
            raise RuntimeError('wrapped')
    except RuntimeError as e:
        implicit = e
    assert provider_error_type(implicit) == 'provider_unreachable'


class _OddResponseError(Exception):
    def __init__(self) -> None:
        super().__init__('odd')
        self.response = {'Error': 'not a mapping', 'ResponseMetadata': ['nor this']}


def test_an_unknown_response_shape_falls_back_to_the_class():
    assert provider_error_type(_OddResponseError()) == 'provider_error'


def test_a_failing_report_still_writes_the_error_answer(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError('report failed')

    monkeypatch.setattr(llm_base, 'report_llm_failure', boom)
    node = _node(_StatusError(401))

    node.writeQuestions(_Question())

    assert '**LLM error**' in node.instance.written[0].getText()
