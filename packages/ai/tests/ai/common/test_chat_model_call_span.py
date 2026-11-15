"""A chat driver's logical model call reaches the trace: its tries, the waits and the final error."""

import pytest

from ai.common import chat as chat_module
from ai.common.chat import ChatBase
from ai.common.telemetry import relay


class _Unavailable(Exception):
    status_code = 503


class _Driver(ChatBase):
    def __init__(self, answers):
        # Only what _chat_with_retries reads; the full constructor loads node config
        self._provider = 'llm_openai'
        self._model = 'gpt-4o'
        self._modelTotalTokens = 100000
        self._is_reasoning = False
        self._raw_client = None
        self._answers = list(answers)

    def _chat(self, prompt):
        answer = self._answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def getTokens(self, value):
        return len(value)

    def is_retryable_error(self, error):
        return isinstance(error, _Unavailable)

    def map_exception(self, error):
        return error


@pytest.fixture
def calls(monkeypatch):
    seen = []
    monkeypatch.setattr(relay, 'model_call', lambda *args: seen.append(args))
    monkeypatch.setattr(relay, '_write', lambda line: None)  # the relay on, as in a task process
    monkeypatch.setattr(chat_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(chat_module, '_error_kind', lambda e: 'provider_unavailable')
    return seen


def test_a_call_answered_after_a_retry_records_the_retry(calls):
    assert _Driver([_Unavailable('busy'), 'hello']).chat_string('hi') == 'hello'
    ((operation, provider, model, start, end, error_type, message, retries, usage),) = calls
    assert (operation, provider, model, error_type, message) == ('chat', 'openai', 'gpt-4o', None, '')
    assert end >= start > 0
    ((at, kind, wait),) = retries
    assert start <= at <= end and kind == 'provider_unavailable' and wait > 0


def test_a_call_that_fails_records_the_error_with_its_message(calls):
    with pytest.raises(ValueError):
        _Driver([ValueError('bad request')]).chat_string('hi')
    ((_op, _provider, _model, _start, _end, error_type, message, retries, _usage),) = calls
    assert error_type == 'provider_unavailable' and message == 'ValueError: bad request' and retries == []


def test_telemetry_failing_never_breaks_the_chat(monkeypatch):
    def broken(*_args):
        raise RuntimeError('relay down')

    monkeypatch.setattr(relay, 'model_call', broken)
    monkeypatch.setattr(relay, '_write', lambda line: None)
    assert _Driver(['hello']).chat_string('hi') == 'hello'


def test_the_tokens_the_provider_reported_go_with_the_call(calls):
    from ai.common.llm_adapter import report_llm_tokens

    class Counting(_Driver):
        def _chat(self, prompt):
            report_llm_tokens(10, 4, model='gpt-4o-2024-08-06', cache_read_tokens=6)
            return 'hello'

    Counting([]).chat_string('hi')
    ((*_rest, usage),) = calls
    assert usage == {'input': 16, 'output': 4, 'cache_read': 6, 'model': 'gpt-4o-2024-08-06'}


def test_a_stream_closed_with_an_error_after_output_is_a_failed_call(calls, monkeypatch):
    def broken_stream(self, prompt, on_chunk, on_finish, on_reasoning_chunk, emitted):
        on_chunk('partial')
        on_finish('error')
        return ''

    monkeypatch.setattr(_Driver, '_chat_string', lambda self, p, c, f, r: broken_stream(self, p, c, f, r, None))
    finished = []
    assert _Driver([]).chat_string('hi', on_chunk=lambda t: None, on_finish=finished.append) == ''
    assert finished == ['error']  # the caller still hears it
    ((*_head, error_type, message, _retries, _usage),) = calls
    assert error_type == 'provider_unavailable' and 'after output' in message


@pytest.fixture
def written(monkeypatch):
    lines = []
    monkeypatch.setattr(relay, '_write', lines.append)
    monkeypatch.setattr(relay, '_pending', [])
    monkeypatch.setattr(relay, '_overflow', 0)
    return lines


def test_the_provider_name_follows_the_genai_conventions():
    assert relay.genai_provider('llm_anthropic') == 'anthropic'
    assert relay.genai_provider('llm_bedrock') == 'aws.bedrock'
    assert relay.genai_provider('llm_ollama') == 'ollama'
    assert relay.genai_provider('llm_vision_ollama') == 'ollama'  # a vision node: its base provider
    assert relay.genai_provider('llm_vision_gemini') == 'gcp.gemini'
    assert relay.genai_provider('') == '_unknown'


def test_off_a_node_call_a_model_call_writes_nothing(written):
    relay.model_call('chat', 'openai', 'gpt-4o', 1, 2)
    assert relay._pending == []
