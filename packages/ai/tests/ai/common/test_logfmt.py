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

"""ai.common.logfmt: RR_LOG_FORMAT=json lines; unset, the server's output is unchanged."""

import pytest
import json
import logging
import sys

from ai.common import logfmt


def test_unset_keeps_uvicorns_own_configuration():
    assert logfmt.log_format({}) == 'text'
    assert logfmt.uvicorn_log_config({}) is None  # uvicorn.Config gets no log_config: as before
    assert logfmt.uvicorn_log_config({'RR_LOG_FORMAT': 'text'}) is None


def test_json_configures_every_uvicorn_logger():
    config = logfmt.uvicorn_log_config({'RR_LOG_FORMAT': 'json'})
    assert set(config['loggers']) == {'uvicorn', 'uvicorn.error', 'uvicorn.access'}
    assert config['formatters']['json']['()'] == 'ai.common.logfmt.JsonFormatter'


def test_json_formatter_writes_one_object_with_the_traceback():
    try:
        raise ValueError('boom token=supersecretvalue')
    except ValueError:
        record = logging.LogRecord('uvicorn.error', logging.ERROR, __file__, 1, 'request failed', None, sys.exc_info())
    line = logfmt.JsonFormatter().format(record)
    assert '\n' not in line
    data = json.loads(line)
    assert (data['level'], data['logger'], data['msg'], data['exc_type']) == (
        'error',
        'uvicorn.error',
        'request failed',
        'ValueError',
    )
    assert 'Traceback' in data['exc'] and 'supersecretvalue' not in data['exc']


def test_text_line_shape():
    line = logfmt.format_line('error', 'rocketride.task', 'bad', 'text', task_id='t', trace_id=None, exc='a\nb')
    assert line == 'ERROR rocketride.task task_id=t | bad\n    a\n    b'


def test_an_error_outside_a_run_is_a_json_line_with_its_context(monkeypatch, capsys):
    """install_error_handler: any module's logger.error reaches stdout as JSON, with the context passed in extra."""
    import logging

    from ai.common import logfmt

    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    monkeypatch.setattr(logfmt, '_error_handler', None)
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        logfmt.install_error_handler()
        logfmt.install_error_handler()  # once
        assert len(root.handlers) == len(before) + 1
        try:
            raise RuntimeError('ledger down')
        except RuntimeError:
            logging.getLogger('rocketride.billing').error(
                'billing: writing usage to the ledger failed',
                exc_info=True,
                extra={'org_id': 'org-a', 'error_type': 'ledger_write_failed', 'not_a_field': 'x'},
            )
        logging.getLogger('rocketride.billing').warning('not kept')  # under ERROR: no line
        (line,) = [json.loads(out) for out in capsys.readouterr().out.splitlines() if out.startswith('{')]
        assert line['level'] == 'error' and line['logger'] == 'rocketride.billing'
        assert line['org_id'] == 'org-a' and line['error_type'] == 'ledger_write_failed'
        assert line['exc_type'] == 'RuntimeError' and 'ledger down' in line['exc']
        assert 'not_a_field' not in line
    finally:
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)


def test_text_format_keeps_pythons_own_logging(monkeypatch):
    import logging

    from ai.common import logfmt

    monkeypatch.delenv('RR_LOG_FORMAT', raising=False)
    monkeypatch.setattr(logfmt, '_error_handler', None)
    before = list(logging.getLogger().handlers)
    logfmt.install_error_handler()
    assert logging.getLogger().handlers == before


def test_underscore_keys_are_redacted():
    from ai.common.logfmt import redact

    text = redact('stripe sk_live_51Habcdefghijk, run pk_3f9a8b7c6d5e, task tk_0123456789ab, api rr_abcdefghijkl')
    for secret in ('51Habcdefghijk', '3f9a8b7c6d5e', '0123456789ab', 'abcdefghijkl'):
        assert secret not in text
    assert redact('a task_id and pk_id stay') == 'a task_id and pk_id stay'


@pytest.mark.parametrize(
    'text, secret',
    [
        ('https://api.telegram.org/bot123456789:AAHabcdefghijklmnop/sendMessage failed', 'AAHabcdefghijklmnop'),
        ('Authorization: Basic dXNlcjpwYXNzd29yZA==', 'dXNlcjpwYXNzd29yZA'),
        ('token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcdef', 'eyJzdWIiOiIxIn0'),
        ('key AIzaSyA1234567890abcdefghijklmnopqrstu', 'AIzaSyA1234567890'),
        ('ghp_abcdefghijklmnopqrstuvwxyz0123', 'abcdefghijklmnopqrstuvwxyz0123'),
        ('hf_abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz'),
        ("[type=string_type, input_value='Payroll of Jane Doe 120k', input_type=str]", 'Jane Doe'),
        ('https://user:hunter22@db.example.com:5432/prod?sslmode=require', 'hunter22'),
        ("{'mistral_api_key': 'Xy7abcdefghij'}", 'Xy7abcdefghij'),
        ('MISTRAL_API_KEY=Xy7abcdefghij', 'Xy7abcdefghij'),
        ('aws_secret_access_key = wJalrXUtnFEMIK7MDENG', 'wJalrXUtnFEMIK7MDENG'),
        ('DefaultEndpointsProtocol=https;AccountName=x;AccountKey=abc123def456==;', 'abc123def456'),
        ('refresh_token=abcdef123456', 'abcdef123456'),
        ('Authorization: Token abcdef123456789', 'abcdef123456789'),
        ('-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBg\n-----END PRIVATE KEY-----', 'MIIEvQIBADANBg'),
        (
            '{"private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgk\\n-----END PRIVATE KEY-----\\n"}',
            'MIIEvQIBADANBgk',
        ),
        ("password: 'my secret pass'", 'secret pass'),
        ('Cookie: session=abc123def456', 'abc123def456'),
        ('pcsk_abcdefghij1234567890xyz', 'abcdefghij1234567890'),
        ('token abcdefghijklmnopqrstuvwxyz012', 'abcdefghijklmnopqrstuvwxyz012'),
    ],
)
def test_tokens_in_urls_headers_and_quoted_inputs_are_redacted(text, secret):
    from ai.common.logfmt import redact

    assert secret not in redact(text)


def test_a_url_keeps_its_scheme_and_host():
    from ai.common.logfmt import redact

    assert redact('POST https://api.openai.com/v1/chat/completions?x=1 503') == 'POST https://api.openai.com 503'


def test_ordinary_error_text_is_kept():
    from ai.common.logfmt import redact

    for text in ("KeyError: 'choices'", 'token count: 512 tokens', 'a task_id and pk_id stay'):
        assert redact(text) == text


@pytest.mark.parametrize(
    'text',
    ['GET /' + 'a.' * 8000 + ' HTTP/1.1', 'a.' * 16000, 'key=' * 4000, 'x@' * 8000, 'https://' * 3000],
)
def test_redaction_time_stays_linear_on_hostile_text(text):
    """A long request line (an access log line anyone can cause) must never stall the server."""
    import time

    from ai.common.logfmt import redact

    started = time.perf_counter()
    redact(text)
    assert time.perf_counter() - started < 0.5
