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
Structured log lines for EaaS's own output: text (default) or JSON.

RR_LOG_FORMAT=json writes one JSON object per line; unset (or 'text') keeps a
plain line, so an existing reader of the server's output sees text as before.
Fields are always: ts, level, logger, msg, plus whatever ids the caller has
(task_id, org_id, trace_id, error_code, component, exc).

Text from customer pipelines (exception messages, URLs) is redacted of secrets
before it is written: bearer tokens, API keys, key=/token=/password= values,
e-mail addresses.
"""

import json
import logging
import os
import re
import sys
import time
from typing import Any, Mapping, Optional, TextIO

from ai.common.secret_names import canonical, is_sensitive_param

MAX_TEXT = 8192  # characters of msg/exc kept per line

# Every repetition that a pattern can restart at many positions is bounded
# ({0,40}, {1,64}...): unbounded ones made redaction quadratic in the text's length,
# and a long request line (an access log) could stall the server.
# Names whose value is a secret: the access log's list (secret_names), plus endings
# only log text needs (AccountKey, x_sig, db_credentials, pwd). 'code' and 'state'
# are OAuth's only on an OAuth line: elsewhere they are an exit code or a state.
_LOG_SUFFIXES = ('key', 'sig', 'credential', 'credentials', 'cookie', 'session', 'pwd')
_CONTEXT_ONLY = frozenset({'code', 'state'})
# One auth-param of an Authorization header (name=token or name="quoted, escapes allowed")
_AUTH_PARAM = r'[A-Za-z0-9_-]{1,64}=(?:"(?:\\.|[^"\\\r\n]){0,1024}"?|[^\s,;"\']{1,1024})'
_OAUTH_LINE = re.compile(r'(?i)oauth|callback|authori[sz]|redirect_uri|openid')
_SECRETS = [
    # A private key block first: inside a "private_key": "..." value too
    (
        re.compile(
            r'-----BEGIN [A-Z ]{0,40}PRIVATE KEY-----.*?(?:-----END [A-Z ]{0,40}PRIVATE KEY-----|(?=["\']|$))',
            re.DOTALL,
        ),
        '[REDACTED]',
    ),
    # A URL keeps its scheme, host and port only: a path or query can carry a token
    # (api.telegram.org/bot<token>/..., ?key=...), and user:password@ is dropped.
    # The port stays: it is not a secret, and tools read it from lines such as
    # Uvicorn's 'running on http://127.0.0.1:<port>' (or http://[::1]:<port>).
    (
        re.compile(
            r'\b([a-z][a-z0-9+.-]{0,31})://(?:[^@/\s]{0,256}@)?(\[[0-9a-f:.]{2,45}\]|[^/:?#\s\'"]{1,253})(:\d{1,5})?[^\s\'"<>]*',
            re.IGNORECASE,
        ),
        r'\1://\2\3',
    ),
    (re.compile(r'(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}'), r'\1 [REDACTED]'),
    # JSON web tokens, Google API keys, GitHub, Hugging Face and Pinecone tokens
    (re.compile(r'\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*'), '[REDACTED]'),
    (re.compile(r'\bAIza[A-Za-z0-9_-]{30,}'), '[REDACTED]'),
    (re.compile(r'\b(gh[pousr])_[A-Za-z0-9]{20,}'), r'\1_[REDACTED]'),
    (re.compile(r'\b(hf)_[A-Za-z0-9]{20,}'), r'\1_[REDACTED]'),
    (re.compile(r'\b(pcsk)_[A-Za-z0-9_]{20,}'), r'\1_[REDACTED]'),
    # A validation error quoting what it validated (pydantic: the prompt or the
    # document text a node sent): the value is the customer's content
    # Up to pydantic's ', input_type=' (a dict or list nests, and its strings can hold
    # any bracket; repr() keeps it on one line), else to the end of the line
    (
        re.compile(r'(input_value=)(?:[^\n]{0,8192}?(?=, input_type=)|[^\n]*)'),
        r'\1[REDACTED]',
    ),
    (re.compile(r'\b(sk|pk|rk)-[A-Za-z0-9_-]{8,}'), r'\1-[REDACTED]'),
    # Underscore keys: Stripe (sk_live_, rk_test_...), RocketRide API and run keys (rr_, pk_, tk_)
    (re.compile(r'\b(sk|pk|rk|tk|rr|whsec)_(?:(?:live|test)_)?[A-Za-z0-9]{8,}'), r'\1_[REDACTED]'),
    (re.compile(r'\b(AKIA|ASIA)[A-Z0-9]{12,}'), r'[REDACTED]'),
    (re.compile(r'\b(xox[abprs])-[A-Za-z0-9-]{8,}'), r'\1-[REDACTED]'),
    # An Authorization header, whatever its scheme or contents: a quoted value to its
    # closing quote (escapes skipped); a bare one is its scheme and its credentials,
    # a token or the scheme's name=value parameters (Digest: quoted, comma-separated)
    (
        re.compile(r'(?i)\b(authorization)(["\']?\s*[:=]\s*)(["\'])(?:\\.|(?!\3)[^\\\r\n]){0,4096}\3?'),
        r'\1\2\3[REDACTED]\3',
    ),
    (
        re.compile(
            r'(?i)\b(authorization)(["\']?\s*[:=]\s*)(?!["\'\s])[^\s"\',;}]{1,256}'
            r'(?:[ \t]{1,8}(?:' + _AUTH_PARAM + r'(?:[ \t]{0,8},[ \t]{0,8}' + _AUTH_PARAM + r'){0,32}'
            r'|[A-Za-z0-9._~+/=\[\]-]{1,4096}))?'
        ),
        r'\1\2[REDACTED]',
    ),
    # A Cookie header: every pair to the end of the line (a=1; b=secret)
    (re.compile(r'(?i)\b(cookie|set-cookie)(["\']?\s*[:=]\s*)[^\r\n"\']{1,4096}'), r'\1\2[REDACTED]'),
    # a secret's name, a space, then a long opaque value ("token abcd...", "X-Api-Key abcd...")
    (re.compile(r'(?i)\b(token|x-api-key|api[_-]?key)\s+[A-Za-z0-9._~+/=-]{20,}'), r'\1 [REDACTED]'),
    (re.compile(r'[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,24}'), '[EMAIL]'),
]


# name=value, name: value or "name": "value": the name and its separator; the value
# (quoted, spaces and escaped quotes allowed, an unterminated quote runs to the end of the line; or
# bare) is matched only after a secret's name, so the scan stays linear
_NAME = re.compile(r'(?<![A-Za-z0-9_.-])([A-Za-z][A-Za-z0-9_.-]{0,63})(["\']?[ \t]{0,8}[=:][ \t]{0,8})')
_VALUE = re.compile(r'(["\'])(?:\\.|(?!\1)[^\\\n]){1,512}(?:\1|[^\n]*)|[^\s&"\',;}]+')


def _secret_name(name: str, oauth: bool) -> bool:
    """Whether a name in log text names a secret (the shared list, the log-only endings, code/state on OAuth lines)."""
    plain = canonical(name)
    if plain in _CONTEXT_ONLY:
        return oauth
    return is_sensitive_param(name) or plain.endswith(_LOG_SUFFIXES)


def _redact_pairs(text: str, oauth: bool) -> str:

    out, pos = [], 0
    while True:
        match = _NAME.search(text, pos)
        if match is None:
            break
        name, sep = match.group(1), match.group(2)
        value = _VALUE.match(text, match.end()) if _secret_name(name, oauth) else None
        if value is None:
            out.append(text[pos : match.end()])
            pos = match.end()
            continue
        quote = value.group(1) or ''
        out.append(text[pos : match.start()])
        out.append(f'{name}{sep}{quote}[REDACTED]{quote}')
        pos = value.end()
    out.append(text[pos:])
    return ''.join(out)


def redact(text: str) -> str:
    """Text with secrets, URL paths, quoted inputs and e-mail addresses replaced, capped at MAX_TEXT characters."""
    # cut first: the patterns run on at most MAX_TEXT characters, whatever was logged
    truncated = len(text) > MAX_TEXT
    if truncated:
        text = text[:MAX_TEXT]
    # before the URL pattern cuts the path that says so (/oauth/callback)
    oauth = bool(_OAUTH_LINE.search(text))
    for pattern, replacement in _SECRETS:
        text = pattern.sub(replacement, text)
    text = _redact_pairs(text, oauth)
    return text + '… [truncated]' if truncated else text


def log_format(environ: Optional[Mapping[str, str]] = None) -> str:
    """'json' when RR_LOG_FORMAT=json, else 'text'."""
    environ = os.environ if environ is None else environ
    return 'json' if (environ.get('RR_LOG_FORMAT') or '').strip().lower() == 'json' else 'text'


def format_line(level: str, logger: str, msg: str, fmt: str = 'text', **fields: Any) -> str:
    """
    One log record as a line (no trailing newline).

    JSON: one object. Text: 'LEVEL logger key=value ... | msg', with a multi-line
    'exc' indented under it, so a log pipeline joining indented lines keeps the
    traceback in the same entry as the ids.
    """
    fields = {k: v for k, v in fields.items() if v not in (None, '')}
    if fmt == 'json':
        record = {
            'ts': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + f'.{int(time.time() * 1000) % 1000:03d}Z',
            'level': level.lower(),
            'logger': logger,
            'msg': msg,
            **fields,
        }
        return json.dumps(record, ensure_ascii=False, default=str)
    exc = fields.pop('exc', None)
    pairs = ' '.join(f'{k}={v}' for k, v in fields.items())
    line = f'{level.upper()} {logger}' + (f' {pairs}' if pairs else '') + f' | {msg}'
    if exc:
        line += '\n' + '\n'.join('    ' + part for part in str(exc).splitlines())
    return line


def write(line: str, stream: Optional[TextIO] = None) -> None:
    """Write one line to stdout (or stream), never raising."""
    try:
        out = stream or sys.stdout
        out.write(line + '\n')
        out.flush()
    except Exception:
        pass


# Context a caller passes with logging's extra=: on the line as fields (who and
# what failed), never any other attribute of the record
CONTEXT_FIELDS = (
    'event',
    'org_id',
    'task_id',
    'project_id',
    'auth_stage',
    'stripe_event_id',
    'stripe_event_type',
    'error_type',
)


class JsonFormatter(logging.Formatter):
    """A logging.Formatter writing format_line JSON: level, logger, msg, exc, context fields and the current trace id."""

    def format(self, record: logging.LogRecord) -> str:
        """One JSON line for the record."""
        exc = self.formatException(record.exc_info) if record.exc_info else None
        if record.levelno >= logging.ERROR:
            _keep_current_trace()
        context = {key: getattr(record, key) for key in CONTEXT_FIELDS if getattr(record, key, None)}
        return format_line(
            record.levelname,
            record.name,
            redact(record.getMessage()),
            'json',
            trace_id=_current_trace_id(),
            exc_type=record.exc_info[0].__name__ if record.exc_info and record.exc_info[0] else None,
            exc=redact(exc) if exc else None,
            **context,
        )


_error_handler: Optional[logging.Handler] = None


def install_error_handler(environ: Optional[Mapping[str, str]] = None) -> None:
    """
    Every ERROR any module logs, as one JSON line on stdout (RR_LOG_FORMAT=json):
    the errors-only log pipeline keeps them. Without it a logger.error outside a
    run (sign-in, billing, a store) has no handler and reaches no log store, or
    prints bare text. Once; text format keeps Python's own behaviour.
    """
    global _error_handler
    if _error_handler is not None or log_format(environ) != 'json':
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.setLevel(logging.ERROR)
    root = logging.getLogger()
    root.addHandler(handler)
    if root.level == logging.NOTSET or root.level > logging.ERROR:
        root.setLevel(logging.ERROR)
    _error_handler = handler


def _current_trace_id() -> Optional[str]:
    """The current OpenTelemetry span's trace id, when there is one (opentelemetry-api is optional)."""
    try:
        from opentelemetry import trace

        context = trace.get_current_span().get_span_context()
        return format(context.trace_id, '032x') if context.is_valid else None
    except Exception:
        return None


def _keep_current_trace() -> None:
    """The line names the current trace (trace_id): keep that trace."""
    try:
        from ai.common import telemetry

        telemetry.error_logged()
    except Exception:
        pass


def uvicorn_log_config(environ: Optional[Mapping[str, str]] = None) -> Optional[dict]:
    """
    The uvicorn logging configuration for RR_LOG_FORMAT=json, or None to keep uvicorn's own (text).

    Same loggers and levels as uvicorn's default, every line through JsonFormatter.
    """
    if log_format(environ) != 'json':
        return None
    handler = {'class': 'logging.StreamHandler', 'formatter': 'json', 'stream': 'ext://sys.stdout'}
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {'json': {'()': 'ai.common.logfmt.JsonFormatter'}},
        'handlers': {'default': dict(handler, stream='ext://sys.stderr'), 'access': handler},
        'loggers': {
            'uvicorn': {'handlers': ['default'], 'level': 'INFO', 'propagate': False},
            'uvicorn.error': {'level': 'INFO'},
            'uvicorn.access': {'handlers': ['access'], 'level': 'INFO', 'propagate': False},
        },
    }
