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
A run's errors on the server's own output, each with the run's trace id.

A task subprocess's output reaches EaaS only as DAP events (Event Monitor, run
log); its errors never reached the server's log. This writes the errors, and
only the errors, as log lines carrying task_id, org_id, project_id (the
pipeline, the same for each of its runs) and trace_id:

- a Python traceback on the subprocess's stderr (from 'Traceback (most recent
  call last):' to the exception line), as one record;
- a stderr line that starts with an error level (ERROR, CRITICAL, FATAL);
- a node error the engine reports (apaevt_status_error);
- an exit with an error (the engine's message and error code).

Anything else on stderr (progress bars, INFO/WARNING logs, pip output) is not
written. Text is redacted of secrets (ai.common.logfmt). Each run is limited
(a token bucket) and repeated tracebacks are written once; what is left out is
counted (rocketride.telemetry.dropped). Naming the run's trace keeps it.

On only when telemetry (RR_OTEL_ENABLED) or RR_LOG_FORMAT is set, so the
server's output is otherwise unchanged. DAP events are never changed.
"""

import hashlib
import os
import re
import time
from typing import Callable, Dict, List, Optional, Union

from ai.common import logfmt, telemetry

LOGGER = 'rocketride.task'
RATE_PER_MINUTE = 20  # error records per run per minute (burst as large)
MAX_TRACEBACK_LINES = 200  # a traceback longer than this is cut
MAX_REMEMBERED = 32  # distinct tracebacks remembered per run, for de-duplication

_TRACEBACK_START = 'Traceback (most recent call last):'
# The source location the engine writes after a node error's text: 'path/file.py:123'
_LOCATION = re.compile(r'^\S+\.(?:py|pyx|cpp|cc|hpp|h)(?::\d+)?$')
_ERROR_PREFIX = re.compile(r'^(?:\[[A-Za-z_-]+\]\s*)?(?i:error|critical|fatal)\b')
# Python's chained-exception separators keep the traceback going
_CHAINED = ('During handling of the above exception', 'The above exception was the direct cause')


def enabled() -> bool:
    """Whether a run's errors are written (telemetry or a log format chosen)."""
    return telemetry.is_enabled_by_environment() or bool((os.environ.get('RR_LOG_FORMAT') or '').strip())


class TaskErrorLog:
    """The error log of one run (one per TaskEngine; reset when a run starts)."""

    def __init__(
        self,
        task_id: str,
        org_id: Callable[[], str],
        write: Callable[[str], None] = logfmt.write,
        project_id: Optional[Callable[[], str]] = None,
    ):
        """
        Args:
            task_id: The run's task id.
            org_id: Returns the run's org id (read when a record is written).
            write: Where a formatted line goes (stdout).
            project_id: Returns the pipeline's project id (opaque, the same for
                every run of the pipeline: which of a customer's pipelines failed).
        """
        self._task_id = task_id
        self._org_id = org_id
        self._project_id = project_id or (lambda: '')
        self._write = write
        self._fmt = logfmt.log_format()
        self._traceback: Optional[List[str]] = None
        # An exception line was seen: the traceback ends there unless a chained
        # exception follows ('During handling…'), which the next lines tell
        self._ended = False
        # The run's first error, for its trace (the cause on the task span)
        self.first: Optional[Dict[str, str]] = None
        self._seen: List[str] = []
        self._tokens = float(RATE_PER_MINUTE)
        self._refilled = time.monotonic()

    def reset(self) -> None:
        """A new run of the task: nothing pending, a full allowance."""
        self._traceback = None
        self._ended = False
        self.first = None
        self._seen = []
        self._tokens = float(RATE_PER_MINUTE)
        self._refilled = time.monotonic()

    # ── Inputs ──────────────────────────────────────────────────────────────

    def stderr(self, line: str) -> None:
        """One line of the subprocess's stderr."""
        line = line.rstrip('\n')
        if self._traceback is not None:
            if self._ended:
                if not line.strip() or line.startswith(_CHAINED):
                    # a chained exception follows ('During handling…', then its own
                    # traceback): the same record, up to the last exception
                    self._traceback.append(line)
                    if line.startswith(_CHAINED):
                        self._ended = False
                    return
                # anything else: the traceback ended at the exception line
                self._flush_traceback()
            else:
                self._traceback.append(line)
                # The exception line: the first non-indented line after the start
                # that is not a new start; final unless a chained exception follows
                if line and not line[0].isspace() and line != _TRACEBACK_START and not line.startswith(_CHAINED):
                    self._ended = True
                elif len(self._traceback) > MAX_TRACEBACK_LINES:
                    self._flush_traceback()
                return
        if line.startswith(_TRACEBACK_START):
            self._traceback = [line]
        elif _ERROR_PREFIX.match(line):
            self._record(line)

    def node_error(self, message: str) -> None:
        """A node error the engine reported ('<code>*<text>*<location>[*<trace line>...]').

        The engine does not escape '*' in the text, and providers write it (a
        masked API key, 'sk-proj-****abcd'): the location field (a source
        'file:line') marks where the text ends, so the text is whole and two
        different errors never share a de-duplication key.
        """
        self.flush()  # a traceback that just ended is its own record, written first
        parts = (message or '').split('*')
        code = parts[0] if parts else ''
        if code == 'Cancelled':
            return  # a requested stop winding down, not an error
        end = next((i for i in range(2, len(parts)) if _LOCATION.match(parts[i])), None)
        if end is None:
            text = '*'.join(parts[1:]) if len(parts) > 1 else message
            location, rest = '', []
        else:
            text, location, rest = '*'.join(parts[1:end]), parts[end], parts[end + 1 :]
        # The engine appends a Python traceback as further '*' fields
        trace = '\n'.join(part for part in rest if part) or None
        self._record(text, exc=trace, error_code=code, location=location, key_text=text)

    def exited(self, exit_code: int, message: str, error_code: Optional[str]) -> None:
        """The subprocess exited; with an error (not a requested stop), that is an error record."""
        self.flush()
        if exit_code and error_code != 'Cancelled':
            # The same failure is usually reported as a node error first: write it once
            self._record(
                f'run exited with an error: {message}',
                error_code=error_code or str(exit_code),
                key_text=message,
            )

    def settle(self) -> None:
        """Another event arrived: a traceback that reached its exception line is complete."""
        if self._traceback is not None and self._ended:
            self._flush_traceback()

    def flush(self) -> None:
        """Write a traceback still being collected (the run ended mid-way)."""
        if self._traceback:
            self._flush_traceback()

    # ── Internals ───────────────────────────────────────────────────────────

    def _flush_traceback(self) -> None:
        lines, self._traceback, self._ended = self._traceback or [], None, False
        if not lines:
            return
        # the exception that propagated: the last non-indented line (after any chain)
        last = next((line for line in reversed(lines) if line.strip() and not line[0].isspace()), '')
        exc_type = last.split(':', 1)[0].strip() if ':' in last else last.strip()
        self._record(last.strip(), exc='\n'.join(lines), exc_type=exc_type)

    def _record(
        self, msg: str, exc: Optional[str] = None, key_text: Optional[str] = None, **fields: Optional[str]
    ) -> None:
        # Repeated errors (a node failing on every object, the exit repeating
        # the node's error) are written once
        key = hashlib.sha1((key_text or exc or msg).encode('utf-8', 'replace')).hexdigest()
        if key in self._seen:
            telemetry.count_dropped('error_log_duplicate')
            return
        if not self._take():
            telemetry.count_dropped('error_log_rate_limited')
            return
        self._seen.append(key)
        if len(self._seen) > MAX_REMEMBERED:
            self._seen.pop(0)
        if self.first is None:
            self.first = {
                'message': logfmt.redact(msg),
                'location': fields.get('location') or '',
                'type': fields.get('exc_type') or '',
            }
        line = logfmt.format_line(
            'error',
            LOGGER,
            logfmt.redact(msg),
            self._fmt,
            task_id=self._task_id,
            org_id=self._org_id() or None,
            project_id=self._project_id() or None,
            trace_id=telemetry.error_log_trace(self._task_id) or None,
            exc=logfmt.redact(exc) if exc else None,
            **fields,
        )
        self._write(line)

    def _take(self) -> bool:
        now = time.monotonic()
        self._tokens = min(float(RATE_PER_MINUTE), self._tokens + (now - self._refilled) * RATE_PER_MINUTE / 60.0)
        self._refilled = now
        if self._tokens < 1.0:
            return False
        self._tokens -= 1.0
        return True


class Disabled:
    """The error log when it is off: every input is ignored."""

    first = None

    def reset(self) -> None:
        pass

    def settle(self) -> None:
        pass

    def stderr(self, line: str) -> None:
        pass

    def node_error(self, message: str) -> None:
        pass

    def exited(self, exit_code: int, message: str, error_code: Optional[str]) -> None:
        pass

    def flush(self) -> None:
        pass


def for_task(
    task_id: str, org_id: Callable[[], str], project_id: Optional[Callable[[], str]] = None
) -> Union[TaskErrorLog, Disabled]:
    """The error log for a task: real when enabled, a no-op otherwise."""
    return TaskErrorLog(task_id, org_id, project_id=project_id) if enabled() else Disabled()
