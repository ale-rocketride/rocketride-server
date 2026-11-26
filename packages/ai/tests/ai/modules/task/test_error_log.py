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
ai.modules.task.error_log: a run's errors on the server's output, with its trace id.

Only errors are written (tracebacks, error-level stderr lines, node errors,
exits with an error), redacted, rate-limited and de-duplicated.
"""

import json

import pytest

from ai.common import logfmt
from ai.modules.task import error_log
from ai.modules.task.error_log import TaskErrorLog


@pytest.fixture
def log(monkeypatch):
    lines = []
    monkeypatch.setattr(error_log.telemetry, 'error_log_trace', lambda task_id: 'ab' * 16)
    dropped = []
    monkeypatch.setattr(error_log.telemetry, 'count_dropped', lambda reason, count=1: dropped.append(reason))
    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    t = TaskErrorLog('task-1', lambda: 'org-a', write=lines.append)
    t.dropped = dropped
    return t, lines


def records(lines):
    return [json.loads(line) for line in lines]


def test_a_traceback_is_one_error_record_with_the_trace_id(log):
    t, lines = log
    for line in [
        'Downloading model: 100%|██████████| 10/10',  # progress on stderr: not an error
        'Traceback (most recent call last):',
        '  File "/x/node.py", line 3, in writeText',
        '    raise ValueError("bad chunk")',
        'ValueError: bad chunk',
        'INFO some library message',
    ]:
        t.stderr(line)
    (record,) = records(lines)
    assert record['level'] == 'error' and record['logger'] == 'rocketride.task'
    assert record['task_id'] == 'task-1' and record['org_id'] == 'org-a' and record['trace_id'] == 'ab' * 16
    assert record['msg'] == 'ValueError: bad chunk' and record['exc_type'] == 'ValueError'
    assert record['exc'].startswith('Traceback (most recent call last):') and 'raise ValueError' in record['exc']


def test_chained_tracebacks_stay_one_record(log):
    t, lines = log
    for line in [
        'Traceback (most recent call last):',
        '  File "a.py", line 1, in f',
        'KeyError: x',
        '',
        'During handling of the above exception, another exception occurred:',
        '',
        'Traceback (most recent call last):',
        '  File "b.py", line 2, in g',
        'RuntimeError: y',
        'INFO the next unrelated line',
    ]:
        t.stderr(line)
    # One record: the exception that propagated, with the whole chain in its traceback
    (record,) = records(lines)
    assert record['msg'] == 'RuntimeError: y' and record['exc_type'] == 'RuntimeError'
    assert 'KeyError: x' in record['exc'] and 'During handling' in record['exc']
    assert t.first['type'] == 'RuntimeError'


def test_a_traceback_then_the_engines_error_report_are_two_records(log):
    t, lines = log
    for line in ['Traceback (most recent call last):', '  File "a.py", line 1, in f', 'KeyError: x']:
        t.stderr(line)
    t.node_error('Error*node failed*a.py:1')  # the engine's report ends the traceback first
    assert [r['msg'] for r in records(lines)] == ['KeyError: x', 'node failed']


def test_error_prefixed_lines_node_errors_and_exits(log):
    t, lines = log
    t.stderr('[otel] WARNING something')  # a warning is not written
    t.stderr('ERROR: could not open the store')
    t.node_error('InvalidParam*bad lane*pipeline_config.cpp:167')
    t.node_error('Cancelled*Condition wait*Queue.hpp:179')  # a stop winding down
    t.exited(1, 'The service x was not found', 'InvalidSchema')
    t.exited(1, 'bad lane', 'InvalidParam')  # the same failure as the node error: once
    t.exited(1, 'stopping', 'Cancelled')  # a requested stop
    t.exited(0, 'ok', None)
    got = [(r['msg'], r.get('error_code')) for r in records(lines)]
    assert got == [
        ('ERROR: could not open the store', None),
        ('bad lane', 'InvalidParam'),
        ('run exited with an error: The service x was not found', 'InvalidSchema'),
    ]


def test_a_node_error_keeps_the_engine_traceback(log):
    t, lines = log
    t.node_error('Exception*boom*x.py:3*Traceback (most recent call last):*  File "x.py", line 3*ValueError: boom')
    (record,) = records(lines)
    assert record['exc'].splitlines() == [
        'Traceback (most recent call last):',
        '  File "x.py", line 3',
        'ValueError: boom',
    ]


def test_repeated_tracebacks_are_written_once(log):
    t, lines = log
    for _ in range(5):
        for line in ['Traceback (most recent call last):', '  File "a.py", line 1, in f', 'ValueError: same']:
            t.stderr(line)
        t.settle()  # the next event from the run
    assert len(lines) == 1
    assert t.dropped.count('error_log_duplicate') == 4


def test_rate_limited_per_run(log, monkeypatch):
    t, lines = log
    monkeypatch.setattr(error_log.time, 'monotonic', lambda: 1000.0)  # no refill
    t.reset()
    for n in range(error_log.RATE_PER_MINUTE + 5):
        t.stderr(f'ERROR failure {n}')
    assert len(lines) == error_log.RATE_PER_MINUTE
    assert t.dropped.count('error_log_rate_limited') == 5


def test_secrets_are_redacted(log):
    t, lines = log
    t.stderr('ERROR call to https://api.example.com/v1?key=AIzaSECRET123456&x=1 failed for bob@example.com')
    t.node_error('Exception*Authorization: Bearer abcdefghijklmnop rejected; token=sk-proj-abcdefghij*x.py:1')
    assert len(lines) == 2
    first, second = (r['msg'] for r in records(lines))
    assert 'ERROR call to https://api.example.com' in first and 'failed for' in first
    assert 'rejected' in second
    text = ' '.join(lines)
    for secret in ('AIzaSECRET123456', 'bob@example.com', 'abcdefghijklmnop', 'sk-proj-abcdefghij'):
        assert secret not in text


def test_a_traceback_cut_by_the_end_of_the_run_is_still_written(log):
    t, lines = log
    t.stderr('Traceback (most recent call last):')
    t.stderr('  File "a.py", line 1, in f')
    t.flush()
    assert len(lines) == 1


def test_text_format_keeps_the_traceback_under_the_ids(monkeypatch):
    lines = []
    monkeypatch.setattr(error_log.telemetry, 'error_log_trace', lambda task_id: 'cd' * 16)
    monkeypatch.delenv('RR_LOG_FORMAT', raising=False)
    t = TaskErrorLog('task-2', lambda: '', write=lines.append)
    for line in ['Traceback (most recent call last):', '  File "a.py", line 1, in f', 'ValueError: v']:
        t.stderr(line)
    t.settle()
    first, *rest = lines[0].split('\n')
    assert first.startswith('ERROR rocketride.task task_id=task-2 trace_id=' + 'cd' * 16)
    assert rest and all(part.startswith('    ') for part in rest)  # indented: one log entry


def test_off_unless_telemetry_or_a_log_format(monkeypatch):
    monkeypatch.delenv('RR_LOG_FORMAT', raising=False)
    monkeypatch.delenv('RR_OTEL_ENABLED', raising=False)
    assert isinstance(error_log.for_task('t', lambda: ''), error_log.Disabled)
    monkeypatch.setenv('RR_OTEL_ENABLED', 'true')
    assert isinstance(error_log.for_task('t', lambda: ''), TaskErrorLog)


def test_redact_keeps_ordinary_text():
    assert logfmt.redact('ValueError: chunk 3 of 7 is empty') == 'ValueError: chunk 3 of 7 is empty'


def test_a_star_in_the_text_keeps_it_whole(log):
    """A provider's masked key ('sk-inval****pose') must not cut the text at the first '*'."""
    t, lines = log
    t.node_error(
        'Error*writeQuestions: LLM call failed (provider_auth): Incorrect API key provided: sk-inval****pose.'
        ' pass_trace_id=' + 'ab' * 16 + '*/workspace/packages/ai/src/ai/common/llm_base.py:133'
    )
    (record,) = records(lines)
    assert record['msg'].endswith('sk-inval****pose. pass_trace_id=' + 'ab' * 16)
    assert record['location'] == '/workspace/packages/ai/src/ai/common/llm_base.py:133'
    assert 'exc' not in record


def test_each_question_failing_the_same_way_has_its_line(log):
    """The same provider error for two questions (two passes) is two lines, not a duplicate."""
    t, lines = log
    for trace in ('ab' * 16, 'cd' * 16):
        t.node_error(f'Error*LLM call failed (provider_auth): sk-****x pass_trace_id={trace}*llm_base.py:133')
    t.node_error('Error*LLM call failed (provider_auth): sk-****x pass_trace_id=' + 'cd' * 16 + '*llm_base.py:133')
    assert len(records(lines)) == 2  # the repeat within one question is still written once


def test_a_line_names_the_pipelines_project(monkeypatch):
    lines = []
    monkeypatch.setattr(error_log.telemetry, 'error_log_trace', lambda task_id: '')
    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    t = TaskErrorLog('task-3', lambda: 'org-a', write=lines.append, project_id=lambda: 'proj-123')
    t.node_error('llm_openai: LLM call failed (provider_unavailable)')
    (record,) = records(lines)
    assert record['project_id'] == 'proj-123' and record['org_id'] == 'org-a'


def test_the_runs_first_error_is_kept_for_its_trace(log):
    t, _ = log
    assert t.first is None
    t.node_error('Exception*Invalid Anthropic API key format*anthropic.py:93')
    t.exited(1, 'Invalid Anthropic API key format', 'Exception')
    assert t.first == {'message': 'Invalid Anthropic API key format', 'location': 'anthropic.py:93', 'type': ''}
    t.reset()  # a new run of the task
    assert t.first is None


def test_blank_lines_after_an_exception_do_not_grow_the_traceback(log):
    t, lines = log
    for line in ['Traceback (most recent call last):', '  File "a.py", line 1, in f', 'KeyError: x']:
        t.stderr(line)
    for _ in range(error_log.MAX_TRACEBACK_LINES + 10):
        t.stderr('')
    assert t._traceback is None or len(t._traceback) <= error_log.MAX_TRACEBACK_LINES + 1
    t.flush()
    (record,) = records(lines)
    assert record['msg'] == 'KeyError: x'
