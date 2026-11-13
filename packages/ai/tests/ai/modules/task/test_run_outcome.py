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
Unit tests for classify_run_outcome and classify_error_type, the single source
of a run's outcome.

The run log and telemetry both record it. Before it existed, the run log tested
the CANCELLED state first, so an abnormal exit (also CANCELLED) read as
'cancelled' and 'error' was never recorded.
"""

import pytest

from ai.modules.task.run_outcome import VALIDATION_ERROR_CODES, classify_error_type, classify_run_outcome


@pytest.mark.parametrize(
    'stop_requested, stop_reason, exit_code, expected',
    [
        # Nobody asked it to stop
        (False, None, 0, 'ok'),
        (False, None, 1, 'error'),
        (False, None, -9, 'error'),
        # Never started (no exit code) and nobody stopped it: a failed start
        (False, None, None, 'error'),
        # A requested stop: the exit code is the stop's, not a failure
        (True, 'user', 0, 'cancelled'),
        (True, 'user', -15, 'cancelled'),
        (True, None, -15, 'cancelled'),
        (True, 'disconnect', -15, 'cancelled'),
        (True, 'unregister', 0, 'cancelled'),
        # The run window elapsed: success
        (True, 'ttl', 0, 'ok'),
        (True, 'ttl', -15, 'ok'),
        # The cleanup of a failed start is a failure, whatever the exit code
        (True, 'start_failed', 0, 'error'),
        (True, 'start_failed', None, 'error'),
    ],
)
def test_classify_run_outcome(stop_requested, stop_reason, exit_code, expected):
    assert classify_run_outcome(stop_requested, stop_reason, exit_code) == expected


@pytest.mark.parametrize(
    'stop_reason, exit_code, expected',
    [
        (None, None, 'start_failed'),
        ('start_failed', 0, 'start_failed'),
        (None, -9, 'signal'),
        (None, -11, 'signal'),
        (None, 1, 'exit_code'),
        (None, 137, 'exit_code'),
    ],
)
def test_classify_error_type(stop_reason, exit_code, expected):
    assert classify_error_type(stop_reason, exit_code) == expected


@pytest.mark.parametrize('code', sorted(VALIDATION_ERROR_CODES))
@pytest.mark.parametrize('stop_reason', [None, 'start_failed'])
def test_a_pipeline_the_engine_refuses_is_rejected(code, stop_reason):
    # e.g. a node that does not exist (InvalidSchema), an unknown lane (InvalidParam):
    # the subprocess refuses the pipeline and exits before it comes up; the failed
    # start's cleanup (stop_reason) finds it already gone
    assert classify_error_type(stop_reason, 1, reached_running=False, error_code=code) == 'start_rejected'


@pytest.mark.parametrize('code', ['Exception', 'Python', 'Failed', 'InvalidJson', 'NotFound', 'FactoryNotFound', None])
def test_any_other_exit_before_running_is_unattributed(code):
    # an exception in a node's setup, a dependency, a provider, an old engine
    # without a code: not the user's by default, not counted as the platform's yet
    assert classify_error_type(None, 1, reached_running=False, error_code=code) == 'start_error'


@pytest.mark.parametrize(
    'stop_reason, exit_code, stop_interrupted, code',
    [
        (None, -9, False, None),  # killed while starting (OOM killer)
        (None, 0, False, None),  # exited cleanly but never came up
        ('start_failed', 0, True, None),  # start timed out: the cleanup stopped it
        ('start_failed', 1, True, 'InvalidParam'),  # same: the stop decided the exit, not the code
        ('start_failed', None, False, None),  # no process at all
    ],
)
def test_other_failures_before_running_are_failed_starts(stop_reason, exit_code, stop_interrupted, code):
    assert (
        classify_error_type(
            stop_reason, exit_code, reached_running=False, stop_interrupted=stop_interrupted, error_code=code
        )
        == 'start_failed'
    )


def test_after_running_the_code_does_not_matter():
    assert classify_error_type(None, 1, reached_running=True, error_code='InvalidParam') == 'exit_code'
    assert classify_error_type(None, -9, reached_running=True, error_code='Exception') == 'signal'
