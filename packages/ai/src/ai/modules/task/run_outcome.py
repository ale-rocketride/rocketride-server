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
The outcome of a finished run, decided in one place.

The run log records it on the run's chapter (and the Runs UI colours a failed
run by it); telemetry counts it. Both call this, so they cannot disagree.
"""

from typing import Optional

# Why a stop was requested (TaskEngine.stop_task's reason)
STOP_REASONS = (
    'user',  # a client asked (terminate, detach with terminate)
    'ttl',  # the run window elapsed
    'disconnect',  # the launching client went away (auto-terminate)
    'unregister',  # the task was removed from the server
    'start_failed',  # cleanup after a start that failed
)


def classify_run_outcome(stop_requested: bool, stop_reason: Optional[str], exit_code: Optional[int]) -> str:
    """
    Classify a run that ended (a restart is not an end and is never classified).

    Args:
        stop_requested: A stop was asked for.
        stop_reason: Why (STOP_REASONS); None reads as 'user'.
        exit_code: The subprocess exit code; None when it never started.

    Returns:
        'ok' for a clean exit or an elapsed run window (the run stayed up exactly
        as configured); 'error' for an exit nobody asked for that was not clean,
        or the cleanup of a failed start; 'cancelled' for any other requested stop.
    """
    if stop_requested:
        if stop_reason == 'ttl':
            return 'ok'
        if stop_reason == 'start_failed':
            return 'error'
        return 'cancelled'
    return 'ok' if exit_code == 0 else 'error'


# Engine error codes that mean "this pipeline or its configuration is invalid":
# the user's to fix, never the platform's (engine-lib/apLib/error/Ec.h). Raised by
# the engine's own validation (InvalidSchema: a node that does not exist,
# InvalidParam: e.g. an unknown lane) and by nodes raising ConfigError (rocketlib).
VALIDATION_ERROR_CODES = frozenset(
    {'InvalidSchema', 'InvalidParam', 'InvalidName', 'InvalidSelection', 'InvalidSyntax', 'InvalidFormat'}
)


def classify_error_type(
    stop_reason: Optional[str],
    exit_code: Optional[int],
    reached_running: bool = True,
    stop_interrupted: bool = False,
    error_code: Optional[str] = None,
) -> str:
    """
    The kind of an 'error' outcome, a small fixed set (telemetry's error.type).

    Args:
        stop_reason: Why a stop was requested, if one was.
        exit_code: The subprocess exit code; None when it never started.
        reached_running: The pipeline came up (RUNNING) before the end.
        stop_interrupted: A requested stop ended a process that was still
            running (the exit code is then the stop's, not the pipeline's).
        error_code: The engine error code the subprocess exited with
            (its >EXIT line, e.g. 'InvalidSchema', 'Exception'), if any.

    Returns:
        Before the pipeline came up (RUNNING):
          'start_rejected': the engine refused the pipeline with a validation
          code (VALIDATION_ERROR_CODES): the user's configuration.
          'start_error': it exited on its own with any other error (an
          exception in a node's setup, a dependency, a provider, or no code):
          not attributable yet; telemetry keeps it out of the error budget
          and alerts when it spreads across customers.
          'start_failed': no process, a killed one, a clean exit before
          RUNNING, or a start our stop interrupted (a timeout): the platform.
        After: 'signal' when the process was killed (negative exit code: OOM
        killer, SIGKILL, crash), 'exit_code' for any other non-zero exit.
    """
    if stop_reason == 'start_failed' or exit_code is None or not reached_running:
        if exit_code is not None and exit_code > 0 and not reached_running and not stop_interrupted:
            return 'start_rejected' if error_code in VALIDATION_ERROR_CODES else 'start_error'
        return 'start_failed'
    if exit_code < 0:
        return 'signal'
    return 'exit_code'
