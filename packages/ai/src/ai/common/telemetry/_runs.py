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

"""Pipeline runs and their passes: task and pipeline traces, the run and turn metrics, the stall and idle lines.

A part of the telemetry sink: mixed into _otel.Telemetry, whose state it uses.
"""

import contextlib
import fnmatch
import json
import queue
import sys
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from opentelemetry import trace
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import Link, SpanContext, SpanKind, Status, StatusCode, TraceFlags


from ._common import (
    STATE_NAMES,
    _STATE_RUNNING,
    _LIVE_STATES,
    _TOKEN_COUNTERS,
    _NODE_PATH,
    _SOURCE_FILE,
    _ERROR_CODE,
    MAX_LIVE_TASKS,
    MAX_OPEN_INVOCATIONS,
    MAX_STACK_DEPTH,
    MAX_COMPONENT_TYPES,
    MAX_ATTRIBUTE_SETS,
    NO_SOURCE,
    MAX_CAUSE_LENGTH,
    TURN_CAUSES,
    TURN_CAUSE_ORDER,
    MAX_BUFFERED_CALLS,
    MAX_VALUE_LENGTH,
    _GENAI_USAGE,
    MAX_PASS_LINKS,
    RAISED_MATCH_SECONDS,
    IDLE_KEY_SECONDS,
    MATCH_TOLERANCE,
    ORG_KEY,
    _ns,
    _preset_ids,
    _new_ids,
    _Invocation,
    _exception_attributes,
    _carried,
    _Run,
    _cut_cause,
    _raised_attributes,
    _exemplar,
    _call_parent,
    _TRACE_ID,
    _other_pass,
)


class _RunsTelemetry:
    """Pipeline runs and their passes (mixed into _otel.Telemetry)."""

    def task_event(self, task_id: str, message: Dict[str, Any], identity: Callable[[], Mapping[str, Any]]) -> None:
        event = message.get('event')
        body = message.get('body')
        if not isinstance(body, dict):
            return
        if event == 'apaevt_status_update':
            self._status(task_id, body, identity())
        elif event == 'apaevt_task':
            if body.get('action') == 'begin':
                self._begin(task_id, identity())
        elif event == 'apaevt_status_metrics':
            self._llm_tokens(task_id, body.get('metrics'))
        elif event in ('apaevt_status_error', 'apaevt_status_warning'):
            self._node_report(task_id, body.get('message'), 'error' if event == 'apaevt_status_error' else 'warning')

    def component_trace(
        self,
        task_id: str,
        op: str,
        pipe_index: Any,
        component_id: str,
        event_time: Optional[float],
        trace_body: Any,
        pipeline: Optional[Dict[str, Any]],
    ) -> None:
        if op not in ('enter', 'leave') or not component_id:
            return
        wall = event_time or time.time()
        mono = time.monotonic()
        finished: Optional[Tuple[_Invocation, _Run]] = None
        reused = False
        record: Optional[Tuple[float, str, Optional[str]]] = None
        stale: Optional[Tuple[_Invocation, _Run]] = None
        stale_reason = ''
        over_budget = False

        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            if not run.source_type and pipeline:
                run.source_type = self._source_type_locked(task_id, pipeline)
            invocation = run.invocations.get(pipe_index)
            if op == 'enter' and invocation is not None and invocation.stack:
                if self._stale_call and mono - invocation.stack[0][2] >= self._stale_call:
                    stale_reason = 'stale_call'
                elif invocation.engine_tid and _other_pass(trace_body, invocation.trace_id):
                    # The engine began a new pass while one of this pass's calls
                    # is still open: an exception skipped its leave (the engine
                    # ended the level without an event)
                    stale_reason = 'pass_unwound'
                if stale_reason:
                    # Its leave never came: close it, and this enter starts a new pass
                    del run.invocations[pipe_index]
                    self._buffered -= len(invocation.components) + len(invocation.calls)
                    stale, invocation = (invocation, run), None
            if op == 'enter':
                if invocation is None and len(run.invocations) >= MAX_OPEN_INVOCATIONS:
                    over_budget = True
                else:
                    if invocation is None:
                        invocation = run.invocations[pipe_index] = _Invocation(wall, _new_ids(self._random))
                    reused = self._adopt_pass_id(run, pipe_index, invocation, trace_body)
                    if len(invocation.stack) >= MAX_STACK_DEPTH:
                        # A lost leave (killed process) must not grow the stack forever
                        invocation.stack.pop(0)
                    invocation.stack.append((component_id, wall, mono))
                    invocation.last_mono = mono
                    invocation.stall_logged = False  # progress: a later stall is logged again
                    if run.types is None:
                        # Known from the first enter: a call that never leaves is still named
                        self._component_type(run, component_id, pipeline)
            elif invocation is not None:
                invocation.last_mono = mono
                invocation.stall_logged = False
                # leave: pair by identity (reentrant calls interleave on one pipe)
                record, finished, over_budget = self._leave(
                    run, invocation, pipe_index, component_id, wall, mono, trace_body, pipeline
                )

        if reused:
            self.count_dropped('pass_tid_reused')
        if stale:
            self.count_dropped(stale_reason)
            self._emit_invocation(task_id, *stale, end=wall, unclosed=True, why=stale_reason)
        if op == 'enter':
            if over_budget:
                self.count_dropped('invocation_cap')
            return
        if record is None:
            return
        duration, component_type, result = record
        attributes = {'rocketride.component.type': component_type}
        if result:
            attributes['rocketride.component.result'] = result
        self._component.record(duration, attributes)
        if over_budget:
            self.count_dropped('span_budget')
        if finished:
            self._emit_invocation(task_id, *finished, end=wall)

    def _adopt_pass_id(self, run: _Run, pipe_index: Any, invocation: _Invocation, trace_body: Any) -> bool:
        """
        Make the engine's pass id ('tid' on an enter: the one its nodes' model
        server requests carry) the pass's trace id. Under the lock.

        A pass that began without one (an engine without it, a tool call's
        enter) takes the first it sees. A tid this pipe already had is not
        trusted: the engine's pass boundary drifted (a lost leave), and two
        passes must not share a trace. True when that happened (counted by the
        caller, outside the lock).
        """
        if invocation.engine_tid or invocation.tid_rejected:
            return False
        tid = trace_body.get('tid') if isinstance(trace_body, dict) else None
        if not (isinstance(tid, str) and _TRACE_ID.match(tid)) or not int(tid, 16):
            return False
        trace_id = int(tid, 16)
        if run.pass_tids.get(pipe_index) == trace_id:
            invocation.tid_rejected = True  # its other enters carry it too: one count for the pass
            return True
        run.pass_tids[pipe_index] = trace_id
        invocation.trace_id, invocation.engine_tid = trace_id, True
        return False

    def _leave(self, run, invocation, pipe_index, component_id, wall, mono, trace_body, pipeline):
        """A component's leave: (duration record, finished invocation, over budget). Under the lock."""
        for index in range(len(invocation.stack) - 1, -1, -1):
            if invocation.stack[index][0] == component_id:
                _, start_wall, start_mono = invocation.stack.pop(index)
                break
        else:
            return None, None, False
        component_type = self._component_type(run, component_id, pipeline)
        result = trace_body.get('result') if isinstance(trace_body, dict) else None
        result = result if result in ('continue', 'skip', 'error') else None
        if result == 'error':
            invocation.error = True
        if len(invocation.left) < 2 and str(component_id) not in invocation.left:
            invocation.left.append(str(component_id))
        over_budget = False
        if len(invocation.components) >= self._span_cap:
            invocation.dropped += 1
        elif self._buffered >= MAX_BUFFERED_CALLS:
            invocation.dropped += 1
            over_budget = True
        else:
            invocation.components.append((component_type, start_wall, max(wall, start_wall), result, str(component_id)))
            self._buffered += 1
        finished = None
        if not invocation.stack:
            del run.invocations[pipe_index]
            self._buffered -= len(invocation.components) + len(invocation.calls)
            finished = (invocation, run)
        return (max(0.0, mono - start_mono), component_type, result), finished, over_budget

    def task_restarted(self, task_id: str) -> None:
        """Same run, new subprocess: its pipeline may differ, open calls are gone, counts restart at 0."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            self._release(run)
            open_passes = list(run.invocations.values())
            run.invocations.clear()
        # A pass open across the restart ended with the old process, as at a
        # run's end: its turn (abandoned, unless it had failed) and its model calls count
        now = time.time()
        for invocation in open_passes:
            self._emit_invocation(task_id, invocation, run, end=now, unclosed=True, why='task_end')
        with self._lock:
            # The new process may run another pipeline: its source and raised errors are not the old one's
            run.types = None
            run.source_type = ''
            run.raised = None
            run.completed = run.failed = 0
            run.tokens = {}

    def task_ended(
        self,
        task_id: str,
        identity: Mapping[str, Any],
        outcome: str,
        stop_reason: Optional[str],
        error_type: Optional[str],
        error_code: Optional[str] = None,
        cause: Optional[Mapping[str, str]] = None,
        exit_cause: Optional[Mapping[str, Any]] = None,
    ) -> None:
        mono = time.monotonic()
        now = time.time()
        with self._lock:
            run = self._runs.pop(task_id, None)
            if run is not None:
                self._release(run)
            self._ended_ids[task_id] = None
            if len(self._ended_ids) > MAX_LIVE_TASKS:
                self._ended_ids.popitem(last=False)
        run_kind = (run.run_kind if run else None) or identity.get('run_kind') or 'dev'
        org_id = (run.org if run else None) or identity.get('org_id')
        project = run.project if run is not None else str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH]
        stop = stop_reason or 'none'
        error_type = error_type if outcome == 'error' else None

        ended = self._ended_attributes(run_kind, outcome, stop, error_type, org_id, project)
        exit_signal = str((exit_cause or {}).get('signal') or '')[:16]
        oom_suspected = bool((exit_cause or {}).get('oom_suspected'))
        if error_type and error_code:
            # why a start failed or a run ended in error (the engine's codes: a fixed set), bounded
            ended['rocketride.engine.error_code'] = self._bounded_error_type(
                str(error_code) if _ERROR_CODE.match(str(error_code)) else '_OTHER', 'engine'
            )
        if exit_signal:
            ended['rocketride.task.exit_signal'] = self._bounded_error_type(exit_signal, 'signal')
            ended['rocketride.task.oom_suspected'] = oom_suspected
        self._ended.add(1, self._bounded('rocketride.task.ended', ended))
        if run is None or run.begin_mono is None:
            return

        duration = max(0.0, mono - run.begin_mono)

        # Invocations still open died with the run
        for invocation in list(run.invocations.values()):
            invocation.error = invocation.error or outcome == 'error'
            self._emit_invocation(task_id, invocation, run, end=now, unclosed=True, why='task_end')

        # The run's trace, with the ids it was given when it began: always for a
        # run that ended in error or that an error log named, otherwise at the
        # sample ratio (0: errors only)
        trace_id = ''
        reason = 'error' if error_type else run.keep_reason
        if not reason and self._slow_run and run_kind != 'deploy' and duration >= self._slow_run:
            reason = 'slow' if self._slow_allowed(task_id, str(org_id or '')) else ''
        if not reason and self._keep_success():
            reason = 'ratio'
        # Measured whatever the sampling; the exemplar names the run's trace when it is kept
        self._duration.record(
            duration,
            self._bounded(
                'rocketride.task.duration',
                self._with_org(
                    {'rocketride.task.run_kind': run_kind, 'rocketride.task.outcome': outcome}, org_id, project
                ),
            ),
            context=_exemplar(run.trace_id, run.span_id, bool(reason)),
        )
        if reason:
            span = self._start_task_span(
                task_id,
                run,
                org_id,
                {
                    'rocketride.task.run_kind': run_kind,
                    'rocketride.task.outcome': outcome,
                    'rocketride.task.stop_reason': stop,
                    'rocketride.sampling.reason': reason,
                },
            )
            if error_type:
                span.set_attribute('error.type', error_type)
                if error_code:
                    span.set_attribute('rocketride.engine.error_code', str(error_code)[:MAX_VALUE_LENGTH])
                if exit_signal:
                    span.set_attribute('rocketride.task.exit_signal', exit_signal)
                    span.set_attribute('rocketride.task.oom_suspected', oom_suspected)
                status = f'run ended in error ({error_type})'
                if exit_signal:
                    status += f' by {exit_signal}' + (' (out of memory suspected)' if oom_suspected else '')
                message = str((cause or {}).get('message') or '')[:MAX_CAUSE_LENGTH]
                if message:
                    # What failed, on the trace itself: the run's first error
                    status += f': {message}'
                    span.add_event('exception', _exception_attributes(cause, message))
                span.set_status(Status(StatusCode.ERROR, status))
            span.end(end_time=_ns(max(now, run.begin_wall)))
            trace_id = format(span.get_span_context().trace_id, '032x')

        # One structured line per run: the link between logs, traces and metrics
        try:
            line = {
                # A standard level field, so log pipelines can keep errors only
                'level': 'error' if outcome == 'error' else 'info',
                'event': 'rocketride.task.ended',
                'task_id': task_id,
                'trace_id': trace_id,
                'outcome': outcome,
                'stop_reason': stop,
                'error_type': error_type,
                'error_code': error_code if error_type else None,
                'run_kind': run_kind,
                'duration_s': round(duration, 3),
            }
            if project:
                line['project_id'] = project  # the pipeline the customer names
            if exit_signal:
                line['exit_signal'] = exit_signal
                line['oom_suspected'] = oom_suspected
            org = self._span_org(org_id)
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('run_ended_log')

    def _start_task_span(self, task_id: str, run: '_Run', org_id: Any, attributes: Dict[str, Any]) -> Any:
        """The run's 'task' span, with the ids it was given when it began and links to its kept passes."""
        with _preset_ids(run.trace_id, run.span_id):
            return self._tracer.start_span(
                'task',
                kind=SpanKind.INTERNAL,
                start_time=_ns(run.begin_wall),
                attributes=self._with_span_org(
                    {
                        'rocketride.task.id': task_id,
                        **attributes,
                        **({'rocketride.project.id': run.project} if run.project else {}),
                    },
                    org_id,
                ),
                # its failed and slow passes, each a trace of its own
                links=[
                    Link(
                        SpanContext(tid, sid, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED)),
                        {'rocketride.sampling.reason': why},
                    )
                    for tid, sid, why in run.pass_links
                ]
                or None,
            )

    def error_log_trace(self, task_id: str) -> str:
        """The run's trace id for an error log line, and keep that trace (the log must lead to it)."""
        with self._lock:
            run = self._runs.get(task_id)
            if run is None or not run.trace_id:
                return ''
            if not run.keep_reason:
                run.keep_reason = 'error_log'
            return format(run.trace_id, '032x')

    def runs_lost(self, identities: List[Mapping[str, Any]]) -> None:
        """Runs that were in progress when a previous EaaS process died."""
        for identity in identities:
            self._ended.add(
                1,
                self._ended_attributes(
                    identity.get('run_kind') or 'dev',
                    'error',
                    'none',
                    'engine_lost',
                    identity.get('org_id'),
                    str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH],
                ),
            )

    def _begin(self, task_id: str, identity: Mapping[str, Any]) -> None:
        mono = time.monotonic()
        wall = time.time()
        run_kind = identity.get('run_kind') or 'dev'
        with self._lock:
            self._ended_ids.pop(task_id, None)  # a new run of the same task
            run = self._runs.get(task_id)
            if run is not None and run.begin_mono is not None:
                return  # a duplicate begin: the run is already counted
            if run is None:
                run = self._admit(task_id)
            else:
                self._runs.move_to_end(task_id)
            run.begin_wall, run.begin_mono = wall, mono
            run.run_kind = run_kind
            run.org = str(identity.get('org_id') or '')
            run.project = str(identity.get('project_id') or '')[:MAX_VALUE_LENGTH]
        self._count_evictions()
        self._started.add(1, self._with_org({'rocketride.task.run_kind': run_kind}, identity.get('org_id')))

    def _admit(self, task_id: str) -> _Run:
        """Track a run; past the cap the least recently active run is forgotten (never a refusal). Under the lock."""
        if len(self._runs) >= MAX_LIVE_TASKS:
            _, evicted = self._runs.popitem(last=False)
            self._release(evicted)
            self._evicted += 1  # counted by _count_evictions, outside the lock
        run = self._runs[task_id] = _Run()
        run.trace_id, run.span_id = _new_ids(self._random)
        return run

    def _count_evictions(self) -> None:
        with self._lock:
            evicted, self._evicted = self._evicted, 0
        if evicted:
            self.count_dropped('run_evicted', evicted)

    def _release(self, run: _Run) -> None:
        """Give back the buffered-call budget of a run's open invocations. Under the lock."""
        for invocation in run.invocations.values():
            self._buffered -= len(invocation.components) + len(invocation.calls)

    def _status(self, task_id: str, body: Dict[str, Any], identity: Mapping[str, Any]) -> None:
        state = body.get('state')
        completed = body.get('completedCount') or 0
        failed = body.get('failedCount') or 0
        startup = None
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                if task_id in self._ended_ids:
                    return  # late status of a run that ended: already counted
                run = self._admit(task_id)  # status before begin (starting)
                run.run_kind = identity.get('run_kind') or 'dev'
                run.org = str(identity.get('org_id') or '')
            else:
                # Least recently active first out: a live run reports status
                self._runs.move_to_end(task_id)
            if 'debuggerAttached' in body:
                run.debugger = bool(body.get('debuggerAttached'))
            if isinstance(state, int):
                run.state = state
                if state == _STATE_RUNNING and not run.startup_recorded and run.begin_mono is not None:
                    run.startup_recorded = True
                    startup = max(0.0, time.monotonic() - run.begin_mono)
            # Counts restart at zero after a restart: a decrease is a new baseline
            done = completed - run.completed if completed >= run.completed else completed
            lost = failed - run.failed if failed >= run.failed else failed
            run.completed, run.failed = completed, failed
            run_kind, org = run.run_kind, run.org

        self._count_evictions()
        if startup is not None:
            self._startup.record(startup, {'rocketride.task.run_kind': run_kind})
        if done > 0:
            self._objects.add(done, self._with_org({'rocketride.object.outcome': 'completed'}, org))
        if lost > 0:
            self._objects.add(lost, self._with_org({'rocketride.object.outcome': 'failed'}, org))

    def _llm_tokens(self, task_id: str, metrics: Any) -> None:
        """Token deltas from the task's cumulative >MET* snapshot (every LLM node reports it)."""
        counters = metrics.get('counters') if isinstance(metrics, dict) else None
        if not isinstance(counters, dict):
            return
        deltas: List[Tuple[str, int]] = []
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            for name, token_type in _TOKEN_COUNTERS.items():
                value = counters.get(name)
                if not isinstance(value, (int, float)):
                    continue
                value = int(value)
                previous = run.tokens.get(name, 0)
                delta = value - previous if value >= previous else value
                run.tokens[name] = value
                if delta > 0:
                    deltas.append((token_type, delta))
            org = run.org
        for token_type, delta in deltas:
            self._tokens.add(delta, self._with_org({'gen_ai.token.type': token_type}, org))

    def _node_report(self, task_id: str, message: Any, severity: str) -> None:
        """A node's error or warning: '<code>*<text>*<location>...', attributed to the node type.

        'Cancelled' is a requested stop winding down (every terminate emits a burst), not an error.
        """
        if not isinstance(message, str):
            return
        code = message.split('*', 1)[0]
        if code == 'Cancelled':
            return
        with self._lock:
            run = self._runs.get(task_id)
            if run is None:
                return
            known = set((run.types or {}).values())
            org, project = run.org, run.project
            # The node whose handler just raised (relay._raised): named, not guessed from the text
            raised = run.raised
            if severity == 'error' and raised is not None:
                run.raised = None
                if time.monotonic() - raised[4] <= RAISED_MATCH_SECONDS:
                    raised[5] = code if _ERROR_CODE.match(code) else '_OTHER'
                else:
                    raised = None
            else:
                raised = None
        error_type = None
        if severity == 'error':
            error_type = self._bounded_error_type(code if _ERROR_CODE.match(code) else '_OTHER', 'node')
        match = _NODE_PATH.search(message)
        node = match.group(1) if match else None
        if raised is not None:
            component_type = raised[0]
        elif node is None:
            # A C++ node's own source file (parse.cpp → parse), only if the pipeline has it
            node = next((stem for stem in _SOURCE_FILE.findall(message) if stem in known), None)
            component_type = self._bounded_type(node) if node else '_engine'
        elif not known or node in known:
            component_type = self._bounded_type(node)
        else:
            component_type = '_other'
        attributes = {'rocketride.component.type': component_type, 'rocketride.component.severity': severity}
        if error_type:
            # error.type on errors only; a warning is not an error (semconv)
            attributes['error.type'] = error_type
        else:
            # a warning's code (the engine's Ec name), bounded as error types are: the
            # logs keep errors only, so without it a warning would be a bare count
            attributes['rocketride.component.code'] = self._bounded_error_type(
                code if _ERROR_CODE.match(code) else '_OTHER', 'warning'
            )
        self._component_errors.add(
            1, self._bounded('rocketride.component.errors', self._with_org(attributes, org, project))
        )

    def _bounded_type(self, component_type: str) -> str:
        with self._lock:
            return self._bounded_type_locked(component_type)

    def _bounded_type_locked(self, component_type: str) -> str:
        if component_type in self._component_types:
            return component_type
        if len(self._component_types) >= MAX_COMPONENT_TYPES:
            return '_other'
        self._component_types.add(component_type)
        return component_type

    def _component_type(self, run: _Run, component_id: str, pipeline: Optional[Dict[str, Any]]) -> str:
        """The component's provider (its node type), bounded; never the user-chosen id. Under the lock."""
        if run.types is None:
            run.types = {}
            for component in (pipeline or {}).get('components') or []:
                if isinstance(component, dict) and component.get('id'):
                    run.types[str(component['id'])] = str(component.get('provider') or '_unknown')[:MAX_VALUE_LENGTH]
        provider = run.types.get(component_id)
        return self._bounded_type_locked(provider) if provider else '_unknown'

    def _turn_outcome(self, invocation: _Invocation, why: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        (outcome, cause, error type) of a pass that ended. why: '' when it finished,
        'task_end' when its run ended with it open, 'shutdown', or the reason a
        stuck or unwound pass was closed.
        """
        if invocation.marks:
            kinds = [mark[2] for mark in invocation.marks]
            causes = [TURN_CAUSES.get(kind, 'platform') for kind in kinds]
            cause = next(c for c in TURN_CAUSE_ORDER if c in causes)
            return 'error', cause, kinds[causes.index(cause)]
        if invocation.error:
            return 'error', 'platform', 'component_error' if not why else why
        if why == 'task_end':
            return 'abandoned', None, None  # the user stopped the run, or it ended, with the pass open
        if why:
            return 'error', 'platform', why  # stuck, unwound, or the engine shutting down
        return 'ok', None, None

    def _turn_server(self, invocation: _Invocation, run: _Run, failed: bool) -> Optional[str]:
        """
        The LLM server a pass called (an LLM node's or an agent's call): the one
        whose call failed for a failed pass, else the first. Bounded as the
        dependency metrics' servers.
        """
        # HTTP calls of LLM nodes and agents: not the model server's DAP calls (ours,
        # with alerts of their own: a customer's model it is not)
        calls = [
            c
            for c in invocation.calls
            if c[0].startswith(('llm_', 'agent_')) and (c[4] or {}).get('rpc.system.name') != 'rocketride_dap'
        ]
        if failed:
            calls = [c for c in calls if c[5]] or calls
        if not calls:
            return None
        server = (calls[0][4] or {}).get('server.address')
        if not server:
            return None
        return self._bounded_pipeline_server(self._capped_org(run.org) or '', str(server)[:MAX_VALUE_LENGTH])

    def _source_type_locked(self, task_id: str, pipeline: Mapping[str, Any]) -> str:
        """The run's source component type (its provider), bounded like component types; under the lock."""
        source = task_id.partition('.')[2]
        for component in pipeline.get('components') or []:
            if isinstance(component, dict) and component.get('id') == source and component.get('provider'):
                return self._bounded_type_locked(str(component['provider'])[:MAX_VALUE_LENGTH])
        return NO_SOURCE

    def _count_turn(
        self, invocation: _Invocation, run: _Run, why: str, end: Optional[float] = None, kept: bool = False
    ) -> None:
        outcome, cause, error_type = self._turn_outcome(invocation, why)
        attributes = {'rocketride.turn.outcome': outcome, 'rocketride.task.run_kind': run.run_kind}
        if cause:
            attributes['rocketride.turn.cause'] = cause
            attributes['error.type'] = self._bounded_error_type(error_type, 'node')
        # The LLM provider a question used: whether a provider is down is the
        # share of its questions failing (a failed question makes many calls)
        server = self._turn_server(invocation, run, outcome == 'error')
        if server:
            attributes['server.address'] = server
        model = self._turn_model(invocation, outcome == 'error')
        attributes.update(model)
        # Whether it carried a question or a document (data through two components or
        # more), not only a component's own open or close step: the turns SLO counts these
        carried = _carried(invocation)
        attributes['rocketride.turn.carried'] = 'true' if carried else 'false'
        exemplar = _exemplar(invocation.trace_id, invocation.span_id, kept)
        self._turns.add(
            1, self._bounded('rocketride.turn', self._with_org(attributes, run.org, run.project)), context=exemplar
        )
        # Timed: a pass that carried a question or a document (answered, failed, or
        # stopped by the user while open: how long they waited before giving up)
        if end is not None and outcome in ('ok', 'error', 'abandoned') and carried:
            timed = {'rocketride.turn.outcome': outcome, 'rocketride.task.run_kind': run.run_kind}
            if server:
                timed['server.address'] = server
            timed.update(model)
            # the answer time a chat user waits is not a document's: the latency SLO is per source type
            if run.source_type and run.source_type != NO_SOURCE:
                timed['rocketride.source.type'] = run.source_type
            self._turn_duration.record(
                max(0.0, end - invocation.start), self._bounded('rocketride.turn.duration', timed), context=exemplar
            )
        for _type, start, finish, gen_attributes, gen_error, _m, _r, usage, _a, span_id in invocation.models:
            self._measure_model_call(
                gen_attributes,
                (finish - start) / 1e9,
                gen_error,
                usage,
                _exemplar(invocation.trace_id, span_id, kept),
            )

    def _turn_model(self, invocation: _Invocation, failed: bool) -> Dict[str, str]:
        """The provider and model a pass asked (the failed call's for a failed pass, else the last), bounded."""
        models = invocation.models
        if failed:
            models = [m for m in models if m[4]] or models
        if not models:
            return {}
        attributes = models[-1][3]
        provider = attributes.get('gen_ai.provider.name') or '_unknown'
        return {
            'gen_ai.provider.name': provider,
            'gen_ai.request.model': self._bounded_model(provider, attributes.get('gen_ai.request.model') or ''),
        }

    def _emit_invocation(
        self, task_id: str, invocation: _Invocation, run: _Run, end: float, unclosed=False, why: str = ''
    ) -> None:
        """Decide whether the pass's trace is kept (failed and slow ones always), count its outcome and queue it."""
        duration = max(0.0, end - invocation.start)
        dropped = ''
        if invocation.error:
            reason = 'error'
        elif invocation.stall_noted:
            reason = 'stalled'  # a stall error line named this trace: it must resolve
        elif self._slow and duration >= self._slow and self._slow_allowed(task_id, run.org):
            reason = 'slow'
        elif invocation.call_error and self._dependency_error_limit.allow(task_id, run.org):
            # A dependency failed inside a pass that went on (a retried 429, a
            # 401 the node reported as a warning): kept, under a flood guard of
            # its own (a provider's outage must not spend the slow traces' budget)
            reason = 'dependency_error'
        elif self._keep_success():  # drawn once: the sample ratio applies once per pass
            reason = 'ratio'
        else:
            reason = ''
            if invocation.call_error:
                dropped = 'dependency_error_rate_limited'
        if dropped:
            self.count_dropped(dropped)
        # Queued first: only a trace that will be exported is named by an exemplar or a link
        kept = False
        if reason:
            try:
                self._traces.put_nowait(
                    (
                        task_id,
                        invocation,
                        run.run_kind,
                        run.org,
                        end,
                        unclosed,
                        reason,
                        run.trace_id,
                        run.span_id,
                        run.project,
                        why,
                    )
                )
                kept = True
            except queue.Full:
                self.count_dropped('trace_queue_full')
        if kept:
            if not run.keep_reason:
                run.keep_reason = 'pass_kept'  # the pass links to the run's trace: keep it too
            if reason in ('error', 'stalled', 'slow', 'dependency_error') and len(run.pass_links) < MAX_PASS_LINKS:
                # the run's trace leads to its failed and slow passes (a pass already leads to its run)
                run.pass_links.append((invocation.trace_id, invocation.span_id, reason))
        # Measured whatever the sampling; the exemplars name the pass's spans when they are kept
        try:
            self._count_turn(invocation, run, why, end, kept)
        except Exception:
            self.count_hook_error('turn')

    def _slow_allowed(self, task_id: str, org: str) -> bool:
        """A slow trace may be kept (flood guard); counted when it may not."""
        if self._slow_limit.allow(task_id, org):
            return True
        self.count_dropped('slow_rate_limited')
        return False

    def _keep_success(self) -> bool:
        """Whether a trace that did not fail is kept (RR_OTEL_TRACES_SAMPLE_RATIO; 0 keeps errors only)."""
        return self._ratio >= 1.0 or (self._ratio > 0.0 and self._random.random() < self._ratio)

    def _trace_worker(self) -> None:
        """The 'otel-traces' thread: builds the queued pipeline traces; None stops it."""
        while True:
            item = self._traces.get()
            try:
                if item is None:
                    return
                self._export_invocation(*item)
            except Exception:
                self.count_hook_error('pipeline_trace')
            finally:
                self._traces.task_done()

    def flush_traces(self, timeout: float = 1.0) -> bool:
        """Wait up to `timeout` seconds for the queued pipeline traces to be built; True when none are left."""
        deadline = time.monotonic() + max(0.0, timeout)
        while self._traces.unfinished_tasks:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.005)
        return True

    def _export_invocation(
        self,
        task_id: str,
        invocation: _Invocation,
        run_kind: str,
        org: str,
        end: float,
        unclosed: bool,
        reason: str,
        run_trace_id: int = 0,
        run_span_id: int = 0,
        project: str = '',
        why: str = '',
    ) -> None:
        attributes = self._with_span_org(
            {
                'rocketride.task.id': task_id,
                'rocketride.task.run_kind': run_kind,
                'rocketride.sampling.reason': reason,
                'rocketride.sampling.ratio': self._ratio,
                'rocketride.pipeline.component_calls': len(invocation.components) + invocation.dropped,
            },
            org,
        )
        # The run id is '<pipeline token>.<source>' (a chat, a webhook, a folder)
        pipeline_id, _, source = task_id.partition('.')
        if source:
            attributes['rocketride.pipeline.id'] = pipeline_id
            attributes['rocketride.source'] = source
        if project:
            attributes['rocketride.project.id'] = project
        if unclosed:
            attributes['rocketride.span.unclosed'] = True
        # The pass's own trace (ids from when it began), linked to its run's trace
        links = []
        if run_trace_id and run_span_id:
            links.append(
                Link(
                    SpanContext(run_trace_id, run_span_id, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
                )
            )
        with _preset_ids(invocation.trace_id, invocation.span_id):
            root = self._tracer.start_span(
                'pipeline',
                kind=SpanKind.INTERNAL,
                start_time=_ns(invocation.start),
                attributes=attributes,
                links=links,
            )
        if invocation.error:
            first = invocation.raised[0] if invocation.raised else None
            if first is not None:
                # which node raised what: the pass's cause, readable from the trace list
                root.set_status(Status(StatusCode.ERROR, f'{first[0]}: {first[1]}: {first[2]}'[:MAX_CAUSE_LENGTH]))
                root.set_attribute('error.type', first[5] or first[1])
                root.set_attribute('rocketride.component.type', first[0])
            elif unclosed:
                root.set_status(Status(StatusCode.ERROR, _cut_cause(invocation, why)))
            else:
                root.set_status(Status(StatusCode.ERROR, 'a component failed'))
        elif invocation.marks:
            # A node reported its call failed and went on (an LLM answering with the
            # provider's error): the pass failed for that question
            component_type, _, error_type = invocation.marks[0][:3]
            root.set_status(Status(StatusCode.ERROR, f'{component_type}: {error_type}'))
            root.set_attribute('error.type', error_type)
            root.set_attribute('rocketride.component.type', component_type)
        elif unclosed and invocation.call_error:
            # Cut open (the run ended, a stop, a shutdown) after a call failed: that failure
            # is the likely cause, not a success
            root.set_status(Status(StatusCode.ERROR, _cut_cause(invocation, why)))
        if unclosed and why:
            root.set_attribute('rocketride.pass.closed_by', why[:MAX_VALUE_LENGTH])
        context = trace.set_span_in_context(root)
        spans = []  # (type, start, end, span) for the relayed calls' parents
        marks = list(invocation.marks)
        raised = list(invocation.raised)
        for component_type, start, finish, result, component_id in invocation.components:
            child = self._tracer.start_span(
                f'component {component_type}',
                context=context,
                kind=SpanKind.INTERNAL,
                start_time=_ns(start),
                attributes={
                    'rocketride.component.type': component_type,
                    'rocketride.component.id': component_id,
                    **({'rocketride.component.result': result} if result else {}),
                },
            )
            if result == 'error':
                child.set_status(Status(StatusCode.ERROR, 'component returned an error'))
            for mark in [
                m
                for m in marks
                # the same component, then the time: two adjacent nodes of one type stay apart
                if m[0] == component_type
                and m[4] == component_id
                and start - MATCH_TOLERANCE <= m[1] <= finish + MATCH_TOLERANCE
            ]:
                marks.remove(mark)
                child.set_status(Status(StatusCode.ERROR, mark[2]))
                child.set_attribute('error.type', mark[2])
                child.add_event(
                    'exception',
                    {'exception.type': mark[2], 'exception.message': mark[3]},
                    timestamp=_ns(mark[1]),
                )
            for error in [
                r
                for r in raised
                if r[0] == component_type
                and r[7] == component_id
                and start - MATCH_TOLERANCE <= r[6] <= finish + MATCH_TOLERANCE
            ]:
                raised.remove(error)
                _component_type, cls, message, location, _mono, code, wall, _component_id = error
                child.set_status(Status(StatusCode.ERROR, f'{cls}: {message}'[:MAX_CAUSE_LENGTH]))
                # error.type: the engine's code for it (bounded); exception.type: the class
                child.set_attribute('error.type', code or cls)
                child.add_event('exception', _raised_attributes(cls, message, location), timestamp=_ns(wall))
            child.end(end_time=_ns(max(finish, start)))
            spans.append((component_type, start, max(finish, start), child))
        # A node's logical model calls (GenAI conventions): their HTTP tries go under them
        models = []
        for (
            component_type,
            start_ns,
            end_ns,
            gen_attributes,
            error_type,
            message,
            retries,
            usage,
            answered_by,
            model_span_id,
        ) in sorted(invocation.models, key=lambda m: m[1]):
            parent = _call_parent(spans, component_type, start_ns / 1e9, end_ns / 1e9)
            name = ' '.join(
                part
                for part in (gen_attributes.get('gen_ai.operation.name'), gen_attributes.get('gen_ai.request.model'))
                if part
            )
            span_attributes = dict(gen_attributes, **{'rocketride.component.type': component_type})
            for key, attribute in _GENAI_USAGE.items():
                if usage.get(key):
                    span_attributes[attribute] = usage[key]
            if answered_by:
                span_attributes['gen_ai.response.model'] = answered_by
            # the span id its metrics' exemplars name
            with _preset_ids(0, model_span_id):
                model = self._tracer.start_span(
                    name or 'model call',
                    context=trace.set_span_in_context(parent) if parent is not None else context,
                    kind=SpanKind.CLIENT,
                    start_time=start_ns,
                    attributes=span_attributes,
                )
            for at, kind, wait in retries:
                model.add_event(
                    'rocketride.model.retry',
                    {'error.type': str(kind)[:MAX_VALUE_LENGTH], 'rocketride.retry.delay_s': float(wait)},
                    timestamp=int(at),
                )
            if error_type:
                model.set_attribute('error.type', error_type)
                model.set_status(Status(StatusCode.ERROR, f'{error_type}: {message}' if message else error_type))
                if message:
                    model.add_event('exception', {'exception.message': message}, timestamp=end_ns)
            model.end(end_time=end_ns)
            models.append([component_type, start_ns, end_ns, model, {}])
        tolerance = int(MATCH_TOLERANCE * 1e9)
        for component_type, name, start_ns, end_ns, call_attributes, error_type, span_id in sorted(
            invocation.calls, key=lambda c: c[2]
        ):
            holder = next(
                (
                    m
                    for m in models
                    if m[0] == component_type and m[1] - tolerance <= start_ns and end_ns <= m[2] + tolerance
                ),
                None,
            )
            if holder is not None:
                # a try of a model call: a later HTTP request with the same method, server
                # and route as an earlier one under it is a resend (HTTP conventions)
                parent = holder[3]
                method = call_attributes.get('http.request.method')
                if method:
                    key = (method, call_attributes.get('server.address'), call_attributes.get('url.template'))
                    tries = holder[4].get(key, 0)
                    if tries:
                        call_attributes = dict(call_attributes, **{'http.request.resend_count': tries})
                    holder[4][key] = tries + 1
            else:
                parent = _call_parent(spans, component_type, start_ns / 1e9, end_ns / 1e9)
            # A call that sent a traceparent (the model server) keeps the span id it sent:
            # the server's span is its child
            with _preset_ids(0, span_id) if span_id else contextlib.nullcontext():
                call = self._tracer.start_span(
                    name,
                    context=trace.set_span_in_context(parent) if parent is not None else context,
                    kind=SpanKind.CLIENT,
                    start_time=start_ns,
                    attributes=dict(call_attributes, **{'rocketride.component.type': component_type}),
                )
            if error_type:
                call.set_attribute('error.type', error_type)
                call.set_status(Status(StatusCode.ERROR, error_type))
            call.end(end_time=end_ns)
        root.end(end_time=_ns(max(end, invocation.start)))

    def _observe_runs(self, options: CallbackOptions):
        counts = {state: 0 for state in _LIVE_STATES}
        with self._lock:
            for run in self._runs.values():
                state = STATE_NAMES.get(run.state)
                if state in counts:
                    counts[state] += 1
        return [Observation(value, {'rocketride.task.state': state}) for state, value in counts.items()]

    def _observe_idle(self, options: CallbackOptions):
        """
        rocketride.pass.idle: for each innermost open component type and org, the
        longest a pass has gone without an enter or leave while a call is still
        open. A hung node, a provider that never answers, a lost leave. A working
        agent keeps emitting its tools' enters and leaves, so it stays near 0;
        an idle source has no pass open, so it is not counted. 0 for a type and
        org reported before and idle no more (dropped IDLE_KEY_SECONDS later),
        so the series do not go stale nor stay forever.
        """
        now = time.monotonic()
        passes: List[Tuple[str, str, str, float]] = []  # (task, component type, raw org, idle seconds)
        stalled: List[Tuple[str, str, str, float, int]] = []
        with self._lock:
            for task_id, run in self._runs.items():
                if run.debugger:
                    continue
                for invocation in run.invocations.values():
                    if not invocation.stack:
                        continue
                    provider = (run.types or {}).get(invocation.stack[-1][0])
                    component_type = self._bounded_type_locked(provider) if provider else '_unknown'
                    if any(fnmatch.fnmatchcase(component_type, pattern) for pattern in self._stall_exempt):
                        continue
                    seconds = max(0.0, now - invocation.last_mono)
                    passes.append((task_id, component_type, run.org, seconds))
                    if self._stall_log and seconds >= self._stall_log and not invocation.stall_logged:
                        invocation.stall_logged = True
                        invocation.stall_noted = True
                        stalled.append((task_id, component_type, run.org, seconds, invocation.trace_id, run.project))
        # The org's cap takes the lock: outside it
        idle: Dict[Tuple[str, str], float] = {}
        for _task_id, component_type, org_id, seconds in passes:
            key = (component_type, self._org(org_id) or '')
            idle[key] = max(idle.get(key, 0.0), seconds)
        with self._lock:
            for key in idle:
                if key in self._idle_keys or len(self._idle_keys) < MAX_ATTRIBUTE_SETS:
                    self._idle_keys[key] = now
            for key, seen in list(self._idle_keys.items()):
                if key not in idle and now - seen > IDLE_KEY_SECONDS:
                    del self._idle_keys[key]
            keys = set(self._idle_keys)
        for task_id, component_type, org_id, seconds, trace_id, project in stalled:
            self._stall_line(task_id, component_type, org_id, seconds, trace_id, project)
        observations = []
        for component_type, org in keys | set(idle):
            attributes = {'rocketride.component.type': component_type}
            if org:
                attributes[ORG_KEY] = org
            observations.append(Observation(round(idle.get((component_type, org), 0.0)), attributes))
        return observations

    def _stall_line(
        self, task_id: str, component_type: str, org_id: str, seconds: float, trace_id: int, project: str = ''
    ) -> None:
        """One error line on stdout for a stalled pass: the run to look at (logs keep errors only)."""
        try:
            line = {
                'level': 'error',
                'event': 'rocketride.pass.stalled',
                'task_id': task_id,
                # The pass's trace: kept if the pass then fails or is slow
                'trace_id': f'{trace_id:032x}' if trace_id else '',
                'component_type': component_type,
                'idle_s': round(seconds),
            }
            if project:
                line['project_id'] = project
            org = self._span_org(org_id)
            if org:
                line['org_id'] = org
            sys.stdout.write(json.dumps(line) + '\n')
            sys.stdout.flush()
        except Exception:
            self.count_hook_error('pass_stalled_log')
