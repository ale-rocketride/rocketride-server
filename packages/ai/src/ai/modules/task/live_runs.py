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
Runs an engine had in progress when it died: counted as engine_lost when it is
noticed, whichever engine notices it.

Every engine writes, once a minute, the runs it has in progress (their opaque
org id, run kind and project id; nothing personal) and a heartbeat to the store
(``engine/live-runs/<instance>.json``). An engine that stops writing has died:
the first engine to see its file older than STALE_S, or a new one at its start,
claims the file (a conditional write on the version it read: only one engine
wins, also on S3, whose delete is not conditional) and counts its runs as ended
in error, engine_lost, with their customer (the pipeline-execution
SLO, Failed runs, Customers affected), and writes one ERROR line per run
(``rocketride.task.engine_lost``: org, project, run kind, task id) so the
pipelines an outage hit can be named. A shutdown writes its runs one last time,
marked stopped, so the next engine counts them without waiting for them to go
stale: within about two and a half minutes (an engine first proves its own writes
work for two intervals, plus up to 30 s of start jitter). A stop or a redeploy
loses the runs in progress, and that counts. A run that already finished but is
still registered (the cleanup delay) is not in progress and is never written.

A run that started less than a minute before the death is not in the file (not
counted); one that ended in that minute is (counted once more). A claimed file is
kept an hour (an engine that read it before the claim must still lose its own),
then deleted uncounted. An engine alive but unable to write for 5 minutes (stalled)
is judged dead: its runs are counted then, and its next write starts a new file
(if it dies later, those runs are counted again). On only when telemetry is.
"""

import asyncio
import json
import os
import random
import socket
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from rocketlib import debug

from ai.common import logfmt, telemetry

from . import error_log

PREFIX = 'engine/live-runs/'
INTERVAL_S = 60.0  # how often an engine writes its runs and looks for dead ones
STALE_S = 300.0  # an engine silent this long is dead (5 missed writes)
CLAIM_KEEP_S = 3600.0  # a claimed file is deleted (uncounted) after this
MAX_LINES = 200  # engine_lost lines per dead engine (its runs in progress)
FIRST_TICK_MAX_S = 30.0  # engines started together (a rolling deploy) do not all look at once


def _instance_id() -> str:
    return f'{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}'


class LiveRuns:
    """This engine's runs in progress, written to the store; dead engines' runs counted as engine_lost."""

    def __init__(
        self,
        runs: Callable[[], List[Dict[str, Any]]],
        store: Any = None,
        instance_id: Optional[str] = None,
        clock: Callable[[], float] = time.time,
        write: Callable[[str], None] = logfmt.write,
    ) -> None:
        """
        Args:
            runs: This engine's runs in progress, as telemetry identities
                ({'org_id', 'run_kind', 'project_id', 'task_id'}).
            store: The raw store (IStore); the server's when None.
            instance_id: This engine's file name (unique per process).
            clock: Wall time (heartbeats are compared across engines).
        """
        self._runs = runs
        self._store = store
        self._id = instance_id or _instance_id()
        self._clock = clock
        self._write_line = write
        self._task: Optional['asyncio.Task[None]'] = None
        # since when this engine's own writes have all worked: an engine that
        # cannot write (the store is down), or has only just been able to again,
        # must not declare the others dead (they may not have written yet)
        self._healthy_since: Optional[float] = None

    @property
    def path(self) -> str:
        return f'{PREFIX}{self._id}.json'

    def _raw(self) -> Any:
        if self._store is None:
            from ai.account.store import Store

            self._store = Store.instance().raw_store()
        return self._store

    def start(self) -> None:
        """Write and look every minute, in the background (only when telemetry is on)."""
        if telemetry.is_enabled_by_environment():
            self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        await asyncio.sleep(random.uniform(0.0, FIRST_TICK_MAX_S))
        while True:
            try:
                await self.tick()
            except Exception as e:  # never let this stop the server
                debug(f'[live-runs] {e}')
            await asyncio.sleep(INTERVAL_S)

    async def tick(self) -> None:
        """Write this engine's runs, then count the runs of any engine that died."""
        try:
            await self._write(self._clock())
            if self._healthy_since is None:
                self._healthy_since = self._clock()
        except Exception:
            self._healthy_since = None  # the store fails us: every file may look stale
            raise
        await self.reap()

    async def _write(self, heartbeat: float) -> None:
        body = {'heartbeat': heartbeat, 'runs': [_identity(r) for r in self._runs()]}
        await self._raw().write_file(self.path, json.dumps(body))

    async def reap(self) -> int:
        """Count, once, the runs of every engine whose file is stale; returns how many."""
        store = self._raw()
        counted = 0
        # Only an engine whose writes have worked for two intervals judges the
        # others: after a store outage every file is stale, and the other live
        # engines need an interval to write again
        if self._healthy_since is None or self._clock() - self._healthy_since < 2 * INTERVAL_S:
            return 0
        try:
            names = await store.list_files(PREFIX)
        except Exception:
            return 0
        now = self._clock()
        for name in names:
            path = name if name.startswith(PREFIX) else f'{PREFIX}{name}'
            if path != self.path:
                counted += await self._reap_one(store, path, now)
        return counted

    async def _reap_one(self, store: Any, path: str, now: float) -> int:
        """
        One other engine's file: drop an old claim, or claim a stale file and count
        its runs. Returns the runs counted (0 when fresh, claimed already, or
        another engine's claim won). Never raises: a file it cannot read waits
        for the next tick.
        """
        try:
            raw, version = await store.read_file_with_metadata(path)
            body = json.loads(raw)
        except Exception:
            return 0
        if body.get('claimed'):
            if now - float(body.get('claimed_at') or 0) >= CLAIM_KEEP_S:
                try:
                    await store.delete_file(path)
                except Exception:
                    pass  # deleted by another engine, or next tick
            return 0
        if now - float(body.get('heartbeat') or 0) < STALE_S:
            return 0
        try:
            # only one engine's write on the version it read succeeds: only it counts the runs
            claim = {'claimed': True, 'claimed_at': now, 'claimed_by': self._id}
            await store.write_file_atomic(path, json.dumps(claim), expected_version=version)
        except Exception:
            return 0
        runs = [r for r in body.get('runs') or [] if isinstance(r, dict)]
        if runs:
            telemetry.runs_lost(runs)
            self._lines(runs)
        return len(runs)

    def _lines(self, runs: List[Dict[str, Any]]) -> None:
        """One error line per lost run: which customer's pipeline the outage hit."""
        if not error_log.enabled():
            return
        fmt = logfmt.log_format()
        for run in runs[:MAX_LINES]:
            self._write_line(
                logfmt.format_line(
                    'error',
                    error_log.LOGGER,
                    'A run was lost with its engine (stopped or crashed while the run was in progress)',
                    fmt,
                    event='rocketride.task.engine_lost',
                    org_id=run.get('org_id') or None,
                    project_id=run.get('project_id') or None,
                    task_id=run.get('task_id') or None,
                    run_kind=run.get('run_kind') or None,
                )
            )

    async def shutdown(self) -> None:
        """Stop writing; leave this engine's runs for the next one to count (heartbeat 0: not stale-aged)."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._task is not None or self._store is not None:
            try:
                await self._write(0.0)
            except Exception:
                pass


def _identity(run: Dict[str, Any]) -> Dict[str, str]:
    return {
        'org_id': str(run.get('org_id') or ''),
        'run_kind': str(run.get('run_kind') or 'dev'),
        'project_id': str(run.get('project_id') or ''),
        'task_id': str(run.get('task_id') or ''),
    }
