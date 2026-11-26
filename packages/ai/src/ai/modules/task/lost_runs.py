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
Whose run a refused call was for: one ERROR line per lost run.

After an engine restart (a deploy, a crash, a node drain) the runs started
through the API are gone, and the apps, webhooks and schedules that call them
are refused with TASK_NOT_REGISTERED ("Your pipeline is not running"). The
refused call carries only the run's key (tk_ or pk_), not the customer. Run
keys are deterministic over the owner, the project and the source (a pk_ the
account's digest; a tk_ an HMAC with the engine's token key, RR_TASK_TOKEN_KEY),
so the keys of every enabled deployment are rebuilt from the deployment registry
(the scheduler's feed) and a refused key is matched to its customer, project and
source. Without RR_TASK_TOKEN_KEY each engine process has a key of its own: a dead
engine's tk_ cannot be rebuilt (its line says org unknown); a pk_ still can.

One ERROR line per key every 10 minutes (with the calls refused since the last
line), at most 20 lines a minute in all: a customer's calls failing, kept by the
errors-only log pipeline. The key itself is never written (it is a credential).
Keys that match no deployment (a dev run from the IDE, a stale or made-up key)
get no line of their own: they are counted, and one ERROR line every 10 minutes
says how many calls were refused and for how many distinct keys, so a flood of
made-up keys neither fills the log nor takes the known keys' lines. On only when
the run error log is (telemetry or RR_LOG_FORMAT).
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from ai.common import logfmt
from rocketlib import debug

from . import error_log

LINE_EVERY_S = 600.0  # one line per key every 10 minutes
LINES_PER_MINUTE = 20  # in all, whatever the keys
REBUILD_EVERY_S = 600.0  # a miss rebuilds the index at most every 10 minutes
MAX_DEPLOYMENTS = 10000  # the registry walk stops there
MAX_KEYS = 5000  # keys remembered between lines (a flood of made-up keys), and keys waiting for a rebuild
FETCH_CONCURRENCY = 8  # registry reads in flight during a rebuild

Owner = Tuple[str, str, str]  # (org id, project id, source)


@dataclass
class _Seen:
    """A refused key: calls refused since its last line, and when that line was written."""

    calls: int = 0
    last: Optional[float] = None
    first: float = 0.0  # when it was first refused


def _owner_content(team_id: str, billing_team_id: str = '') -> Dict[str, Any]:
    """
    The owner part of a deployed run's key, as the scheduler starts it: a personal
    (@me) deploy ('user~<uid>') is the user's; a team deploy runs as its billing
    team (the stamp written at deploy time, the slot's team for older records).
    """
    if team_id.startswith('user~'):
        return {'userId': team_id[len('user~') :], 'run_kind': 'deploy'}
    return {'teamId': billing_team_id or team_id}


class LostRuns:
    """Refused run keys matched to the deployment they belong to, written as ERROR lines."""

    def __init__(
        self,
        account: Any = None,
        write: Callable[[str], None] = logfmt.write,
        clock: Callable[[], float] = time.monotonic,
        task_token: Optional[Callable[[Dict[str, Any]], str]] = None,
    ) -> None:
        self._account = account
        # how a tk_ is minted (the engine's keyed HMAC); a pk_ is the account's
        # unkeyed digest (a share link)
        self._task_token = task_token
        self._write = write
        self._clock = clock
        self._index: Dict[str, Owner] = {}
        self._built: Optional[float] = None
        self._rebuilding: Optional['asyncio.Task[None]'] = None
        self._pending: set = set()  # keys whose line waits for the rebuild (at most MAX_KEYS)
        # Refused calls for keys no deployment has, since their last summary line
        self._unknown_calls = 0
        self._unknown_keys: set = set()  # distinct ones (at most MAX_KEYS)
        self._unknown_last: Optional[float] = None
        self._unknown_capped = False  # more distinct keys came than are kept: the count is a lower bound
        self._summary_timer: Any = None  # the summary held back by its interval, written when it is due
        self._keys: Dict[str, _Seen] = {}
        # (org, team, project, version) -> its run keys: a rebuild reads only
        # deployments whose version moved (a billing team re-stamped without a
        # new version keeps its old keys until the engine restarts: rare, and
        # its lost runs then read org unknown)
        self._by_deployment: Dict[Tuple[str, str, str, int], Dict[str, Owner]] = {}
        self._tokens = float(LINES_PER_MINUTE)
        self._refilled = clock()

    def refused(self, key: str) -> None:
        """A call with run key `key` was refused: no live run has it."""
        if not key or not key.startswith(('tk_', 'pk_')) or not error_log.enabled():
            return
        now = self._clock()
        if key not in self._index and not self._stale(now):
            # no deployment has it (the index is current): counted, in the summary line
            self._count_unknown(key, 1)
            self._summary(now)
            return
        seen = self._keys.get(key)
        if seen is None:
            if len(self._keys) >= MAX_KEYS:
                self._prune(now)
            if len(self._keys) >= MAX_KEYS:
                return
            seen = self._keys[key] = _Seen(first=now)
        seen.calls += 1
        if seen.last is not None and now - seen.last < LINE_EVERY_S:
            return
        if key not in self._index:
            # not known yet: rebuild the index, then write the line (or count it)
            self._rebuild_then(key)
            return
        self._line(key, now)

    def _prune(self, now: float) -> None:
        """
        Forget keys with no refused call since their last line, and keys that never got
        a line, older than a line's interval. Still full (a flood of made-up keys): the
        oldest half of the keys no run ever had, so a real run's lost calls still get
        their line.
        """
        for key, seen in list(self._keys.items()):
            idle = seen.last if seen.last is not None else seen.first
            if (not seen.calls or seen.last is None) and now - idle >= LINE_EVERY_S:
                del self._keys[key]
                self._pending.discard(key)
        if len(self._keys) >= MAX_KEYS:
            unknown = sorted((seen.first, key) for key, seen in self._keys.items() if key not in self._index)
            for _first, key in unknown[: max(1, len(unknown) // 2)]:
                del self._keys[key]
                self._pending.discard(key)

    def _stale(self, now: float) -> bool:
        return self._built is None or now - self._built >= REBUILD_EVERY_S

    def _rebuild_then(self, key: str) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            now = self._clock()
            self._resolve(key, now)  # no rebuild without a loop: the index as it is
            self._summary(now)
            return
        if self._rebuilding is None or self._rebuilding.done():
            # one callback per rebuild: it walks the keys waiting for it
            self._rebuilding = loop.create_task(self._rebuild())
            self._rebuilding.add_done_callback(self._rebuilt)
        if len(self._pending) < MAX_KEYS:
            self._pending.add(key)

    def _rebuilt(self, _task: Any = None) -> None:
        """The rebuild ended: a line for each waiting key a deployment has; the others are counted."""
        now = self._clock()
        pending, self._pending = self._pending, set()
        for key in pending:
            self._resolve(key, now)
        self._summary(now)

    def _resolve(self, key: str, now: float) -> None:
        if key in self._index:
            self._line(key, now)
            return
        seen = self._keys.pop(key, None)
        if seen is not None:
            self._count_unknown(key, seen.calls)

    def _count_unknown(self, key: str, calls: int) -> None:
        self._unknown_calls += calls
        if len(self._unknown_keys) < MAX_KEYS:
            self._unknown_keys.add(key)
        elif key not in self._unknown_keys:
            self._unknown_capped = True

    def _summary(self, now: float) -> None:
        """One line every LINE_EVERY_S for the calls refused for keys no deployment has (none: no line)."""
        if not self._unknown_calls:
            return
        if self._unknown_last is not None and now - self._unknown_last < LINE_EVERY_S:
            # held back: written when due, even if no other call comes
            self._schedule_summary(LINE_EVERY_S - (now - self._unknown_last))
            return
        calls, keys, capped = self._unknown_calls, len(self._unknown_keys), self._unknown_capped
        self._unknown_calls, self._unknown_keys, self._unknown_last, self._unknown_capped = 0, set(), now, False
        self._write(
            logfmt.format_line(
                'error',
                error_log.LOGGER,
                f'{calls} refused calls, {"at least " if capped else ""}{keys} distinct unknown keys: '
                'calls to a pipeline that is not running '
                '(TASK_NOT_REGISTERED) with a run key no deployment has (a dev run, a stale or made-up key)',
                logfmt.log_format(),
                event='rocketride.task.lost_call_unknown',
                org_id='unknown',
                calls=calls,
                distinct_keys=keys,
                distinct_keys_capped=True if capped else None,
            )
        )

    def _schedule_summary(self, delay: float) -> None:
        if self._summary_timer is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no loop (a test, a shutdown): the next call writes it
        self._summary_timer = loop.call_later(max(0.0, delay), self._summary_due)

    def _summary_due(self) -> None:
        self._summary_timer = None
        self._summary(self._clock())

    def _line(self, key: str, now: float) -> None:
        seen = self._keys.get(key)
        if seen is None or (seen.last is not None and now - seen.last < LINE_EVERY_S) or not self._take(now):
            return
        calls, seen.calls, seen.last = seen.calls, 0, now
        org, project, source = self._index.get(key, ('', '', ''))
        self._write(
            logfmt.format_line(
                'error',
                error_log.LOGGER,
                'Calls to a pipeline that is not running (TASK_NOT_REGISTERED): its run was lost '
                '(an engine restart) or stopped; the caller must start it again',
                logfmt.log_format(),
                event='rocketride.task.lost_call',
                org_id=org or 'unknown',
                project_id=project or None,
                source=source or None,
                key_kind='public' if key.startswith('pk_') else 'task',
                calls=calls,
            )
        )

    def _take(self, now: float) -> bool:
        self._tokens = min(float(LINES_PER_MINUTE), self._tokens + (now - self._refilled) * LINES_PER_MINUTE / 60.0)
        self._refilled = now
        if self._tokens < 1.0:
            return False
        self._tokens -= 1.0
        return True

    async def _rebuild(self) -> None:
        """Every enabled deployment's run keys, from the registry (sha256-verified artifacts)."""
        account = self._account
        if account is None:
            from ai.account import account
        deployments = []
        try:
            async for dep in account.deployments_iter_enabled():
                if len(deployments) >= MAX_DEPLOYMENTS:
                    break
                deployments.append(dep)
        except Exception as e:
            debug(f'lost runs: the deployment registry walk stopped ({type(e).__name__}); naming what it found')
        gate = asyncio.Semaphore(FETCH_CONCURRENCY)

        results = await asyncio.gather(
            *(self._deployment_keys(account, dep, gate) for dep in deployments), return_exceptions=True
        )
        by_deployment = {
            ident: keys for ident, keys in (r for r in results if isinstance(r, tuple)) if keys is not None
        }
        self._by_deployment = by_deployment
        self._index = {key: owner for keys in by_deployment.values() for key, owner in keys.items()}
        self._built = self._clock()

    async def _deployment_keys(
        self, account: Any, dep: Dict[str, Any], gate: asyncio.Semaphore
    ) -> Tuple[Tuple[str, str, str, int], Optional[Dict[str, Owner]]]:
        """One deployment's run keys (remembered by version); None when it could not be read."""
        org, team, project = dep['orgId'], dep['teamId'], dep['projectId']
        ident = (org, team, project, int(dep.get('version') or 0))
        if ident in self._by_deployment:
            return ident, self._by_deployment[ident]
        async with gate:
            try:
                pipeline = await account.deployments_artifact(org, project, dep['version'])
                record = await account.deployments_get(org, team, project) or {}
            except Exception:
                return ident, None  # not remembered: read again at the next rebuild
        owner = _owner_content(team, str(record.get('billingTeamId') or ''))
        # the run's project id is the pipeline's own (task_server), the slot's otherwise
        run_project = str(pipeline.get('project_id') or project)
        keys: Dict[str, Owner] = {}
        # every component: the sources among them name the runs
        for component in pipeline.get('components') or []:
            source = component.get('id') if isinstance(component, dict) else None
            if source:
                for key in self._run_keys(account, owner, run_project, source):
                    keys[key] = (org, project, source)
        return ident, keys

    def _run_keys(self, account: Any, owner: Dict[str, Any], project: str, source: str) -> List[str]:
        """The two keys a run of this source gets: its control key (tk_) and its public key (pk_)."""
        keys = []
        for kind, prefix in (('task', 'tk_'), ('public', 'pk_')):
            content = {'kind': kind, **owner, 'project_id': project, 'source': source}
            if kind == 'task' and self._task_token is not None:
                keys.append(self._task_token(content))
            else:
                keys.append(account.generate_token(content=content, prefix=prefix))
        return keys
