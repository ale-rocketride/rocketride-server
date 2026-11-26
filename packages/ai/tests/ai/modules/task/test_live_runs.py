# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""Runs an engine had in progress when it died are counted, once, as engine_lost."""

import json

import pytest

from ai.account.store import VersionMismatchError
from ai.account.store_providers.memory.memory import MemoryStore
from ai.modules.task import live_runs
from ai.modules.task.live_runs import LiveRuns

ACME = {'org_id': 'org-acme', 'run_kind': 'deploy', 'project_id': 'proj-bot', 'task_id': '7f3a1b2c.webhook_1'}
BETA = {'org_id': 'org-beta', 'run_kind': 'dev', 'project_id': 'proj-chat', 'task_id': '9e8d7c6b.chat_1'}


def _healthy(engine):
    """An engine whose own writes have worked for two intervals (it may judge the others)."""
    engine._healthy_since = engine._clock() - 2 * live_runs.INTERVAL_S
    return engine


class _Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def lost(monkeypatch):
    counted = []
    monkeypatch.setattr(live_runs.telemetry, 'runs_lost', lambda runs: counted.extend(runs))
    return counted


async def test_an_engine_writes_its_runs_in_progress():
    store, clock = MemoryStore(), _Clock()
    engine = LiveRuns(lambda: [ACME, BETA], store=store, instance_id='a', clock=clock)
    await engine.tick()
    body = json.loads(await store.read_file('engine/live-runs/a.json'))
    assert body['heartbeat'] == clock.now and body['runs'] == [ACME, BETA]


async def test_a_dead_engines_runs_are_counted_once_by_whichever_engine_sees_it(lost):
    store, clock = MemoryStore(), _Clock()
    dead = LiveRuns(lambda: [ACME, BETA], store=store, instance_id='dead', clock=clock)
    await dead.tick()
    clock.now += live_runs.STALE_S + 1  # it stopped writing
    first = _healthy(LiveRuns(lambda: [], store=store, instance_id='b', clock=clock))
    second = _healthy(LiveRuns(lambda: [], store=store, instance_id='c', clock=clock))
    await first.tick()
    first_counted = len(lost)
    await second.tick()
    assert first_counted == 2 and len(lost) == 2
    assert lost == [ACME, BETA]
    # claimed, kept an hour (a late reader must lose), then deleted uncounted
    assert json.loads(await store.read_file('engine/live-runs/dead.json'))['claimed_by'] == 'b'
    clock.now += live_runs.CLAIM_KEEP_S
    await second.tick()
    assert lost == [ACME, BETA]
    assert 'engine/live-runs/dead.json' not in await store.list_files(live_runs.PREFIX)


async def test_a_reader_that_read_before_the_claim_loses(lost):
    store, clock = MemoryStore(), _Clock()
    await LiveRuns(lambda: [ACME], store=store, instance_id='dead', clock=clock).tick()
    clock.now += live_runs.STALE_S + 1
    _, version = await store.read_file_with_metadata('engine/live-runs/dead.json')
    await _healthy(LiveRuns(lambda: [], store=store, instance_id='b', clock=clock)).tick()
    # another engine's claim on the version it read before: refused
    with pytest.raises(VersionMismatchError):
        await store.write_file_atomic('engine/live-runs/dead.json', '{}', expected_version=version)
    assert lost == [ACME]


async def test_a_live_engine_is_left_alone(lost):
    store, clock = MemoryStore(), _Clock()
    alive = LiveRuns(lambda: [ACME], store=store, instance_id='alive', clock=clock)
    await alive.tick()
    clock.now += live_runs.STALE_S - 30
    await _healthy(LiveRuns(lambda: [], store=store, instance_id='b', clock=clock)).tick()
    assert lost == []


async def test_a_stop_loses_its_runs_and_the_next_engine_counts_them_at_once(lost):
    store, clock = MemoryStore(), _Clock()
    stopping = LiveRuns(lambda: [ACME], store=store, instance_id='old', clock=clock)
    await stopping.tick()
    stopping._task = None
    await stopping.shutdown()  # the redeploy: the run in progress is lost
    clock.now += 5  # the new engine starts right away (and judges after two intervals)
    await _healthy(LiveRuns(lambda: [], store=store, instance_id='new', clock=clock)).tick()
    assert lost == [ACME]


async def test_an_engine_never_counts_its_own_file(lost):
    store, clock = MemoryStore(), _Clock()
    engine = LiveRuns(lambda: [ACME], store=store, instance_id='me', clock=clock)
    await engine.tick()
    clock.now += live_runs.STALE_S + 1
    await engine.tick()
    assert lost == []


class _FailingStore(MemoryStore):
    fail = False

    async def write_file(self, filename, data):
        if self.fail:
            raise OSError('store down')
        return await super().write_file(filename, data)


async def test_after_a_store_outage_an_engine_that_cannot_write_judges_nobody(lost):
    store, clock = _FailingStore(), _Clock()
    await LiveRuns(lambda: [ACME], store=store, instance_id='alive', clock=clock).tick()
    me = _healthy(LiveRuns(lambda: [], store=store, instance_id='me', clock=clock))
    await me.tick()  # healthy: may judge (nothing stale yet)
    store.fail = True
    clock.now += live_runs.STALE_S + 1  # every file looks stale
    with pytest.raises(OSError):
        await me.tick()
    assert await me.reap() == 0 and lost == []


async def test_each_lost_run_is_one_error_line_naming_its_pipeline(lost, monkeypatch):
    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    store, clock, lines = MemoryStore(), _Clock(), []
    await LiveRuns(lambda: [ACME], store=store, instance_id='dead', clock=clock).tick()
    clock.now += live_runs.STALE_S + 1
    await _healthy(LiveRuns(lambda: [], store=store, instance_id='b', clock=clock, write=lines.append)).tick()
    (record,) = [json.loads(line) for line in lines]
    assert record['level'] == 'error' and record['event'] == 'rocketride.task.engine_lost'
    assert (record['org_id'], record['project_id'], record['task_id']) == ('org-acme', 'proj-bot', '7f3a1b2c.webhook_1')


async def test_an_engine_just_able_to_write_again_waits_before_judging(lost):
    store, clock = _FailingStore(), _Clock()
    await LiveRuns(lambda: [ACME], store=store, instance_id='alive', clock=clock).tick()
    me = _healthy(LiveRuns(lambda: [], store=store, instance_id='me', clock=clock))
    await me.tick()  # healthy first
    store.fail = True
    clock.now += live_runs.STALE_S + 1  # the store is down: the other file goes stale
    with pytest.raises(OSError):
        await me.tick()  # the failed heartbeat ends its health
    store.fail = False
    await me.tick()  # the store is back: my first write works
    assert lost == []  # the other engine has had no interval to write yet
    clock.now += 2 * live_runs.INTERVAL_S
    await me.tick()  # healthy for two intervals, and the other file is still stale
    assert lost == [ACME]
