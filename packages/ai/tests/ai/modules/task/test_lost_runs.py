# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""A call refused for a lost run names the customer, project and source the key belonged to."""

import hashlib
import json

import pytest

from ai.modules.task import lost_runs
from ai.modules.task.lost_runs import LostRuns


def _token(content, prefix=''):
    # as account.generate_token: deterministic over the sorted content
    return f'{prefix}{hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:32]}'


class _Account:
    """Two enabled deployments: ACME's team webhook and a personal (@me) chat deploy."""

    def __init__(self):
        self.walks = 0
        self.artifact_reads = 0

    async def deployments_iter_enabled(self):
        self.walks += 1
        yield {'orgId': 'org-acme', 'teamId': 'team-1', 'projectId': 'proj-bot', 'version': 3}
        yield {'orgId': 'org-beta', 'teamId': 'user~u-7', 'projectId': 'proj-chat', 'version': 1}

    async def deployments_get(self, org_id, team_id, project_id):
        # ACME's team deploy is billed to another team: its runs carry that team
        return {'billingTeamId': 'team-bill'} if team_id == 'team-1' else {}

    async def deployments_artifact(self, org_id, project_id, version):
        self.artifact_reads += 1
        source = 'webhook_1' if project_id == 'proj-bot' else 'chat_1'
        return {'components': [{'id': source}, {'id': 'llm_openai_1'}]}

    def generate_token(self, content, prefix=''):
        return _token(content, prefix)


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def lost(monkeypatch):
    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    lines, clock = [], _Clock()
    return LostRuns(_Account(), write=lines.append, clock=clock), lines, clock


# as the scheduler starts a team deploy: owned by its billing team
ACME_PK = _token({'kind': 'public', 'teamId': 'team-bill', 'project_id': 'proj-bot', 'source': 'webhook_1'}, 'pk_')
BETA_TK = _token(
    {'kind': 'task', 'userId': 'u-7', 'run_kind': 'deploy', 'project_id': 'proj-chat', 'source': 'chat_1'}, 'tk_'
)


async def _settle(runs):
    if runs._rebuilding is not None:
        await runs._rebuilding


async def test_a_refused_deployed_webhook_names_its_customer_project_and_source(lost):
    runs, lines, _ = lost
    runs.refused(ACME_PK)
    await _settle(runs)
    (record,) = [json.loads(line) for line in lines]
    # error: the errors-only log pipeline keeps it (a customer's calls are failing)
    assert record['level'] == 'error' and record['event'] == 'rocketride.task.lost_call'
    assert (record['org_id'], record['project_id'], record['source']) == ('org-acme', 'proj-bot', 'webhook_1')
    assert record['key_kind'] == 'public' and record['calls'] == 1
    assert ACME_PK not in lines[0]  # a run key is a credential: never written


async def test_a_personal_deploy_is_found_by_its_owner(lost):
    runs, lines, _ = lost
    runs.refused(BETA_TK)
    await _settle(runs)
    record = json.loads(lines[0])
    assert (record['org_id'], record['source'], record['key_kind']) == ('org-beta', 'chat_1', 'task')


async def test_one_line_per_lost_run_every_ten_minutes_with_the_calls_since(lost):
    runs, lines, clock = lost
    runs.refused(ACME_PK)
    await _settle(runs)
    for _ in range(899):
        runs.refused(ACME_PK)
    assert len(lines) == 1
    clock.now += lost_runs.LINE_EVERY_S
    runs.refused(ACME_PK)
    assert len(lines) == 2 and json.loads(lines[1])['calls'] == 900


async def test_keys_no_deployment_has_are_counted_in_one_summary_line(lost):
    runs, lines, clock = lost
    runs.refused('pk_' + 'f' * 32)
    await _settle(runs)
    (record,) = [json.loads(line) for line in lines]
    assert record['level'] == 'error' and record['event'] == 'rocketride.task.lost_call_unknown'
    assert record['org_id'] == 'unknown' and 'project_id' not in record
    assert (record['calls'], record['distinct_keys']) == (1, 1)
    assert 'f' * 32 not in lines[0]  # never the key
    for key in ('pk_' + 'e' * 32, 'pk_' + 'e' * 32, 'tk_' + 'd' * 32):
        runs.refused(key)
    assert len(lines) == 1  # one summary line every ten minutes
    clock.now += lost_runs.LINE_EVERY_S
    runs.refused('pk_' + 'f' * 32)  # the index is due a rebuild too: counted once it is done
    await _settle(runs)
    record = json.loads(lines[1])
    assert (record['calls'], record['distinct_keys']) == (4, 3)
    # which kind, and whether one key hammers: by fingerprint, never the key
    assert (record['public_keys'], record['task_keys']) == (2, 1)
    top = [entry.split(':') for entry in record['top_keys'].split(',')]
    assert [int(count) for _, count in top] == [2, 1, 1]
    assert all(len(fingerprint) == 8 for fingerprint, _ in top)
    assert 'e' * 32 not in lines[1] and 'd' * 32 not in lines[1]


async def test_the_registry_is_walked_at_most_every_ten_minutes(lost):
    runs, lines, clock = lost
    runs.refused('pk_' + '1' * 32)
    await _settle(runs)
    runs.refused('pk_' + '2' * 32)  # another miss right after: no second walk
    await _settle(runs)
    assert runs._account.walks == 1 and len(lines) == 1  # and one summary line for both


def test_nothing_is_written_when_the_run_log_is_off(monkeypatch):
    monkeypatch.delenv('RR_LOG_FORMAT', raising=False)
    monkeypatch.setattr(lost_runs.error_log, 'enabled', lambda: False)
    lines = []
    LostRuns(_Account(), write=lines.append).refused(ACME_PK)
    assert lines == []


def test_other_tokens_are_not_run_keys(lost):
    runs, lines, _ = lost
    runs.refused('rr_abc')
    runs.refused('')
    assert lines == []


async def test_a_rebuild_reads_only_deployments_whose_version_moved(lost):
    runs, _, clock = lost
    runs.refused('pk_' + '1' * 32)
    await _settle(runs)
    reads = runs._account.artifact_reads
    clock.now += lost_runs.REBUILD_EVERY_S
    runs.refused('pk_' + '2' * 32)
    await _settle(runs)
    assert runs._account.walks == 2 and runs._account.artifact_reads == reads


async def test_a_registry_outage_keeps_the_known_customers_and_retries_soon(lost):
    runs, lines, clock = lost
    runs.refused('pk_' + '1' * 32)  # a first walk: every deployment known
    await _settle(runs)
    assert ACME_PK in runs._index

    async def broken():  # the registry fails during the next walk
        raise ConnectionError('registry down')
        yield  # pragma: no cover

    runs._account.deployments_iter_enabled = broken
    clock.now += lost_runs.REBUILD_EVERY_S
    runs.refused('pk_' + '2' * 32)
    await _settle(runs)
    assert ACME_PK in runs._index  # still named, not counted as unknown
    assert not runs._stale(clock.now)
    assert runs._stale(clock.now + lost_runs.RETRY_AFTER_S)  # tried again a minute later, not in ten


async def test_a_task_key_is_rebuilt_with_the_engines_token_key(monkeypatch):
    """A tk_ is the engine's keyed HMAC: rebuilt with the same minting function."""
    monkeypatch.setenv('RR_LOG_FORMAT', 'json')
    lines = []

    def keyed(content):
        return 'tk_' + hashlib.sha256(b'server-key' + json.dumps(content, sort_keys=True).encode()).hexdigest()[:32]

    runs = LostRuns(_Account(), write=lines.append, clock=_Clock(), task_token=keyed)
    content = {'kind': 'task', 'teamId': 'team-bill', 'project_id': 'proj-bot', 'source': 'webhook_1'}
    runs.refused(keyed(content))
    await _settle(runs)
    assert json.loads(lines[0])['org_id'] == 'org-acme'


async def test_a_flood_of_made_up_keys_never_hides_a_real_lost_run(lost, monkeypatch):
    """Keys no run ever had, past the table's size: the oldest go, and a real run's call still gets its line."""
    from ai.modules.task import lost_runs

    runs, lines, clock = lost
    monkeypatch.setattr(lost_runs, 'MAX_KEYS', 50)
    runs.refused(ACME_PK)  # a real run's key (it builds the index)
    await _settle(runs)
    lines.clear()
    for n in range(200):
        runs.refused(f'tk_madeup{n:06d}')
    assert len(runs._keys) <= 50
    clock.now += lost_runs.LINE_EVERY_S + 1
    runs.refused(BETA_TK)
    await _settle(runs)
    assert any(json.loads(line)['org_id'] == 'org-beta' for line in lines if 'lost_call' in line)


async def test_a_flood_of_made_up_keys_is_one_summary_line_and_known_keys_keep_theirs(lost, monkeypatch):
    """1000 keys no run ever had, arriving while the index is built, and one real run's key."""
    runs, lines, clock = lost
    monkeypatch.setattr(lost_runs, 'MAX_KEYS', 100)
    for n in range(1000):
        runs.refused(f'tk_madeup{n:06d}')
    runs.refused(ACME_PK)
    assert len(runs._pending) <= 100 and len(runs._keys) <= 100  # bounded while the rebuild runs
    await _settle(runs)
    assert runs._pending == set()
    records = [json.loads(line) for line in lines]
    (known,) = [r for r in records if r['event'] == 'rocketride.task.lost_call']
    assert known['org_id'] == 'org-acme'
    (summary,) = [r for r in records if r['event'] == 'rocketride.task.lost_call_unknown']
    assert summary['distinct_keys'] <= 100
    for n in range(1000):  # the index is current: counted, no line, nothing remembered per key
        runs.refused(f'pk_madeup{n:06d}')
    clock.now += lost_runs.LINE_EVERY_S
    runs.refused(ACME_PK)
    assert len(lines) == 3 and json.loads(lines[2])['event'] == 'rocketride.task.lost_call'
    assert len(runs._keys) <= 100 and runs._account.walks == 1


async def test_a_slow_rebuild_has_one_callback_whatever_the_keys_waiting(lost):
    runs, lines, _ = lost
    walked = []
    rebuilt = runs._rebuilt
    runs._rebuilt = lambda task: (walked.append(len(runs._pending)), rebuilt(task))
    for n in range(50):
        runs.refused(f'pk_slow{n:06d}')
    await _settle(runs)
    assert walked == [50] and len(lines) == 1


async def test_a_summary_held_back_by_its_interval_is_written_when_due(lost, monkeypatch):
    import asyncio

    runs, lines, clock = lost
    monkeypatch.setattr(lost_runs, 'LINE_EVERY_S', 0.05)
    runs.refused('pk_' + 'f' * 32)
    await _settle(runs)
    assert len(lines) == 1
    runs.refused('pk_' + 'e' * 32)  # within the interval: held back, and no other call comes
    assert len(lines) == 1
    clock.now += 0.06
    await asyncio.sleep(0.2)
    assert len(lines) == 2 and json.loads(lines[1])['calls'] == 1


async def test_distinct_keys_past_the_cap_are_reported_as_a_lower_bound(lost, monkeypatch):
    runs, lines, _ = lost
    monkeypatch.setattr(lost_runs, 'MAX_KEYS', 2)
    for key in ('pk_' + 'a' * 32, 'pk_' + 'b' * 32, 'pk_' + 'c' * 32):
        runs._count_unknown(key, 1)
    runs._summary(runs._clock())
    (record,) = [json.loads(line) for line in lines]
    assert (record['calls'], record['distinct_keys'], record['distinct_keys_capped']) == (3, 2, True)
    assert 'at least 2 distinct' in record['msg']
