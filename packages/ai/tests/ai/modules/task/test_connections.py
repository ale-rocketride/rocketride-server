# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""Who is connected: open connections and people per customer, disconnects by reason; never a user id."""

import pytest

pytest.importorskip('opentelemetry.sdk')

from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402

from ai.common import telemetry as facade  # noqa: E402
from ai.common.telemetry._otel import Telemetry  # noqa: E402
from ai.modules.task import connections  # noqa: E402


class _Account:
    def __init__(self, org, user, auth='rr_key'):
        self.organization = {'id': org}
        self.userId = user
        self.auth = auth


class _Conn:
    """A TaskConn as the metrics read it (TaskConn.connection_state)."""

    def __init__(self, account=None, error=False, closed_by=None, code=None):
        self._transport = type('T', (), {'closed_by': closed_by, 'close_code': code})()
        self._authenticated = account is not None
        self._account_info = account
        self._closed_with_error = error
        self._signed_in_org = account.organization['id'] if account else None

    @property
    def connection_state(self):
        account = self._account_info if self._authenticated else None
        return account, self._signed_in_org, self._closed_with_error


OPEN = [
    _Conn(_Account('org-acme', 'u-1')),  # Ana in the console
    _Conn(_Account('org-acme', 'u-1')),  # ... and in the IDE: one person
    _Conn(_Account('org-acme', 'u-2')),
    _Conn(_Account('org-acme', 'u-owner', auth='pk_abc')),  # a chat widget: the deploy owner, not a person here
    _Conn(_Account('org-beta', 'u-9')),
    _Conn(),  # not signed in yet
]


@pytest.fixture
def otel(monkeypatch):
    reader = InMemoryMetricReader()
    impl = Telemetry(metric_reader=reader, service_version='9.9.9')
    monkeypatch.setattr(facade, '_impl', impl)
    for instrument in (
        connections.CONNECTIONS,
        connections.USERS,
        connections.USERS_ALL,
        connections.DISCONNECTS,
        connections.CONNECTION_DURATION,
    ):
        instrument._bind(impl)
    connections.observe(lambda: OPEN)
    yield reader
    connections.observe(lambda: ())
    impl.shutdown(1)


def _points(reader, name):
    out = {}
    for rm in reader.get_metrics_data().resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for point in metric.data.data_points:
                        out[tuple(sorted(dict(point.attributes).items()))] = (
                            getattr(point, 'value', None) or point.count
                        )
    return out


def test_connections_by_customer_and_how_they_signed_in(otel):
    points = _points(otel, 'rocketride.ws.connections')
    assert points == {
        (('rocketride.org.id', 'org-acme'), ('rocketride.ws.client', 'user')): 3,
        (('rocketride.org.id', 'org-acme'), ('rocketride.ws.client', 'run_key')): 1,
        (('rocketride.org.id', 'org-beta'), ('rocketride.ws.client', 'user')): 1,
        (('rocketride.ws.client', 'unauthenticated'),): 1,
    }
    assert sum(points.values()) == len(OPEN)  # the per-process total, split


def test_distinct_people_per_customer_without_their_ids(otel):
    points = _points(otel, 'rocketride.ws.users')
    # u-1 twice is one person; the run key's owner is not counted
    assert points == {(('rocketride.org.id', 'org-acme'),): 2, (('rocketride.org.id', 'org-beta'),): 1}
    assert not any('u-' in str(key) for key in points)


def test_disconnects_by_reason_who_closed_and_code(otel):
    connections.disconnected(_Conn(_Account('org-acme', 'u-1'), closed_by='client', code=1000))
    connections.disconnected(_Conn(_Account('org-acme', 'u-1'), error=True, closed_by='network'))
    connections.disconnected(_Conn(closed_by='server', code=4321))  # an unexpected code is bounded
    assert _points(otel, 'rocketride.ws.disconnects') == {
        (
            ('rocketride.org.id', 'org-acme'),
            ('rocketride.ws.close_code', '1000'),
            ('rocketride.ws.closed_by', 'client'),
            ('rocketride.ws.disconnect.reason', 'closed'),
        ): 1,
        (
            ('rocketride.org.id', 'org-acme'),
            ('rocketride.ws.close_code', 'none'),  # no close frame: it broke
            ('rocketride.ws.closed_by', 'network'),
            ('rocketride.ws.disconnect.reason', 'error'),
        ): 1,
        (
            ('rocketride.ws.close_code', '_other'),
            ('rocketride.ws.closed_by', 'server'),
            ('rocketride.ws.disconnect.reason', 'unauthenticated'),
        ): 1,
    }


def test_how_long_connections_stayed_open(otel):
    import time

    connections.disconnected(_Conn(_Account('org-acme', 'u-1'), closed_by='client', code=1001), time.monotonic() - 5)
    ((key, count),) = _points(otel, 'rocketride.ws.connection.duration').items()
    assert dict(key) == {'rocketride.ws.disconnect.reason': 'closed', 'rocketride.ws.closed_by': 'client'}
    assert count == 1


def _capped(monkeypatch, org_cardinality, **kwargs):
    reader = InMemoryMetricReader()
    impl = Telemetry(metric_reader=reader, service_version='9.9.9', org_cardinality=org_cardinality, **kwargs)
    monkeypatch.setattr(facade, '_impl', impl)
    for instrument in (connections.CONNECTIONS, connections.USERS, connections.USERS_ALL, connections.DISCONNECTS):
        instrument._bind(impl)
    return reader, impl


def test_customers_past_the_cap_add_up_in_other(monkeypatch):
    """Two orgs past the cap are one '_other' series with both counts, not the last one's."""
    reader, impl = _capped(monkeypatch, 1)
    many = [_Conn(_Account('org-x', f'x{n}')) for n in range(40)] + [
        _Conn(_Account('org-y', f'y{n}')) for n in range(3)
    ]
    connections.observe(lambda: [_Conn(_Account('org-a', 'u'))] + many)
    try:
        points = _points(reader, 'rocketride.ws.connections')
        assert points[(('rocketride.org.id', '_other'), ('rocketride.ws.client', 'user'))] == 43
        assert sum(points.values()) == 44  # still the per-process total
    finally:
        connections.observe(lambda: ())
        impl.shutdown(1)


def test_attribute_sets_are_sized_to_the_org_cap(monkeypatch):
    """Every org keeps its own series up to the org cap, whatever MAX_ATTRIBUTE_SETS is."""
    from ai.common.telemetry import _otel

    monkeypatch.setattr(_otel, 'MAX_ATTRIBUTE_SETS', 4)
    reader, impl = _capped(monkeypatch, 10)
    conns = [_Conn(_Account(f'org-{n}', 'u')) for n in range(10)]
    conns += [_Conn(_Account(f'org-{n}', 'o', auth='tk_x')) for n in range(10)] + [_Conn()]
    connections.observe(lambda: conns)
    try:
        points = _points(reader, 'rocketride.ws.connections')
        assert len(points) == 21 and not any('_other' in str(key) for key in points)
    finally:
        connections.observe(lambda: ())
        impl.shutdown(1)


def test_people_in_all_organizations_count_once(otel):
    # u-1 twice, u-2, u-9: three people (the run key's owner is not one)
    assert _points(otel, 'rocketride.ws.users.all') == {(): 3}


def test_a_close_after_signing_out_counts_for_the_last_customer(otel):
    conn = _Conn(_Account('org-acme', 'u-1'), closed_by='client', code=1000)
    conn._authenticated, conn._account_info = False, None  # deauth: still open, signed out
    assert connections.owner(conn)[0] == 'unauthenticated'
    connections.disconnected(conn)
    ((key, count),) = _points(otel, 'rocketride.ws.disconnects').items()
    assert dict(key)['rocketride.org.id'] == 'org-acme' and dict(key)['rocketride.ws.disconnect.reason'] == 'closed'
    assert count == 1


def test_the_owner_of_a_run_key_is_counted_once_as_a_person(otel):
    connections.observe(
        lambda: [_Conn(_Account('org-acme', 'u-owner', auth='pk_abc')), _Conn(_Account('org-acme', 'u-owner'))]
    )
    assert _points(otel, 'rocketride.ws.users') == {(('rocketride.org.id', 'org-acme'),): 1}


def test_off_is_a_no_op():
    connections.disconnected(_Conn())  # telemetry off: nothing raises
    assert connections.disconnect_reason(_Conn(_Account('o', 'u'), error=True)) == 'error'


def test_a_gauge_adds_up_values_that_end_in_the_same_series(monkeypatch):
    """A plain Gauge (last value per series in the SDK) adds up orgs that fall into '_other'."""
    reader, impl = _capped(monkeypatch, 1)
    gauge = facade.Gauge(
        'rocketride.test.depth',
        '{item}',
        'test',
        observe=lambda: [(1, {'org': 'org-a'}), (40, {'org': 'org-x'}), (3, {'org': 'org-y'})],
        attributes={'org': 'rocketride.org.id'},
    )
    gauge._bind(impl)
    try:
        assert _points(reader, 'rocketride.test.depth') == {
            (('rocketride.org.id', 'org-a'),): 1,
            (('rocketride.org.id', '_other'),): 43,
        }
    finally:
        impl.shutdown(1)


def test_the_task_connection_exposes_what_the_metrics_read():
    """TaskConn.connection_state is the one contract between the connections and their metrics."""
    from ai.modules.task.task_conn import TaskConn

    conn = TaskConn.__new__(TaskConn)  # the state only, no transport
    conn._authenticated, conn._account_info, conn._signed_in_org, conn._closed_with_error = False, None, None, False
    assert conn.connection_state == (None, None, False)
    account = _Account('org-acme', 'u-1')
    conn._authenticated, conn._account_info, conn._signed_in_org = True, account, 'org-acme'
    assert connections.owner(conn) == ('user', 'org-acme', 'u-1')
    conn._authenticated, conn._account_info, conn._closed_with_error = False, None, True  # signed out, then it broke
    assert connections.disconnect_reason(conn) == 'error'


def test_refused_connections_are_counted_by_why_without_an_address(otel):
    connections.REFUSED._bind(facade._impl)
    connections.refused(connections.REFUSED_PER_IP)
    connections.refused(connections.REFUSED_PER_IP)
    connections.refused(connections.REFUSED_IP_TABLE_FULL)
    assert _points(otel, 'rocketride.ws.refused') == {
        (('error.type', 'per_ip_limit'),): 2,
        (('error.type', 'ip_table_full'),): 1,
    }


def test_the_unauthenticated_slots_say_how_close_an_address_is_to_the_limit(otel):
    connections.UNAUTHENTICATED_SLOTS._bind(facade._impl)
    connections.observe_unauthenticated_slots(lambda: [10, 2, 1])  # one address (the balancer's?) at the limit
    try:
        assert _points(otel, 'rocketride.ws.unauthenticated_slots') == {
            (('rocketride.ws.slots.measure', 'max_per_address'),): 10,
            (('rocketride.ws.slots.measure', 'addresses'),): 3,
        }
    finally:
        connections.observe_unauthenticated_slots(lambda: ())
