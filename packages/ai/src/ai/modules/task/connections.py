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
Who is connected to this engine: open connections and signed-in people per customer.

Four metrics (no-ops unless telemetry is on):

- rocketride.ws.connections: open client connections, by organization and by
  how the client signed in: `user` (a person's credential: the console, the
  IDE, the SDK with an API key), `run_key` (a run's tk_/pk_ key: apps,
  webhooks, chat widgets) or `unauthenticated` (not signed in, or signed out;
  no org). Adds up to rocketride.ws.connection.count.
- rocketride.ws.users: distinct people connected, by organization. Counted
  here from `user` connections only (a run key carries its deploy owner, not
  the person using the app). User ids are never exported, only the count.
- rocketride.ws.users.all: distinct people connected to this engine, whatever
  their organization (one person in two organizations counts once).
- rocketride.ws.disconnects: closed connections, by reason (`closed`: closed
  normally, by either side; `error`: the connection broke: dropped without a
  close (1006), reset, or closed with an error code; `unauthenticated`: it
  closed without ever signing in), and by organization (the last one it
  signed in to).

The organization is bounded by the telemetry org cap (past it: '_other'), as
every other per-customer metric; the attribute sets of each metric are sized
to that cap (sets_per_org).
"""

import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from ai.common import telemetry

CLIENT_USER = 'user'
CLIENT_RUN_KEY = 'run_key'
CLIENT_UNAUTHENTICATED = 'unauthenticated'
CLIENTS = (CLIENT_USER, CLIENT_RUN_KEY, CLIENT_UNAUTHENTICATED)

REASON_CLOSED = 'closed'
REASON_ERROR = 'error'
REASON_UNAUTHENTICATED = 'unauthenticated'
REASONS = (REASON_CLOSED, REASON_ERROR, REASON_UNAUTHENTICATED)
# what an entry that is not a TaskConn reads as (never raises: a not-signed-in connection)
_NOT_A_CONNECTION = (None, None, False)


def _none() -> Iterable[Any]:
    return ()


# the open connections, set by the task server (read on the SDK's export thread)
_source: Callable[[], Iterable[Any]] = _none


def observe(source: Callable[[], Iterable[Any]]) -> None:
    """Register the source of the open connections (the task server's registry)."""
    global _source, _last
    _source = source
    _last = (-SNAPSHOT_TTL_S, [])  # a new source: read it at once


def owner(conn: Any) -> Tuple[str, Optional[str], Optional[str]]:
    """(client kind, org id, user id) of a connection (TaskConn.connection_state); never raises."""
    account, _, _ = getattr(conn, 'connection_state', _NOT_A_CONNECTION)
    if account is None:
        return CLIENT_UNAUTHENTICATED, None, None
    org = telemetry.org_id_of(account)
    if (getattr(account, 'auth', '') or '').startswith(('pk_', 'tk_')):
        return CLIENT_RUN_KEY, org, None
    return CLIENT_USER, org, getattr(account, 'userId', None) or None


# One walk of the registry serves the three metrics of one collection
SNAPSHOT_TTL_S = 1.0
_last: Tuple[float, List[Tuple[str, Optional[str], Optional[str]]]] = (-SNAPSHOT_TTL_S, [])


def _snapshot() -> List[Tuple[str, Optional[str], Optional[str]]]:
    global _last
    taken, owners = _last
    now = time.monotonic()
    if now - taken < SNAPSHOT_TTL_S:
        return owners
    owners = _walk()
    _last = (now, owners)
    return owners


def _walk() -> List[Tuple[str, Optional[str], Optional[str]]]:
    # a copy first: the registry changes on the event loop while this runs (a
    # copy that raced a change is taken again)
    for _ in range(3):
        try:
            conns = list(_source())
            break
        except RuntimeError:
            continue
    else:
        return []
    return [owner(conn) for conn in conns]


def connections_by_org() -> List[Tuple[float, Dict[str, Any]]]:
    """Open connections per (organization, client kind)."""
    counts: Dict[Tuple[Optional[str], str], int] = {}
    for kind, org, _ in _snapshot():
        counts[(org, kind)] = counts.get((org, kind), 0) + 1
    return [(float(count), {'org': org, 'client': kind}) for (org, kind), count in counts.items()]


def users_by_org() -> List[Tuple[float, Dict[str, Any]]]:
    """Distinct people connected per organization (user ids stay here)."""
    users: Dict[Optional[str], set] = {}
    for kind, org, user in _snapshot():
        if kind == CLIENT_USER and user:
            users.setdefault(org, set()).add(user)
    return [(float(len(ids)), {'org': org}) for org, ids in users.items()]


def users_all() -> List[Tuple[float, Dict[str, Any]]]:
    """Distinct people connected to this engine, whatever their organization."""
    users = {user for kind, _, user in _snapshot() if kind == CLIENT_USER and user}
    return [(float(len(users)), {})]


def disconnect_reason(conn: Any) -> str:
    """Why a connection closed: never signed in, broken, or a normal close."""
    account, last_org, broke = getattr(conn, 'connection_state', _NOT_A_CONNECTION)
    if account is None and not last_org:
        return REASON_UNAUTHENTICATED
    return REASON_ERROR if broke else REASON_CLOSED


CONNECTIONS = telemetry.ObservableCount(
    'rocketride.ws.connections',
    '{connection}',
    'Open client connections, by organization and how the client signed in (user | run_key | unauthenticated)',
    observe=connections_by_org,
    attributes={'org': 'rocketride.org.id', 'client': 'rocketride.ws.client'},
    sets_per_org=len(CLIENTS),
)

USERS = telemetry.ObservableCount(
    'rocketride.ws.users',
    '{user}',
    'Distinct people with an open connection (signed in with their own credential), by organization',
    observe=users_by_org,
    attributes={'org': 'rocketride.org.id'},
    sets_per_org=1,
)

USERS_ALL = telemetry.ObservableCount(
    'rocketride.ws.users.all',
    '{user}',
    'Distinct people with an open connection to this engine, whatever their organization',
    observe=users_all,
)

# Who ended a connection, and the close codes kept as values (the rest '_other')
CLOSED_BY = ('client', 'server', 'network')  # and '_unknown' (close_details)
CLOSE_CODES = (1000, 1001, 1005, 1006, 1008, 1009, 1011, 1012, 1013)

DISCONNECTS = telemetry.Counter(
    'rocketride.ws.disconnects',
    '{connection}',
    'Closed client connections, by reason (closed | error | unauthenticated), who closed it, close code '
    'and organization',
    attributes={
        'reason': 'rocketride.ws.disconnect.reason',
        'org': 'rocketride.org.id',
        'closed_by': 'rocketride.ws.closed_by',
        'code': 'rocketride.ws.close_code',
    },
    sets_per_org=len(REASONS) * (len(CLOSED_BY) + 1) * (len(CLOSE_CODES) + 2),
)

CONNECTION_DURATION = telemetry.Histogram(
    'rocketride.ws.connection.duration',
    's',
    'How long client connections stayed open, by reason and who closed them (no org: bounded series)',
    attributes={'reason': 'rocketride.ws.disconnect.reason', 'closed_by': 'rocketride.ws.closed_by'},
)


# Connections refused before the WebSocket opened (close 1008), by why. No client
# address is exported (personal data)
REFUSED_PER_IP = 'per_ip_limit'
REFUSED_IP_TABLE_FULL = 'ip_table_full'

REFUSED = telemetry.Counter(
    'rocketride.ws.refused',
    '{connection}',
    'Connections refused before signing in (close 1008): an address already holds the most unauthenticated '
    'connections (per_ip_limit), or too many addresses do (ip_table_full)',
    attributes={'reason': 'error.type'},
)

# the unauthenticated slots per address, set by the task server
_slots: Callable[[], Iterable[int]] = _none


def observe_unauthenticated_slots(source: Callable[[], Iterable[int]]) -> None:
    """Register the source of the unauthenticated connections held per address."""
    global _slots
    _slots = source


def _slot_readings() -> List[Tuple[float, Dict[str, Any]]]:
    try:
        held = list(_slots())
    except RuntimeError:
        return []
    return [(float(max(held, default=0)), {'measure': 'max_per_address'}), (float(len(held)), {'measure': 'addresses'})]


UNAUTHENTICATED_SLOTS = telemetry.Gauge(
    'rocketride.ws.unauthenticated_slots',
    '{connection}',
    'Unauthenticated connections: the most one address holds (max_per_address; refused at the per-address limit) '
    'and the addresses holding any (addresses)',
    observe=_slot_readings,
    attributes={'measure': 'rocketride.ws.slots.measure'},
)


def refused(reason: str) -> None:
    """Count a connection refused before it opened; never raises."""
    try:
        REFUSED.add(1, reason=reason)
    except Exception:
        pass


def close_details(conn: Any) -> Tuple[str, str]:
    """(who closed, close code) of a closed connection, from its transport; bounded values, never raises."""
    transport = getattr(conn, '_transport', None)
    closed_by = getattr(transport, 'closed_by', None)
    code = getattr(transport, 'close_code', None)
    if closed_by not in CLOSED_BY:
        closed_by = '_unknown'
    if code is None:
        return closed_by, 'none'  # no close frame: the connection broke
    return closed_by, str(code) if code in CLOSE_CODES else '_other'


def disconnected(conn: Any, opened_at: Optional[float] = None) -> None:
    """Count a closed connection, for the last org it signed in to, and time it; never raises."""
    try:
        _, org, _ = owner(conn)
        _, last_org, _ = getattr(conn, 'connection_state', _NOT_A_CONNECTION)
        reason = disconnect_reason(conn)
        closed_by, code = close_details(conn)
        DISCONNECTS.add(1, reason=reason, org=org or last_org, closed_by=closed_by, code=code)
        if opened_at is not None:
            CONNECTION_DURATION.record(max(0.0, time.monotonic() - opened_at), reason=reason, closed_by=closed_by)
    except Exception:
        pass
