"""
TransportWebSocket (server side) — the close code tells a closed connection from a broken one.

A peer that closes (1000, 1001, or no code: 1005) is a normal close; a
connection that dropped without a close frame (1006, e.g. a lost network) or
closed with an error code is reported with has_error, as is a reset.
"""

import asyncio

import pytest

pytest.importorskip('fastapi')

from fastapi import WebSocket  # noqa: E402

from rocketride.core.transport_websocket import TransportWebSocket  # noqa: E402


def _socket(close_code):
    """A FastAPI WebSocket whose peer connects, then closes with close_code."""
    incoming = [{'type': 'websocket.connect'}, {'type': 'websocket.disconnect', 'code': close_code}]

    async def receive():
        await asyncio.sleep(0)
        return incoming.pop(0)

    async def send(message):
        pass

    scope = {'type': 'websocket', 'path': '/task/service', 'headers': [], 'query_string': b''}
    return WebSocket(scope, receive=receive, send=send)


async def _closed(close_code):
    reports = []

    async def on_disconnected(reason, has_error):
        reports.append((reason, has_error))

    transport = TransportWebSocket()
    transport.bind(on_disconnected=on_disconnected)
    await asyncio.wait_for(transport.accept(websocket=_socket(close_code)), 2)
    return reports


@pytest.mark.parametrize('code', [1000, 1001, 1005])
async def test_a_normal_close_is_not_an_error(code):
    assert await _closed(code) == [('Connection closed', False)]


@pytest.mark.parametrize('code', [1006, 1011])
async def test_a_dropped_connection_is_an_error(code):
    assert await _closed(code) == [(f'Connection closed ({code})', True)]


async def test_our_own_close_is_not_an_error_even_when_the_framework_raises():
    """After disconnect(), an exception from receive() (not a disconnect message) is still our close."""
    reports = []

    async def on_disconnected(reason, has_error):
        reports.append(has_error)

    async def receive():
        await asyncio.sleep(0)
        if not getattr(receive, 'connected', False):
            receive.connected = True
            return {'type': 'websocket.connect'}
        transport._closed_by_us = True  # as disconnect() sets it
        raise RuntimeError('Cannot call "receive" once a close message has been sent.')

    async def send(message):
        pass

    transport = TransportWebSocket()
    transport.bind(on_disconnected=on_disconnected)
    scope = {'type': 'websocket', 'path': '/task/service', 'headers': [], 'query_string': b''}
    await asyncio.wait_for(transport.accept(websocket=WebSocket(scope, receive=receive, send=send)), 2)
    assert reports == [False]


async def test_the_server_side_remembers_the_close_code_and_who_closed():
    """The server's telemetry reads transport.close_code / closed_by after a disconnect."""
    transport = TransportWebSocket()

    async def on_disconnected(reason, has_error):
        pass

    transport.bind(on_disconnected=on_disconnected)
    await asyncio.wait_for(transport.accept(websocket=_socket(1011)), 2)
    assert (transport.close_code, transport.closed_by) == (1011, 'client')
