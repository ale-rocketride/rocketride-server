"""What an Event Monitor client receives for one run: ./engine event_monitor_ab.py <port> > out.json

Subscribes to every event type (as the Event Monitor app does), runs a pipeline that
parses text and one that fails in a node, and writes a normalised list of the events:
event name, category/type, the body's keys, and every OUTPUT line's text (timestamps,
ids and durations removed so two runs compare). An event carrying a 'tid' member at
any depth (the engine's pass trace id, which EaaS removes) is marked "tid": true.
"""

import asyncio
import json
import re
import sys

from rocketride import RocketRideClient

PORT = sys.argv[1]
TYPES = ['debugger', 'detail', 'summary', 'output', 'flow', 'task', 'sse', 'dashboard', 'billing', 'deploy']
SRC = {'id': 'webhook_1', 'provider': 'webhook', 'config': {'mode': 'Source', 'type': 'webhook', 'parameters': {}}}
GOOD = [
    SRC,
    {'id': 'parse_1', 'provider': 'parse', 'config': {}, 'input': [{'lane': 'tags', 'from': 'webhook_1'}]},
    {
        'id': 'response_text_1',
        'provider': 'response_text',
        'config': {'laneName': 'text'},
        'input': [{'lane': 'text', 'from': 'parse_1'}],
    },
]
BAD = [
    SRC,
    {'id': 'parse_1', 'provider': 'no_such_node', 'config': {}, 'input': [{'lane': 'tags', 'from': 'webhook_1'}]},
]
VOLATILE = re.compile(
    r'\b(tk_[0-9a-f]+|[0-9a-f]{8,}|\d+(\.\d+)?\s*(ms|s|sec|seconds|MB|KB)|\d{4}-\d\d-\d\dT[\d:.]+Z?|\d+)\b'
)

events = []


class Client(RocketRideClient):
    async def on_event(self, message):
        body = message.get('body') or {}
        inner = body.get('body') if isinstance(body.get('body'), dict) else body
        entry = {
            'event': message.get('event'),
            'type': body.get('type') or body.get('category'),
            'keys': sorted(inner)[:12] if isinstance(inner, dict) else [],
        }
        text = inner.get('output') if isinstance(inner, dict) else None
        if isinstance(text, str):
            entry['output'] = VOLATILE.sub('#', text.strip())[:200]
        if '"tid"' in json.dumps(message, default=str):
            entry['tid'] = True
        events.append(entry)
        await super().on_event(message)


async def run(client, components, name):
    try:
        r = await client.use(pipeline={'source': 'webhook_1', 'components': components}, name=name)
    except Exception as e:
        events.append({'event': 'start_failed', 'error': VOLATILE.sub('#', str(e))[:200]})
        return
    await client.add_monitor({'token': r['token']}, TYPES)
    for i in range(2):
        await client.send(
            r['token'], f'hello event monitor {i}\n' * 5, objinfo={'name': f'doc{i}.txt'}, mimetype='text/plain'
        )
    await asyncio.sleep(2)
    await client.terminate(r['token'])
    await asyncio.sleep(1)


async def main():
    async with Client(uri=f'ws://localhost:{PORT}', auth='local-test') as client:
        await run(client, GOOD, 'event monitor good')
        await run(client, BAD, 'event monitor bad')
    print(json.dumps(events, indent=1, sort_keys=True))


asyncio.run(main())
