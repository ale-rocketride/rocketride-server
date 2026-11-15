"""
End-to-end load on one EaaS: ./engine load.py PORT [REQUESTS] [CLIENTS] [text|llm] [WARMUP] [LLM_URL]

Positional: the engine reads some --options itself (--pipeline among them).

text: webhook → parse → response_text, 2 KB objects (EaaS and the run's process,
no outside call). llm: chat → llm_openai_api → answers against mock_llm.py
(every request makes an HTTP call from the node: its span, relay and metric).
Each client starts its own run and sends its requests one after the other; the
first WARMUP requests of each client are not counted. Prints one JSON line:
the latency of a request (send, or chat) p50/p95 in ms.
"""

import argparse
import asyncio
import json
import time

from rocketride import RocketRideClient
from rocketride.schema import Question

TEXT = {
    'source': 'webhook_1',
    'components': [
        {'id': 'webhook_1', 'provider': 'webhook', 'config': {'mode': 'Source', 'type': 'webhook', 'parameters': {}}},
        {'id': 'parse_1', 'provider': 'parse', 'config': {}, 'input': [{'lane': 'tags', 'from': 'webhook_1'}]},
        {
            'id': 'response_text_1',
            'provider': 'response_text',
            'config': {'laneName': 'text'},
            'input': [{'lane': 'text', 'from': 'parse_1'}],
        },
    ],
}


def llm_pipeline(base_url: str) -> dict:
    return {
        'source': 'chat_1',
        'components': [
            {'id': 'chat_1', 'provider': 'chat', 'config': {}},
            {
                'id': 'llm_1',
                'provider': 'llm_openai_api',
                'config': {'model': 'mock', 'base_url': base_url, 'apikey': 'load-test'},
                'input': [{'lane': 'questions', 'from': 'chat_1'}],
            },
            {
                'id': 'answers_1',
                'provider': 'response_answers',
                'config': {},
                'input': [{'lane': 'answers', 'from': 'llm_1'}],
            },
        ],
    }


async def client_run(port: str, args, latencies: list) -> None:
    async with RocketRideClient(uri=f'ws://localhost:{port}', auth='local-test') as client:
        pipeline = TEXT if args.pipeline == 'text' else llm_pipeline(args.llm_url)
        run = await client.use(pipeline=pipeline, name=f'telemetry overhead {args.pipeline}')
        for i in range(args.requests + args.warmup):
            started = time.perf_counter()
            if args.pipeline == 'text':
                await client.send(run['token'], 'x' * 2000, objinfo={'name': f'l{i}.txt'}, mimetype='text/plain')
            else:
                question = Question()
                question.addQuestion(f'question {i}')
                await client.chat(token=run['token'], question=question)
            if i >= args.warmup:
                latencies.append(time.perf_counter() - started)
        await client.terminate(run['token'])


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('port')
    parser.add_argument('requests', type=int, nargs='?', default=200)
    parser.add_argument('clients', type=int, nargs='?', default=1)
    parser.add_argument('pipeline', choices=('text', 'llm'), nargs='?', default='text')
    parser.add_argument('warmup', type=int, nargs='?', default=20)
    parser.add_argument('llm_url', nargs='?', default='http://127.0.0.1:18089/v1')
    args = parser.parse_args()
    latencies: list = []
    await asyncio.gather(*(client_run(args.port, args, latencies) for _ in range(args.clients)))
    latencies.sort()

    def at(q):
        return round(latencies[min(len(latencies) - 1, int(len(latencies) * q))] * 1000, 2)

    print(json.dumps({'n': len(latencies), 'p50_ms': at(0.5), 'p95_ms': at(0.95)}))


asyncio.run(main())
