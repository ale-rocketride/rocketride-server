"""
A local OpenAI-compatible endpoint for load tests: ./engine mock_llm.py [port] [delay_ms]

Answers POST /v1/chat/completions with a fixed reply after delay_ms, so a
pipeline's LLM node makes a real HTTP call (and its telemetry: the call's span,
its relay to EaaS, its metric) without a provider, a key or a cost.
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18089
DELAY = (int(sys.argv[2]) if len(sys.argv) > 2 else 20) / 1000.0
REPLY = {
    'id': 'chatcmpl-load',
    'object': 'chat.completion',
    'created': 0,
    'model': 'mock',
    'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'ok'}, 'finish_reason': 'stop'}],
    'usage': {'prompt_tokens': 10, 'completion_tokens': 1, 'total_tokens': 11},
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length') or 0))
        time.sleep(DELAY)
        body = json.dumps(REPLY).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        body = json.dumps({'object': 'list', 'data': [{'id': 'mock', 'object': 'model'}]}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


ThreadingHTTPServer(('127.0.0.1', PORT), Handler).serve_forever()
