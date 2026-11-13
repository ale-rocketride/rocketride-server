# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""An HTTP call's endpoint as a route template from an allowlist: never a raw path (it can carry a token)."""

import pytest

pytest.importorskip('opentelemetry.sdk')

from ai.common.telemetry import _deps  # noqa: E402


@pytest.mark.parametrize(
    'url, template',
    [
        ('http://llm-stub:8001/v1/chat/completions', '/v1/chat/completions'),
        ('https://api.openai.com/v1/embeddings', '/v1/embeddings'),
        ('https://api.anthropic.com/v1/messages?beta=true', '/v1/messages'),
        ('http://ollama:11434/api/chat', '/api/chat'),
        ('https://api.groq.com/openai/v1/chat/completions', '/openai/v1/chat/completions'),
        ('https://api.deepseek.com/chat/completions', '/chat/completions'),
        (
            'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent',
            '/v1beta/models/{model}:generateContent',
        ),
        (
            'https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3:0/converse',
            '/model/{model}/converse',
        ),
        (
            'https://acme.openai.azure.com/openai/deployments/acme-gpt4/chat/completions?api-version=2024-10-21',
            '/openai/deployments/{deployment}/chat/completions',
        ),
        ('http://qdrant:6333/collections/customer-docs/points/search', '/collections/{collection}/points/search'),
        ('https://api.cohere.com/v2/rerank', '/v2/rerank'),
    ],
)
def test_known_routes_are_templates(url, template):
    assert _deps.url_template(url) == template


@pytest.mark.parametrize(
    'url',
    [
        'https://api.telegram.org/bot123456:ABC-secret/sendMessage',  # a token in the path
        'https://hooks.example.com/services/T000/B000/XXXXsecretXXXX',
        'https://api.example.com/tenant-8f3a9c2e1b/v1/chat/completions',  # an id in the prefix
        'https://example.com/',
    ],
)
def test_anything_else_is_other_never_the_path(url):
    assert _deps.url_template(url) == '_OTHER'


def test_the_span_is_named_by_its_route_and_the_port_defaults_to_the_scheme():
    assert _deps.call_name('POST', '/v1/chat/completions') == 'POST /v1/chat/completions'
    assert _deps.call_name('POST', '_OTHER') == 'POST'
    assert _deps._port('https://api.openai.com/v1/messages') == 443
    assert _deps._port('http://llm-stub:8001/v1/chat/completions') == 8001
