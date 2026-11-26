# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""
Log levels of the failures that per-chunk and per-frame nodes catch and survive.

A model that is down fails every chunk of a document the same way: the first
failure of an object is logged at ERROR, the later ones at WARNING, and the next
object (``open``) starts again at ERROR.

Each node's real IInstance.py runs with rocketlib, ai.common.* and the node's own
IGlobal stubbed; the model call raises.
"""

import importlib.util
from pathlib import Path
import sys
import threading
import types

import pytest

_NODES_DIR = Path(__file__).parents[1] / 'src' / 'nodes'

_BEGIN, _WRITE, _END = 0, 1, 2

VISION_NODES = ['llm_vision_gemini', 'llm_vision_openai', 'llm_vision_mistral', 'llm_vision_ollama']


class _FakeBase:
    """Stand-in for rocketlib.IInstanceBase and ai.common.llm_base.LLMBase."""

    def preventDefault(self):
        """Nothing to suppress in a test."""
        return None


class _Question:
    """Stand-in for ai.common.schema.Question."""

    def addContext(self, context):
        """Ignore the context."""

    def addQuestion(self, question):
        """Ignore the question."""


class _Doc:
    """Stand-in for ai.common.schema.Doc."""

    def __init__(self, type, page_content, metadata=None):
        """Keep the fields the nodes read."""
        self.type = type
        self.page_content = page_content
        self.metadata = metadata


class _FakeInstance:
    """Stand-in for the engine-side instance handle."""

    def hasListener(self, lane):
        """Every lane is connected."""
        return True

    def writeText(self, text):
        """Drop the write."""

    def writeDocuments(self, documents):
        """Drop the write."""


def _fail(*args, **kwargs):
    raise ConnectionError('model unreachable')


@pytest.fixture
def load(monkeypatch):
    """Return a loader that builds a node's real IInstance and records its log lines."""

    def load_node(node):
        lines = []
        package = types.ModuleType('_first_failure_' + node)
        package.__path__ = [str(_NODES_DIR / node)]
        iglobal = types.ModuleType(package.__name__ + '.IGlobal')
        iglobal.IGlobal = object
        stubs = {
            package.__name__: package,
            iglobal.__name__: iglobal,
            'rocketlib': types.SimpleNamespace(
                IInstanceBase=_FakeBase,
                AVI_ACTION=types.SimpleNamespace(BEGIN=_BEGIN, WRITE=_WRITE, END=_END),
                debug=lambda *a, **kw: None,
                warning=lambda message: lines.append(('WARNING', message)),
                error=lambda message: lines.append(('ERROR', message)),
            ),
            'ai.common.llm_base': types.SimpleNamespace(LLMBase=_FakeBase),
            'ai.common.schema': types.SimpleNamespace(Doc=_Doc, Question=_Question),
            'ai.common.avi.descriptor': types.SimpleNamespace(rename_ext=lambda metadata, ext: metadata),
            'ai.common.image': types.SimpleNamespace(
                ImageProcessor=types.SimpleNamespace(load_image_from_bytes=lambda data: object())
            ),
        }
        for name, value in stubs.items():
            monkeypatch.setitem(sys.modules, name, value)

        spec = importlib.util.spec_from_file_location(
            package.__name__ + '.IInstance', _NODES_DIR / node / 'IInstance.py'
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        instance = module.IInstance()
        instance.instance = _FakeInstance()
        instance.IGlobal = types.SimpleNamespace(
            _chat=types.SimpleNamespace(_prompt='describe', chat=_fail),
            captioner=types.SimpleNamespace(caption=_fail),
            device_lock=threading.Lock(),
        )
        return instance, lines

    return load_node


def _chunks(n):
    return [_Doc('Image', 'cGl4ZWxz', types.SimpleNamespace(chunkId=i)) for i in range(n)]


@pytest.mark.parametrize('node', VISION_NODES)
def test_vision_chunk_failures_first_error_then_warning(load, node):
    """Three failing chunks: one ERROR, two WARNINGs; the next object starts at ERROR."""
    instance, lines = load(node)
    instance.open(object())
    instance.writeDocuments(_chunks(3))
    assert [level for level, _ in lines] == ['ERROR', 'WARNING', 'WARNING']
    assert all('inference failed for chunk' in message for _, message in lines)

    lines.clear()
    instance.open(object())
    instance.writeDocuments(_chunks(2))
    assert [level for level, _ in lines] == ['ERROR', 'WARNING']


def test_vision_without_open_still_logs_the_first_failure_at_error(load):
    """An instance that never saw open() logs its first failure at ERROR."""
    instance, lines = load('llm_vision_mistral')
    instance.writeDocuments(_chunks(2))
    assert [level for level, _ in lines] == ['ERROR', 'WARNING']


def _frame(instance):
    instance.writeImage(_BEGIN, 'image/png', b'')
    instance.writeImage(_WRITE, 'image/png', b'data')
    instance.writeImage(_END, 'image/png', b'')


def test_caption_frame_failures_first_error_then_warning(load):
    """Caption: the first failed frame of an object is an ERROR, the next ones WARNINGs."""
    instance, lines = load('caption')
    instance.open(object())
    _frame(instance)
    _frame(instance)
    assert [level for level, _ in lines] == ['ERROR', 'WARNING']
    assert all(message.startswith('caption: inference failed, passing empty:') for _, message in lines)

    lines.clear()
    instance.open(object())
    _frame(instance)
    assert [level for level, _ in lines] == ['ERROR']
