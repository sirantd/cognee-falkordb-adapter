"""initialize() issues its CREATE INDEX commands once per adapter instance.

cognee's get_graph_engine() re-runs initialize() on every lookup (a fresh handle
each call), so a repeated call must not reach FalkorDB again. No live server needed.
"""

from types import SimpleNamespace

import pytest

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.constants import BASE_LABEL, NODE_TYPE_LABELS

pytestmark = pytest.mark.asyncio


class _IndexRecordingGraph:
    def __init__(self, fail_with=None):
        self.calls = []
        self.fail_with = fail_with

    async def create_node_range_index(self, label, prop):
        self.calls.append((label, prop))
        if self.fail_with:
            raise self.fail_with


def _adapter(graph):
    return FalkorDBAdapter(driver=SimpleNamespace(select_graph=lambda name: graph))


async def test_first_call_creates_every_id_index():
    graph = _IndexRecordingGraph()
    await _adapter(graph).initialize()
    assert graph.calls == [(label, "id") for label in (BASE_LABEL, *NODE_TYPE_LABELS)]


async def test_repeat_calls_do_not_reach_the_server():
    graph = _IndexRecordingGraph()
    adapter = _adapter(graph)
    for _ in range(3):
        await adapter.initialize()
    assert len(graph.calls) == 1 + len(NODE_TYPE_LABELS)


async def test_already_indexed_counts_as_ready():
    graph = _IndexRecordingGraph(fail_with=Exception("Attribute 'id' is already indexed"))
    adapter = _adapter(graph)
    await adapter.initialize()
    await adapter.initialize()
    assert len(graph.calls) == 1 + len(NODE_TYPE_LABELS)


async def test_a_real_failure_is_retried_on_the_next_call():
    graph = _IndexRecordingGraph(fail_with=RuntimeError("connection reset"))
    adapter = _adapter(graph)
    with pytest.raises(RuntimeError):
        await adapter.initialize()
    graph.fail_with = None
    await adapter.initialize()
    assert graph.calls[-1] == (NODE_TYPE_LABELS[-1], "id")
