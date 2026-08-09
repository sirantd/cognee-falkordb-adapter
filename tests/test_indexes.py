"""Integration: the index families exist and id lookups are actually index-backed.

Needs a live FalkorDB. This is the check that would have caught #68's silent
All-Node-Scan — the spike's own probe passed a "no All-Node-Scan" assertion while
the labelled path was doing a `Node By Label Scan`, because nothing had created
the indexes and nothing errored.

So asserting "the index exists" is not enough. The assertion has to be on the
EXECUTION PLAN, because that is the thing that silently changes.
"""

from __future__ import annotations

import os
import uuid

import pytest

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.constants import BASE_LABEL, NODE_TYPE_LABELS

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))


@pytest.fixture
async def adapter():
    """A throwaway graph per test — construction proves the server is reachable."""
    name = f"test_{uuid.uuid4().hex[:12]}"
    try:
        instance = FalkorDBAdapter(host=HOST, port=PORT, graph_database_name=name)
    except Exception as exc:
        pytest.skip(f"FalkorDB not reachable at {HOST}:{PORT}: {exc}")

    try:
        yield instance
    finally:
        try:
            await instance._graph.delete()
        except Exception:
            pass
        await instance.close()


async def test_initialize_creates_both_index_families(adapter):
    await adapter.initialize()

    # A2 made query() return dict rows keyed by column name; `db.indexes()`
    # names its first column `label`.
    indexed = {row["label"] for row in await adapter.query("CALL db.indexes()")}
    for label in (BASE_LABEL, *NODE_TYPE_LABELS):
        assert label in indexed, f"no index on {label}(id) — id lookups will All-Node-Scan"


async def test_initialize_is_idempotent(adapter):
    """Every converge after the first re-runs this; it must not raise."""
    await adapter.initialize()
    await adapter.initialize()


async def test_id_lookup_uses_the_index_not_a_scan(adapter):
    """🚨 The plan, not the index list, is the thing that silently regresses."""
    await adapter.initialize()
    await adapter.query(
        f"CREATE (n:{BASE_LABEL}:Entity {{id: $id, name: 'probe'}})", {"id": "probe-1"}
    )

    plan = " ".join(
        str(step) for step in await adapter._graph.explain(
            f"MATCH (n:{BASE_LABEL} {{id: 'probe-1'}}) RETURN n"
        )
    )

    assert "Index Scan" in plan, f"id lookup is not index-backed — plan was:\n{plan}"
    assert "All Node Scan" not in plan, f"id lookup degraded to a full scan:\n{plan}"
