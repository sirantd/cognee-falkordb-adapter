"""The A4 gate: id lookups are index-backed, on every path that does one.

Needs a live FalkorDB. This is the check that would have caught #68's silent
All-Node-Scan — the spike's own probe passed a "no All-Node-Scan" assertion while
the labelled path was doing a `Node By Label Scan`, because nothing had created
the indexes and nothing errored.

So asserting "the index exists" is not enough. Two further things have to be
true, and each has burned this project once:

* The assertion is on the EXECUTION PLAN, because that is what silently changes.
* The plan is read from ``str(plan)``, **not** by iterating it. ⚠ Iterating an
  ``ExecutionPlan`` yields each operation name once: a two-endpoint MERGE whose
  tree holds two ``Node By Index Scan`` siblings iterates as a single entry, so a
  regression on the *second* endpoint is invisible that way. ``str(plan)`` renders
  the tree, one line per operation, with the bound variable and label attached.
* The query under test is the one the adapter ACTUALLY EMITS, params and all —
  see the emitted-query tests at the bottom. A hand-written approximation is how
  a probe passes while production scans.
"""

from __future__ import annotations

import os
import uuid
from uuid import uuid4

import pytest

from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.constants import BASE_LABEL, NODE_TYPE_LABELS

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))

INDEX_SCAN = "Node By Index Scan"

# The two plans the gate exists to exclude. `All Node Scan` is the unindexed
# collapse; `Node By Label Scan` is the one #68's probe let through, because a
# label without an index still narrows the scan and still walks every node of it.
FORBIDDEN_SCANS = ("All Node Scan", "Node By Label Scan")


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


@pytest.fixture
async def adapter():
    """A throwaway graph per test — construction proves the server is reachable."""
    name = f"test_{uuid.uuid4().hex[:12]}"
    try:
        instance = FalkorDBAdapter(host=HOST, port=PORT, graph_database_name=name)
    except Exception as exc:
        pytest.skip(f"FalkorDB not reachable at {HOST}:{PORT}: {exc}")

    await instance.initialize()
    try:
        yield instance
    finally:
        try:
            await instance._graph.delete()
        except Exception:
            pass
        await instance.close()


async def _plan(adapter, query, params=None) -> str:
    """The rendered plan tree for a query, with its real parameters bound."""
    return str(await adapter._graph.explain(query, params or {}))


def _assert_index_backed(plan: str, expected_scans: int = 1, context: str = "") -> None:
    where = f" [{context}]" if context else ""
    for forbidden in FORBIDDEN_SCANS:
        assert forbidden not in plan, f"{forbidden} in plan{where}:\n{plan}"
    assert plan.count(INDEX_SCAN) == expected_scans, (
        f"expected {expected_scans} × {INDEX_SCAN}{where}, got {plan.count(INDEX_SCAN)}:\n{plan}"
    )


async def _emitted(adapter, call):
    """Run a real adapter call; return every ``(query, params)`` it sent.

    Wraps the driver rather than faking it, so the call under test does its real
    work against the real server and the captured Cypher is exactly what
    production runs — including whatever ``query()`` did to the params on the way
    through.
    """
    sent: list[tuple[str, dict]] = []
    original = adapter._graph.query

    async def recording(query, params=None, *args, **kwargs):
        sent.append((query, params or {}))
        return await original(query, params, *args, **kwargs)

    adapter._graph.query = recording
    try:
        await call()
    finally:
        adapter._graph.query = original
    return sent


# ----------------------------------------------------------------------
# The indexes themselves
# ----------------------------------------------------------------------


async def test_initialize_creates_both_index_families(adapter):
    # A2 made query() return dict rows keyed by column name; `db.indexes()`
    # names its first column `label`.
    indexed = {row["label"] for row in await adapter.query("CALL db.indexes()")}
    for label in (BASE_LABEL, *NODE_TYPE_LABELS):
        assert label in indexed, f"no index on {label}(id) — id lookups will All-Node-Scan"


async def test_initialize_is_idempotent(adapter):
    """Every converge after the first re-runs this; it must not raise."""
    await adapter.initialize()
    await adapter.initialize()


# ----------------------------------------------------------------------
# The shared label — every adapter id lookup
# ----------------------------------------------------------------------


async def test_id_lookup_uses_the_index_not_a_scan(adapter):
    """🚨 The plan, not the index list, is the thing that silently regresses."""
    await adapter.query(
        f"CREATE (n:{BASE_LABEL}:Entity {{id: $id, name: 'probe'}})", {"id": "probe-1"}
    )

    plan = await _plan(adapter, f"MATCH (n:{BASE_LABEL} {{id: 'probe-1'}}) RETURN n")

    _assert_index_backed(plan, context="shared-label lookup")


async def test_an_id_list_lookup_is_index_backed_too(adapter):
    """``WHERE n.id IN $ids`` is the shape every provenance and batch read uses;
    a planner that handles equality but not membership would scan on all of them."""
    plan = await _plan(
        adapter,
        f"MATCH (n:{BASE_LABEL}) WHERE n.id IN $ids RETURN n",
        {"ids": ["probe-1"]},
    )

    _assert_index_backed(plan, context="IN-list lookup")


# ----------------------------------------------------------------------
# The type labels — what the bulk edge loader rides on
# ----------------------------------------------------------------------


@pytest.mark.parametrize("label", NODE_TYPE_LABELS)
async def test_every_type_label_lookup_is_index_backed(adapter, label):
    """📌 This family exists solely for Stage D's loader, which groups its MERGEs
    by endpoint type. #68 measured the difference at 84.5 s versus 2,114 s, so a
    single unindexed type label is the whole migration's wall clock."""
    plan = await _plan(adapter, f"MATCH (n:`{label}` {{id: $id}}) RETURN n", {"id": "probe-1"})

    _assert_index_backed(plan, context=f"{label} lookup")


async def test_the_loaders_two_endpoint_merge_indexes_BOTH_endpoints(adapter):
    """🚨 The one that needs ``str(plan)``: iterating collapses the two sibling
    scans into one entry, so an unindexed *second* endpoint reads as fine.

    This is the loader's exact shape — grouped by (source type, target type) so
    each MERGE is index-backed on both sides.
    """
    plan = await _plan(
        adapter,
        "MATCH (s:`Entity` {id: $s}) MATCH (t:`DocumentChunk` {id: $t}) MERGE (s)-[r:`rel`]->(t)",
        {"s": "probe-1", "t": "probe-2"},
    )

    _assert_index_backed(plan, expected_scans=2, context="loader edge MERGE")


# ----------------------------------------------------------------------
# The queries the adapter actually emits — not approximations of them
# ----------------------------------------------------------------------


async def test_the_read_path_the_adapter_emits_is_index_backed(adapter):
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="Germany")])

    for query, params in await _emitted(adapter, lambda: adapter.get_node(str(node_id))):
        _assert_index_backed(await _plan(adapter, query, params), context="get_node")


async def test_the_node_write_path_the_adapter_emits_is_index_backed(adapter):
    node = _Ent(id=uuid4(), name="Germany")

    for query, params in await _emitted(adapter, lambda: adapter.add_nodes([node])):
        _assert_index_backed(await _plan(adapter, query, params), context="add_nodes")


async def test_the_edge_write_path_the_adapter_emits_indexes_both_endpoints(adapter):
    """``add_edges`` MATCHes each endpoint on the shared label; both must hit the
    index, or every edge written costs a scan of the whole graph."""
    source, target = uuid4(), uuid4()
    await adapter.add_nodes([_Ent(id=source, name="A"), _Ent(id=target, name="B")])

    emitted = await _emitted(
        adapter, lambda: adapter.add_edge(str(source), str(target), "relates_to")
    )

    for query, params in emitted:
        _assert_index_backed(
            await _plan(adapter, query, params), expected_scans=2, context="add_edges"
        )
