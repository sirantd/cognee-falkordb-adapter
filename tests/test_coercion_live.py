"""The A3 gate, live half: the store behaviours the coercion layer is built on.

Two kinds of test live here, and the split is the point:

* **What FalkorDB does** — asserted against the raw driver, deliberately
  bypassing the adapter. These are the measurements the rules in
  ``test_coercion.py`` cite. If a FalkorDB upgrade changes one, this file goes
  red and the rule gets revisited instead of quietly becoming folklore.
* **What the adapter does about it** — the consequence a cognify would see.

Needs a live FalkorDB (``FALKORDB_HOST`` / ``FALKORDB_PORT``).
"""

from __future__ import annotations

import os
import uuid
from typing import Optional
from uuid import uuid4

import pytest

from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.constants import BASE_LABEL

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))

REJECTED = "Property values can only be of primitive types or arrays of primitive types"


class _Ent(DataPoint):
    name: str
    summary: Optional[str] = None
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


async def _raw(adapter, query, params=None):
    """Straight to the driver, skipping the adapter's own scrubbing."""
    return await adapter._graph.query(query, params or {})


# ----------------------------------------------------------------------
# What FalkorDB does — the measurements the coercion rules cite
# ----------------------------------------------------------------------


async def test_a_null_property_value_DELETES_the_stored_property(adapter):
    """🚨 The correction that makes null-stripping load-bearing.

    The spec called a null "accepted and silently dropped", i.e. a no-op write.
    It is not: FalkorDB follows Neo4j here, and a null **removes** the property.
    So passing a DataPoint's unset optionals through would not waste a write — it
    would destroy whatever another pipeline had stored under those keys.
    """
    await _raw(adapter, "CREATE (n:T {id: 'n1', keep: 'value'})")

    await _raw(adapter, "MATCH (n:T {id:'n1'}) SET n += $props", {"props": {"keep": None}})

    rows = await _raw(adapter, "MATCH (n:T {id:'n1'}) RETURN properties(n) AS p")
    assert "keep" not in dict(rows.result_set[0][0])


async def test_a_null_inside_an_array_is_a_hard_error_not_a_silent_drop(adapter):
    """It fails the statement, which under ``UNWIND`` is the whole batch."""
    with pytest.raises(Exception, match=REJECTED):
        await _raw(
            adapter,
            "MERGE (n:T {id:'n2'}) SET n.tags = $tags",
            {"tags": ["Dev", None]},
        )


async def test_a_map_property_is_rejected(adapter):
    with pytest.raises(Exception, match=REJECTED):
        await _raw(adapter, "MERGE (n:T {id:'n3'}) SET n.m = $m", {"m": {"k": "v"}})


async def test_nested_and_heterogeneous_arrays_are_stored_natively(adapter):
    """📌 Not "arrays of primitives" — arrays of anything but a map."""
    rows = await _raw(
        adapter,
        "MERGE (n:T {id:'n4'}) SET n.v = $v RETURN n.v AS v",
        {"v": [1, ["a", [True]]]},
    )
    assert rows.result_set[0][0] == [1, ["a", [True]]]


async def test_nul_is_the_only_control_character_the_parser_rejects(adapter):
    """⚠ Measured 2026-08-09 on v4.20.1 / falkordb-py 1.6.2, and it contradicts
    both the spec and the spike's ``graph_io.scrub``, which strip all of C0."""
    with pytest.raises(Exception, match="Failed to parse query parameter"):
        await _raw(adapter, "MERGE (n:T {id:'c0'}) SET n.v = $v", {"v": "a\x00b"})

    for code in (0x07, 0x08, 0x0B, 0x0C, 0x1A, 0x1B, 0x1F, 0x7F):
        text = f"a{chr(code)}b"
        rows = await _raw(
            adapter, "MERGE (n:T {id:'c0'}) SET n.v = $v RETURN n.v AS v", {"v": text}
        )
        assert rows.result_set[0][0] == text, f"0x{code:02x} did not round-trip"


# ----------------------------------------------------------------------
# What the adapter does about it — the cognify-visible consequence
# ----------------------------------------------------------------------


async def test_rewriting_a_node_with_unset_optionals_keeps_the_stored_values(adapter):
    """The whole point: a second cognify must not strip what the first extracted."""
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="Germany", summary="a federal republic")])

    await adapter.add_nodes([_Ent(id=node_id, name="Germany")])  # summary unset -> None

    node = await adapter.get_node(str(node_id))
    assert node["summary"] == "a federal republic"


async def test_a_property_holding_nul_is_written_and_reads_back_scrubbed(adapter):
    """Without the scrub this write raises and takes its whole batch with it."""
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="Ger\x00many")])

    node = await adapter.get_node(str(node_id))
    assert node["name"] == "Germany"


async def test_an_array_property_holding_a_null_still_writes(adapter):
    """📌 Unreachable from a DataPoint, and that is exactly why it needs a test.

    pydantic rejects ``belongs_to_set=["Dev", None]`` before the adapter sees it,
    so the validated path is safe on its own. The unvalidated ones are not: the
    ``(id, properties)`` tuple and bare-dict node forms the interface declares,
    and edge properties, which are a plain dict all the way down. One null in one
    array there fails the whole ``UNWIND`` batch, not the one artifact.
    """
    node_id = str(uuid4())
    await adapter.add_nodes([(node_id, {"type": "Entity", "aliases": ["Deutschland", None]})])

    node = await adapter.get_node(node_id)
    assert node["aliases"] == ["Deutschland"]


async def test_an_edge_property_holding_a_null_still_writes(adapter):
    """Edge properties are never model-validated — they arrive as a plain dict."""
    source, target = uuid4(), uuid4()
    await adapter.add_nodes([_Ent(id=source, name="A"), _Ent(id=target, name="B")])

    await adapter.add_edge(
        str(source), str(target), "relates_to", {"evidence": ["chunk-1", None], "score": None}
    )

    _, edges = await adapter.get_graph_data()
    _, _, _, properties = edges[0]
    assert properties["evidence"] == ["chunk-1"]
    assert "score" not in properties


async def test_a_map_valued_property_survives_a_round_trip_as_json(adapter):
    """``metadata`` is a map on every DataPoint, so this is the hot path, not an
    edge case — and ``get_node_delete_data`` parses it back out."""
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="Germany")])

    delete_data = await adapter.get_node_delete_data([str(node_id)])
    assert delete_data[str(node_id)].indexed_fields == ["name"]


async def test_a_relationship_name_holding_nul_does_not_break_the_write(adapter):
    """The identifier path: a NUL here lands in the query TEXT, not in a param."""
    source, target = uuid4(), uuid4()
    await adapter.add_nodes([_Ent(id=source, name="A"), _Ent(id=target, name="B")])

    await adapter.add_edge(str(source), str(target), "is\x00related_to")

    rows = await adapter.query(
        f"MATCH (:`{BASE_LABEL}`)-[r]->(:`{BASE_LABEL}`) RETURN type(r) AS rel"
    )
    assert rows[0]["rel"] == "isrelated_to"
