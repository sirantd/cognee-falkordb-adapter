"""``get_id_filtered_graph_data`` equals the neo4j and ladybug semantics of cognee 1.6.1.

``CogneeGraph._get_full_or_id_filtered_graph`` calls this method when the adapter
class has it. Without it, every GRAPH_COMPLETION search reads the full graph.

Both in-core adapters are edge-driven: an edge comes back when one endpoint id is
a target, and a node comes back only as an endpoint of such an edge. ``_oracle``
runs ladybug's query shape (``MATCH (n:Node)-[r]->(m:Node) WHERE n.id IN $ids OR
m.id IN $ids`` — a full scan, on our shared label) and then the row handling of
neo4j and ladybug verbatim. The adapter seeds from the id index instead; the
plan test for that is in ``test_indexes.py``.

Needs a live FalkorDB (``FALKORDB_HOST`` / ``FALKORDB_PORT``).
"""

from __future__ import annotations

import os
import uuid
from uuid import uuid4

import pytest

from cognee.infrastructure.databases.provenance import EdgeIdentity, make_source_ref_key
from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import BASE_LABEL, PROVENANCE_COLUMNS, FalkorDBAdapter
from cognee_falkordb_adapter.adapter import _strip_provenance

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))
REQUIRE_SERVER = os.getenv("FALKORDB_REQUIRED", "").strip().lower() in {"1", "true", "yes"}


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


@pytest.fixture
async def adapter():
    """A throwaway graph per test, with the id indexes the adapter expects."""
    try:
        instance = FalkorDBAdapter(
            host=HOST, port=PORT, graph_database_name=f"idfilter_{uuid.uuid4().hex[:12]}"
        )
    except Exception as exc:
        unreachable = f"FalkorDB not reachable at {HOST}:{PORT}: {exc}"
        if REQUIRE_SERVER:
            pytest.fail(unreachable)
        pytest.skip(unreachable)

    await instance.initialize()
    try:
        yield instance
    finally:
        try:
            await instance._graph.delete()
        except Exception:
            pass
        await instance.close()


async def _oracle(adapter, target_ids):
    """Ladybug's query shape on the shared label, then the neo4j/ladybug row handling."""
    rows = await adapter.query(
        f"""
        MATCH (n:`{BASE_LABEL}`)-[r]->(m:`{BASE_LABEL}`)
        WHERE n.id IN $target_ids OR m.id IN $target_ids
        RETURN properties(n) AS n_properties, properties(m) AS m_properties,
               type(r) AS type, properties(r) AS properties
        """,
        {"target_ids": target_ids},
    )
    nodes_dict = {}
    edges = []
    for record in rows:
        n_props = _strip_provenance(record["n_properties"])
        m_props = _strip_provenance(record["m_properties"])
        r_props = _strip_provenance(record["properties"])
        nodes_dict[n_props["id"]] = (n_props["id"], n_props)
        nodes_dict[m_props["id"]] = (m_props["id"], m_props)
        source_id = r_props.get("source_node_id", n_props["id"])
        target_id = r_props.get("target_node_id", m_props["id"])
        edges.append((source_id, target_id, record["type"], r_props))
    return list(nodes_dict.values()), edges


def _canonical(result):
    """Order-free form: nodes by id, edges as a sorted multiset."""
    nodes, edges = result
    by_id = {node_id: properties for node_id, properties in nodes}
    assert len(by_id) == len(nodes), "a node came back twice"
    return by_id, sorted(edges, key=repr)


@pytest.fixture
async def world(adapter):
    """A small graph with every shape the edge-driven read must get right.

    a -knows-> b -hosts-> d, c -fears-> a, a -self-> a, b -likes-> c, e -far-> f,
    a -bulk-> d without the endpoint id properties (as the bulk loader writes it),
    a -stale-> e whose source_node_id names another node, an edge from a node
    outside the shared label to a, and an isolated node g.
    """
    names = "abcdefg"
    nodes = {name: _Ent(id=uuid4(), name=name) for name in names}
    ref = make_source_ref_key(uuid4(), uuid4())
    await adapter.add_nodes([nodes[name] for name in "ab"], source_ref_key=ref)
    await adapter.add_nodes([nodes[name] for name in "cdefg"])
    ids = {name: str(node.id) for name, node in nodes.items()}

    await adapter.add_edges(
        [(ids["a"], ids["b"], "knows", {"edge_text": "a knows b"})], source_ref_key=ref
    )
    await adapter.add_edges(
        [
            (ids["b"], ids["d"], "hosts", {}),
            (ids["c"], ids["a"], "fears", {}),
            (ids["a"], ids["a"], "self", {}),
            (ids["b"], ids["c"], "likes", {}),
            (ids["e"], ids["f"], "far", {}),
        ]
    )
    await adapter.query(
        f"""
        MATCH (a:`{BASE_LABEL}` {{id: $a}}), (d:`{BASE_LABEL}` {{id: $d}}),
              (e:`{BASE_LABEL}` {{id: $e}})
        CREATE (a)-[:bulk {{edge_text: 'loaded'}}]->(d)
        CREATE (a)-[:stale {{source_node_id: 'ghost', target_node_id: $e}}]->(e)
        CREATE (:Foreign {{id: 'outside'}})-[:outside]->(a)
        """,
        {"a": ids["a"], "d": ids["d"], "e": ids["e"]},
    )
    return ids


@pytest.mark.parametrize(
    "targets",
    [
        ["a"],
        ["b"],
        ["d"],
        ["a", "b"],
        ["a", "c", "e"],
        ["f", "a", "a"],
        list("abcdefg"),
    ],
)
async def test_result_equals_the_neo4j_and_ladybug_semantics(adapter, world, targets):
    target_ids = [world[name] for name in targets]

    result = await adapter.get_id_filtered_graph_data(target_ids)

    assert _canonical(result) == _canonical(await _oracle(adapter, target_ids))


async def test_edges_touch_a_target_and_nodes_are_their_endpoints(adapter, world):
    """The shapes the oracle comparison depends on, stated directly for target a."""
    a, b, c, d, e = (world[name] for name in "abcde")

    nodes, edges = await adapter.get_id_filtered_graph_data([a])
    node_ids = {node_id for node_id, _properties in nodes}
    identities = {(source, target, relationship) for source, target, relationship, _ in edges}

    # Both directions, the self-loop one time, and the bulk-loaded edge through
    # the endpoint fallback; the stale edge keeps its source_node_id property.
    assert identities == {
        (a, b, "knows"),
        (c, a, "fears"),
        (a, a, "self"),
        (a, d, "bulk"),
        ("ghost", e, "stale"),
    }
    assert len(edges) == 5
    # b -hosts-> d is two hops from a; the foreign node is outside the scope.
    assert node_ids == {a, b, c, d, e}


async def test_provenance_is_stripped_from_nodes_and_edges(adapter, world):
    nodes, edges = await adapter.get_id_filtered_graph_data([world["a"]])

    # The fixture wrote refs on a, b and a -knows-> b; prove it before the check.
    stored = await adapter.query(
        f"""
        MATCH (n:`{BASE_LABEL}` {{id: $a}})-[r:knows]->()
        RETURN n.source_ref_keys AS node_keys, r.source_ref_keys AS edge_keys
        """,
        {"a": world["a"]},
    )
    assert stored[0]["node_keys"] and stored[0]["edge_keys"]

    for _node_id, properties in nodes:
        assert not set(PROVENANCE_COLUMNS) & set(properties)
    for *_identity, properties in edges:
        assert not set(PROVENANCE_COLUMNS) & set(properties)


async def test_no_edge_means_an_empty_result(adapter, world):
    """An isolated target, an unknown id, and no ids all give ``([], [])``.

    📌 cognee then falls back to ``get_graph_data``: an empty result here is the
    full read. That is upstream behaviour, mirrored on purpose.
    """
    assert await adapter.get_id_filtered_graph_data([world["g"]]) == ([], [])
    assert await adapter.get_id_filtered_graph_data([str(uuid4())]) == ([], [])
    assert await adapter.get_id_filtered_graph_data([]) == ([], [])


async def test_edge_identities_match_the_edge_identity_of_get_graph_data(adapter, world):
    """The 4-tuples are the ones ``get_graph_data`` returns for the same edges."""
    _all_nodes, all_edges = await adapter.get_graph_data()
    _nodes, edges = await adapter.get_id_filtered_graph_data([world["b"]])

    def identity(edge):
        return EdgeIdentity(source_id=edge[0], target_id=edge[1], relationship_name=edge[2])

    by_identity = {identity(edge): edge for edge in all_edges}
    for edge in edges:
        assert by_identity[identity(edge)] == edge
