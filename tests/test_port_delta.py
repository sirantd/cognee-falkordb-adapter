"""The A2 gate: the port delta from the Neo4j seed, against a live FalkorDB.

Every test here covers something the seed did with APOC, with GDS, or not at
all. The provenance surface itself is not re-tested — cognee's own contract
suite covers that, and wiring it in is stage A5.

Needs a live FalkorDB (``FALKORDB_HOST`` / ``FALKORDB_PORT``).
"""

from __future__ import annotations

import os
import uuid
from uuid import uuid4

import pytest

from cognee.infrastructure.databases.provenance import EdgeIdentity
from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.constants import BASE_LABEL

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


class _Other(DataPoint):
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


# ----------------------------------------------------------------------
# apoc.create.addLabels — 2 of the 5 APOC sites
# ----------------------------------------------------------------------


async def test_nodes_carry_both_the_shared_label_and_their_type(adapter):
    """The whole point of the shared label: id lookups are index-backed for any type."""
    node = _Ent(id=uuid4(), name="Germany")
    await adapter.add_nodes([node])

    rows = await adapter.query(
        f"MATCH (n:`{BASE_LABEL}` {{id: $id}}) RETURN labels(n) AS labels", {"id": str(node.id)}
    )
    assert set(rows[0]["labels"]) == {BASE_LABEL, "_Ent"}


async def test_a_mixed_type_batch_labels_each_node_correctly(adapter):
    """One statement per label group — the batch must not be labelled by its first member."""
    ent, other = _Ent(id=uuid4(), name="A"), _Other(id=uuid4(), name="B")
    await adapter.add_nodes([ent, other])

    rows = await adapter.query(
        f"MATCH (n:`{BASE_LABEL}`) RETURN n.id AS id, labels(n) AS labels"
    )
    labels = {row["id"]: set(row["labels"]) for row in rows}
    assert labels[str(ent.id)] == {BASE_LABEL, "_Ent"}
    assert labels[str(other.id)] == {BASE_LABEL, "_Other"}


async def test_retyping_a_node_does_not_create_a_second_row_for_its_id(adapter):
    """🚨 The duplicate-id trap this migration exists to leave behind.

    MERGEing on ``(:__Node__:<Type> {id})`` would miss the existing node when its
    type changes and create a second row with the same id — ladybug's exact
    failure mode. The MERGE keys on the shared label alone; the type label is a
    separate SET.
    """
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="A")])
    await adapter.add_nodes([_Other(id=node_id, name="A")])

    rows = await adapter.query(
        f"MATCH (n:`{BASE_LABEL}`) RETURN count(n) AS rows, count(DISTINCT n.id) AS distinct_ids"
    )
    assert rows[0]["rows"] == rows[0]["distinct_ids"] == 1


# ----------------------------------------------------------------------
# apoc.coll.toSet — 2 more APOC sites
# ----------------------------------------------------------------------


async def test_belongs_to_set_is_a_union_across_writes(adapter):
    """A DataPoint cognified into a second dataset keeps both tags."""
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="A", belongs_to_set=["Dev"])])
    await adapter.add_nodes([_Ent(id=node_id, name="A", belongs_to_set=["Prod"])])

    node = await adapter.get_node(str(node_id))
    assert node["belongs_to_set"] == ["Dev", "Prod"]


async def test_belongs_to_set_union_does_not_duplicate_an_existing_tag(adapter):
    node_id = uuid4()
    await adapter.add_nodes([_Ent(id=node_id, name="A", belongs_to_set=["Dev", "Prod"])])
    await adapter.add_nodes([_Ent(id=node_id, name="A", belongs_to_set=["Dev"])])

    node = await adapter.get_node(str(node_id))
    assert node["belongs_to_set"] == ["Dev", "Prod"]


async def test_duplicate_ids_within_one_batch_keep_every_tag(adapter):
    """UNWIND visits the same node twice and each pass recomputes the union from
    the pre-SET state, so the batch is deduped in Python first."""
    node_id = uuid4()
    await adapter.add_nodes(
        [
            _Ent(id=node_id, name="A", belongs_to_set=["Dev"]),
            _Ent(id=node_id, name="A", belongs_to_set=["Prod"]),
        ]
    )

    node = await adapter.get_node(str(node_id))
    assert sorted(node["belongs_to_set"]) == ["Dev", "Prod"]


# ----------------------------------------------------------------------
# apoc.merge.relationship — the 5th APOC site
# ----------------------------------------------------------------------


async def test_edges_of_different_types_are_grouped_not_collapsed(adapter):
    """The type cannot be parameterized, so the batch is grouped by type in
    Python — a bug here would give every edge in a batch the first one's type."""
    a, b = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B")
    await adapter.add_nodes([a, b])
    await adapter.add_edges(
        [
            (str(a.id), str(b.id), "knows", {"edge_text": "t"}),
            (str(a.id), str(b.id), "trusts", {"edge_text": "t2"}),
        ]
    )

    rows = await adapter.query("MATCH ()-[r]->() RETURN type(r) AS rel")
    assert sorted(row["rel"] for row in rows) == ["knows", "trusts"]


async def test_a_relationship_name_that_is_not_an_identifier_round_trips(adapter):
    """Extraction emits names with spaces and punctuation; the stored type must
    still match the name provenance lookups address it by."""
    a, b = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B")
    await adapter.add_nodes([a, b])
    await adapter.add_edges([(str(a.id), str(b.id), "is related to", {"edge_text": "t"})])

    edge = EdgeIdentity(str(a.id), str(b.id), "is related to")
    assert await adapter.has_edge(str(a.id), str(b.id), "is related to") is True
    assert edge in await adapter.get_edge_delete_data([edge])


async def test_re_adding_an_edge_updates_it_and_keeps_its_first_created_at(adapter):
    """Properties are SET after the MERGE (not ON CREATE) so a re-cognify updates."""
    a, b = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B")
    await adapter.add_nodes([a, b])
    await adapter.add_edges([(str(a.id), str(b.id), "knows", {"edge_text": "first"})])
    first = (await adapter.query("MATCH ()-[r]->() RETURN properties(r) AS p"))[0]["p"]

    await adapter.add_edges([(str(a.id), str(b.id), "knows", {"edge_text": "second"})])
    second = (await adapter.query("MATCH ()-[r]->() RETURN properties(r) AS p"))[0]["p"]

    assert second["edge_text"] == "second"
    assert second["created_at"] == first["created_at"]


async def test_has_edges_returns_the_subset_that_exists(adapter):
    """⚠ The seed returns bools and matches on Neo4j-internal ids; the interface
    and ladybug both return the existing edges."""
    a, b = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B")
    await adapter.add_nodes([a, b])
    await adapter.add_edges([(str(a.id), str(b.id), "knows", {})])

    present = (str(a.id), str(b.id), "knows", {})
    absent = (str(b.id), str(a.id), "knows", {})
    assert await adapter.has_edges([present, absent]) == [present]


# ----------------------------------------------------------------------
# GDS is not ported — get_graph_metrics computes what it honestly can
# ----------------------------------------------------------------------


async def test_metrics_count_components_without_gds(adapter):
    a, b, island = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B"), _Ent(id=uuid4(), name="C")
    await adapter.add_nodes([a, b, island])
    await adapter.add_edges([(str(a.id), str(b.id), "knows", {})])

    metrics = await adapter.get_graph_metrics()
    assert metrics["num_nodes"] == 3
    assert metrics["num_edges"] == 1
    assert metrics["num_connected_components"] == 2
    assert metrics["sizes_of_connected_components"] == [2, 1]


async def test_all_pairs_metrics_are_reported_as_not_computed(adapter):
    """⚠ ``-1`` means unavailable, not measured. Nothing computes an all-pairs
    metric over 59k nodes, on any backend."""
    metrics = await adapter.get_graph_metrics(include_optional=True)
    assert metrics["diameter"] == -1
    assert metrics["avg_shortest_path_length"] == -1
    assert metrics["avg_clustering"] == -1
    assert metrics["num_selfloops"] == 0  # this one IS computed when asked


# ----------------------------------------------------------------------
# Truth state — ported from ladybug, the only in-core adapter that has it
# ----------------------------------------------------------------------


async def test_truth_state_round_trips(adapter):
    node = _Ent(id=uuid4(), name="A")
    await adapter.add_nodes([node])
    node_id = str(node.id)

    assert await adapter.set_node_truth_state(
        {node_id: {"truth_alignment": ["supported"], "truth_epoch": 3}}
    ) == {node_id: True}
    assert await adapter.get_node_truth_state([node_id]) == {
        node_id: {"truth_alignment": ["supported"], "truth_epoch": 3}
    }


async def test_truth_state_defaults_for_a_node_that_has_none(adapter):
    node = _Ent(id=uuid4(), name="A")
    await adapter.add_nodes([node])

    assert await adapter.get_node_truth_state([str(node.id)]) == {
        str(node.id): {"truth_alignment": [], "truth_epoch": None}
    }


async def test_truth_state_without_an_epoch_leaves_the_stored_one_alone(adapter):
    """⚠ Writing ``null`` would be accepted and silently dropped, which reads as
    a successful clear while the old epoch survives. The epoch-less write is a
    separate statement that does not touch the property at all."""
    node = _Ent(id=uuid4(), name="A")
    await adapter.add_nodes([node])
    node_id = str(node.id)

    await adapter.set_node_truth_state({node_id: {"truth_alignment": ["a"], "truth_epoch": 7}})
    await adapter.set_node_truth_state({node_id: {"truth_alignment": ["b"]}})

    assert await adapter.get_node_truth_state([node_id]) == {
        node_id: {"truth_alignment": ["b"], "truth_epoch": 7}
    }


async def test_truth_state_reports_a_missing_node_as_not_updated(adapter):
    assert await adapter.set_node_truth_state({"ghost": {"truth_alignment": []}}) == {
        "ghost": False
    }


# ----------------------------------------------------------------------
# Whole-graph reads and delete
# ----------------------------------------------------------------------


async def test_query_returns_rows_keyed_by_column_name(adapter):
    """FalkorDB returns positional rows; the ported bodies (and cognee's Cypher
    retrievers) expect the Neo4j driver's dict shape."""
    node = _Ent(id=uuid4(), name="Germany")
    await adapter.add_nodes([node])

    rows = await adapter.query(
        f"MATCH (n:`{BASE_LABEL}`) RETURN n.id AS id, n.name AS name"
    )
    assert rows == [{"id": str(node.id), "name": "Germany"}]


async def test_query_unwraps_a_returned_node_to_its_properties(adapter):
    await adapter.add_nodes([_Ent(id=uuid4(), name="Germany")])

    rows = await adapter.query(f"MATCH (n:`{BASE_LABEL}`) RETURN n")
    assert rows[0]["n"]["name"] == "Germany"


async def test_graph_data_falls_back_to_matched_endpoints(adapter):
    """⚠ The seed reads ``source_node_id`` off the edge and raises without it —
    which is every edge the Stage D bulk loader writes."""
    a, b = _Ent(id=uuid4(), name="A"), _Ent(id=uuid4(), name="B")
    await adapter.add_nodes([a, b])
    await adapter.query(
        f"""
        MATCH (x:`{BASE_LABEL}` {{id: $a}}), (y:`{BASE_LABEL}` {{id: $b}})
        CREATE (x)-[:knows]->(y)
        """,
        {"a": str(a.id), "b": str(b.id)},
    )

    nodes, edges = await adapter.get_graph_data()
    assert len(nodes) == 2
    assert edges == [(str(a.id), str(b.id), "knows", {})]


async def test_graph_data_never_returns_the_metadata_marker(adapter):
    """The marker carries no shared label, which is also what keeps is_empty true."""
    await adapter.set_graph_metadata({"graph_delete_mode": "graph_provenance"})

    nodes, _ = await adapter.get_graph_data()
    assert nodes == []
    assert await adapter.is_empty() is True


async def test_delete_graph_empties_the_graph_but_keeps_the_indexes(adapter):
    """🚨 ``GRAPH.DELETE`` would drop the id indexes with the data, and nothing on
    the write path recreates them — the silent All-Node-Scan regression again."""
    await adapter.add_nodes([_Ent(id=uuid4(), name="A")])
    await adapter.delete_graph()

    assert await adapter.is_empty() is True
    indexed = {row["label"] for row in await adapter.query("CALL db.indexes()")}
    assert BASE_LABEL in indexed


async def test_filtered_graph_data_parameterizes_its_values(adapter):
    """⚠ The seed interpolates values into the query text with hand-rolled
    quoting; one apostrophe in an entity name is a syntax error."""
    await adapter.add_nodes([_Ent(id=uuid4(), name="O'Brien")])

    nodes, _ = await adapter.get_filtered_graph_data([{"name": ["O'Brien"]}])
    assert [properties["name"] for _, properties in nodes] == ["O'Brien"]


async def test_remove_belongs_to_set_tags_also_prunes_the_stale_nodeset_edge(adapter):
    """The contract suite checks the property; the edge half is ours to prove.

    A detagged node keeps an edge to a NodeSet that survives the delete, and a
    graph whose property and edges disagree is worse than either alone.
    """
    node = _Ent(id=uuid4(), name="Shared", belongs_to_set=["Dev"])
    await adapter.add_nodes([node])
    await adapter.query(
        f"""
        MATCH (n:`{BASE_LABEL}` {{id: $id}})
        MERGE (ns:`{BASE_LABEL}`:NodeSet {{id: 'ns-dev', name: 'Dev'}})
        MERGE (n)-[:belongs_to_set]->(ns)
        """,
        {"id": str(node.id)},
    )

    await adapter.remove_belongs_to_set_tags(["Dev"])

    assert (await adapter.get_node(str(node.id)))["belongs_to_set"] == []
    assert await adapter.query("MATCH ()-[r:belongs_to_set]->() RETURN r") == []


# ----------------------------------------------------------------------
# The ported read surface — rewritten from the seed, so each shape is proven
# ----------------------------------------------------------------------


@pytest.fixture
async def triangle(adapter):
    """a -knows-> b -likes-> c, plus an unconnected d."""
    nodes = [_Ent(id=uuid4(), name=name) for name in ("a", "b", "c", "d")]
    await adapter.add_nodes(nodes)
    a, b, c, _ = (str(node.id) for node in nodes)
    await adapter.add_edges(
        [(a, b, "knows", {"edge_text": "ab"}), (b, c, "likes", {"edge_text": "bc"})]
    )
    return {node.name: str(node.id) for node in nodes}


async def test_get_neighbors_is_undirected(adapter, triangle):
    names = {node["name"] for node in await adapter.get_neighbors(triangle["b"])}
    assert names == {"a", "c"}


async def test_get_edges_returns_both_directions(adapter, triangle):
    edges = await adapter.get_edges(triangle["b"])
    assert sorted(properties["relationship_name"] for _, _, properties in edges) == [
        "knows",
        "likes",
    ]


async def test_get_connections_orients_each_triple(adapter, triangle):
    triples = await adapter.get_connections(triangle["b"])
    assert sorted((s["name"], r["relationship_name"], t["name"]) for s, r, t in triples) == [
        ("a", "knows", "b"),
        ("b", "likes", "c"),
    ]


async def test_get_neighborhood_respects_depth(adapter, triangle):
    one_hop, _ = await adapter.get_neighborhood([triangle["a"]], depth=1)
    two_hop, edges = await adapter.get_neighborhood([triangle["a"]], depth=2)

    assert {properties["name"] for _, properties in one_hop} == {"a", "b"}
    assert {properties["name"] for _, properties in two_hop} == {"a", "b", "c"}
    assert len(edges) == 2


async def test_get_neighborhood_filters_by_edge_type(adapter, triangle):
    nodes, _ = await adapter.get_neighborhood([triangle["a"]], depth=2, edge_types=["knows"])
    assert {properties["name"] for _, properties in nodes} == {"a", "b"}


async def test_get_nodeset_subgraph_or_takes_every_neighbour(adapter, triangle):
    nodes, edges = await adapter.get_nodeset_subgraph(_Ent, ["a", "c"])

    assert {properties["name"] for _, properties in nodes} == {"a", "b", "c"}
    assert len(edges) == 2


async def test_get_nodeset_subgraph_and_takes_only_shared_neighbours(adapter, triangle):
    """``b`` is adjacent to both named nodes; nothing else is."""
    nodes, _ = await adapter.get_nodeset_subgraph(_Ent, ["a", "c"], node_name_filter_operator="AND")
    assert {properties["name"] for _, properties in nodes} == {"a", "b", "c"}

    nodes, _ = await adapter.get_nodeset_subgraph(_Ent, ["a", "d"], node_name_filter_operator="AND")
    assert {properties["name"] for _, properties in nodes} == {"a", "d"}


async def test_get_nodeset_subgraph_of_an_unknown_name_is_empty(adapter, triangle):
    assert await adapter.get_nodeset_subgraph(_Ent, ["nobody"]) == ([], [])


async def test_get_triplets_batch_pages(adapter, triangle):
    first = await adapter.get_triplets_batch(offset=0, limit=1)
    everything = await adapter.get_triplets_batch(offset=0, limit=10)

    assert len(first) == 1
    assert len(everything) == 2
    assert first[0]["start_node"]["name"] in {"a", "b"}
    assert "edge_text" in first[0]["relationship_properties"]


async def test_add_node_accepts_the_bare_id_form_the_interface_declares(adapter):
    """⚠ Neither in-core adapter accepts it; refusing a declared call narrows."""
    await adapter.add_node("plain-id", {"name": "Plain"})
    assert (await adapter.get_node("plain-id"))["name"] == "Plain"


async def test_delete_node_and_delete_nodes_detach(adapter, triangle):
    await adapter.delete_node(triangle["a"])
    assert await adapter.has_node(triangle["a"]) is False
    assert await adapter.query("MATCH ()-[r:knows]->() RETURN r") == []

    await adapter.delete_nodes([triangle["b"], triangle["c"], "ghost"])
    assert await adapter.get_nodes([triangle["b"], triangle["c"]]) == []
