"""The A2 gate, server-free half: the pure helpers the APOC/GDS port introduced.

Everything here is a property of a function, not of a database, so none of it
needs FalkorDB. The behaviours that DO need a server — labels applied without
``apoc.create.addLabels``, edges merged without ``apoc.merge.relationship`` —
live in ``test_port_delta.py``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.adapter import (
    _connected_components,
    _dedupe,
    _quote,
    _strip_provenance,
)


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


@pytest.fixture
def adapter():
    """An adapter with a stub driver — no server, no I/O, real methods."""
    return FalkorDBAdapter(driver=SimpleNamespace(select_graph=lambda name: None))


# ----------------------------------------------------------------------
# _quote — the only place this adapter interpolates into Cypher
# ----------------------------------------------------------------------


def test_quote_wraps_in_backticks():
    assert _quote("Entity") == "`Entity`"


def test_quote_survives_names_that_are_not_bare_identifiers():
    """Relationship names come from LLM extraction and are arbitrary text."""
    assert _quote("is related to") == "`is related to`"
    assert _quote("работает в") == "`работает в`"
    assert _quote("a-b.c/d") == "`a-b.c/d`"


def test_quote_drops_backticks_because_falkordb_cannot_escape_them():
    """🚨 The injection surface. FalkorDB has no escape for a backtick inside a
    quoted identifier, so it is removed rather than passed through."""
    assert _quote("evil`) MATCH (n) DETACH DELETE n //") == (
        "`evil) MATCH (n) DETACH DELETE n //`"
    )


# ----------------------------------------------------------------------
# _dedupe — the Python half of the apoc.coll.toSet port
# ----------------------------------------------------------------------


def test_dedupe_preserves_first_occurrence_order():
    assert _dedupe(["Dev", "Prod", "Dev", "Mirror"]) == ["Dev", "Prod", "Mirror"]


def test_dedupe_of_empty_is_empty():
    assert _dedupe([]) == []


# ----------------------------------------------------------------------
# _connected_components — the GDS replacement
# ----------------------------------------------------------------------


def test_components_counts_isolated_nodes():
    assert _connected_components(["a", "b", "c"], []) == [1, 1, 1]


def test_components_merges_across_edges_regardless_of_direction():
    sizes = _connected_components(["a", "b", "c", "d"], [("a", "b"), ("c", "b")])
    assert sizes == [3, 1]


def test_components_ignores_edges_to_unknown_nodes():
    """A dangling edge must not invent a node — the count would silently inflate."""
    assert _connected_components(["a"], [("a", "ghost")]) == [1]


def test_components_of_empty_graph_is_empty():
    assert _connected_components([], []) == []


# ----------------------------------------------------------------------
# serialize_properties — what FalkorDB will and will not store
# ----------------------------------------------------------------------


def test_uuid_is_stringified(adapter):
    node_id = uuid4()
    assert adapter.serialize_properties({"id": node_id}) == {"id": str(node_id)}


def test_map_is_json_encoded_under_its_own_key(adapter):
    """🚨 FalkorDB rejects a map property outright, so it must not reach the write.

    Encoded under the SAME key, unlike the Neo4j seed's edge path which renames
    it to ``<key>_json`` — a rename makes the property invisible to anything
    looking it up by name, including ``_indexed_fields_from_properties``.
    """
    out = adapter.serialize_properties({"metadata": {"index_fields": ["name"]}})
    assert json.loads(out["metadata"]) == {"index_fields": ["name"]}


def test_array_of_primitives_is_passed_through(adapter):
    """FalkorDB stores these natively; JSON-encoding them would lose queryability."""
    assert adapter.serialize_properties({"belongs_to_set": ["a", "b"]}) == {
        "belongs_to_set": ["a", "b"]
    }


def test_array_containing_a_map_is_json_encoded(adapter):
    out = adapter.serialize_properties({"items": [{"a": 1}]})
    assert json.loads(out["items"]) == [{"a": 1}]


def test_weights_are_flattened_not_encoded(adapter):
    """cognee's visualization preprocessor reads ``weight_<name>`` scalars."""
    assert adapter.serialize_properties({"weights": {"trust": 0.5}}) == {"weight_trust": 0.5}


def test_none_still_passes_through_until_a3(adapter):
    """⚠ Deliberate, and deliberately asserted: FalkorDB ACCEPTS a null property
    value and then silently DROPS it. Stripping nulls before the write is stage
    A3's gate, so this assertion is what makes that change visible rather than
    incidental."""
    assert adapter.serialize_properties({"nothing": None}) == {"nothing": None}


# ----------------------------------------------------------------------
# _node_payload — every shape the interface's Union allows
# ----------------------------------------------------------------------


def test_payload_from_datapoint_takes_label_from_the_type_field(adapter):
    node = _Ent(id=uuid4(), name="Germany")
    node_id, label, properties = adapter._node_payload(node)

    assert node_id == str(node.id)
    assert label == "_Ent"
    assert properties["name"] == "Germany"
    assert properties["id"] == str(node.id)


def test_payload_from_id_properties_tuple(adapter):
    """The ``Node`` alias the interface names — what a bulk loader holds."""
    node_id, label, properties = adapter._node_payload(("abc", {"name": "x", "type": "Entity"}))

    assert (node_id, label) == ("abc", "Entity")
    assert properties["id"] == "abc"


def test_payload_from_bare_dict_declares_no_type_label(adapter):
    """A payload with no ``type`` gets the shared label only — never ``dict``."""
    node_id, label, properties = adapter._node_payload({"id": "abc", "name": "x"})

    assert (node_id, label) == ("abc", None)
    assert properties["name"] == "x"


def test_payload_dedupes_belongs_to_set(adapter):
    _, _, properties = adapter._node_payload(("abc", {"belongs_to_set": ["Dev", "Dev", "Prod"]}))
    assert properties["belongs_to_set"] == ["Dev", "Prod"]


# ----------------------------------------------------------------------
# _strip_provenance — storage-internal fields must not leak to retrieval
# ----------------------------------------------------------------------


def test_strip_provenance_removes_all_four_fields():
    stripped = _strip_provenance(
        {
            "id": "a",
            "source_ref_keys": ["k"],
            "source_dataset_ids": ["d"],
            "source_run_ids": ["r"],
            "source_run_refs": ["r|k"],
        }
    )
    assert stripped == {"id": "a"}


def test_strip_provenance_returns_a_plain_dict():
    """Callers mutate the result; a FalkorDB OrderedDict view would be shared."""
    source = {"id": "a"}
    stripped = _strip_provenance(source)
    stripped["extra"] = 1
    assert "extra" not in source
