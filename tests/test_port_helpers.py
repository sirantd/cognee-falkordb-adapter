"""The A2 gate, server-free half: the pure helpers the APOC/GDS port introduced.

Everything here is a property of a function, not of a database, so none of it
needs FalkorDB. The behaviours that DO need a server — labels applied without
``apoc.create.addLabels``, edges merged without ``apoc.merge.relationship`` —
live in ``test_port_delta.py``.

📌 ``serialize_properties`` used to be covered here. Stage A3 moved it to
``test_coercion.py`` along with the rest of the coercion layer, so the store's
rules and the tests asserting them stay in one place.
"""

from __future__ import annotations

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
    quoted identifier, so it is removed rather than passed through.

    (``_quote`` also strips NUL — that is a coercion rule rather than an
    injection one, so it is asserted in ``test_coercion.py``.)
    """
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
