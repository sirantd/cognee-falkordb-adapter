"""The A3 gate: the coercion layer, server-free.

Every rule here exists because FalkorDB v4.20.1 was **measured** doing something,
and the measurement is quoted in the test that depends on it. The live half —
proving the store still behaves that way — is ``test_coercion_live.py``; if that
file goes red, the rules in this one are the ones to revisit.

⚠ These are deliberately server-free. Coercion is a property of a function, and
gating it behind infrastructure would mean the cheapest check rarely runs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.adapter import _quote
from cognee_falkordb_adapter.coercion import NUL, coerce_properties, scrub_nul


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


class _RecordingGraph:
    """A graph that records what would be sent instead of sending it."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def query(self, query, params=None):
        self.calls.append((query, params or {}))
        return SimpleNamespace(header=[], result_set=[])


@pytest.fixture
def adapter():
    """An adapter over a recording graph — real methods, no I/O.

    Tests read ``adapter._graph.calls`` to assert on what left Python, which is
    the only place the coercion layer is observable without a server.
    """
    return FalkorDBAdapter(driver=SimpleNamespace(select_graph=lambda name: _RecordingGraph()))


# ----------------------------------------------------------------------
# Values FalkorDB rejects outright: maps, and arrays that contain one
# ----------------------------------------------------------------------


def test_map_is_json_encoded_under_its_own_key(adapter):
    """🚨 ``Property values can only be of primitive types or arrays of primitive
    types`` — a map is a hard ResponseError that kills the whole batch.

    Encoded under the SAME key, unlike the Neo4j seed's edge path which renames it
    to ``<key>_json`` — a rename makes the property invisible to anything looking
    it up by name, including ``_indexed_fields_from_properties``.
    """
    out = adapter.serialize_properties({"metadata": {"index_fields": ["name"]}})
    assert json.loads(out["metadata"]) == {"index_fields": ["name"]}


def test_array_containing_a_map_is_json_encoded(adapter):
    out = adapter.serialize_properties({"items": [{"a": 1}]})
    assert json.loads(out["items"]) == [{"a": 1}]


def test_a_map_nested_deep_inside_an_array_is_still_json_encoded(adapter):
    """The rejection is about the map, not about its depth."""
    out = adapter.serialize_properties({"items": [1, [2, {"a": 1}]]})
    assert json.loads(out["items"]) == [1, [2, {"a": 1}]]


# ----------------------------------------------------------------------
# Values FalkorDB stores natively: encode nothing it can hold
# ----------------------------------------------------------------------


def test_array_of_primitives_is_passed_through(adapter):
    """Native. JSON-encoding it would cost queryability for nothing."""
    assert adapter.serialize_properties({"belongs_to_set": ["a", "b"]}) == {
        "belongs_to_set": ["a", "b"]
    }


@pytest.mark.parametrize(
    "value",
    [[], [0.1, 0.2], [True, False], [[1, 2]], [[["a"]]], [1, ["a"]]],
    ids=["empty", "floats", "bools", "nested", "deep-nested", "heterogeneous"],
)
def test_arrays_without_maps_are_never_encoded(adapter, value):
    """📌 Nested and heterogeneous arrays store and round-trip natively — measured.

    The spike's "arrays of primitives" shorthand undersells this: only a **map**
    forces an encode.
    """
    assert adapter.serialize_properties({"v": value}) == {"v": list(value)}


def test_uuid_is_stringified(adapter):
    node_id = uuid4()
    assert adapter.serialize_properties({"id": node_id}) == {"id": str(node_id)}


def test_uuid_inside_an_array_is_stringified_too(adapter):
    """falkordb-py stringifies an unknown type with a bare ``str()`` — unquoted,
    so a UUID left in a list is a Cypher *syntax* error, not a type error."""
    first, second = uuid4(), uuid4()
    out = adapter.serialize_properties({"ids": [first, second]})
    assert out["ids"] == [str(first), str(second)]


def test_an_unstorable_scalar_falls_back_to_its_string_form(adapter):
    """Same reason: anything falkordb-py does not know becomes an unquoted token."""
    moment = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
    assert adapter.serialize_properties({"at": moment}) == {"at": str(moment)}


def test_bytes_are_decoded_rather_than_repr_ed(adapter):
    """⚠ ``bytes`` looks storable and is not.

    falkordb-py's ``quote_string`` decodes bytes — but never sees them:
    ``stringify_param_value`` tests ``isinstance(value, str)`` first, so bytes
    fall through to ``str(value)`` and are sent as the unquoted token
    ``b'hello'``. Measured: that statement dies with ``Failed to parse query
    parameter``, taking its whole batch. ``str()`` would store the repr, so this
    is the one unknown type worth decoding instead.
    """
    assert adapter.serialize_properties({"v": b"hello"}) == {"v": "hello"}


def test_nul_inside_bytes_is_gone_once_they_are_decoded(adapter):
    """The transport scrub only sees ``str``, so the decode has to happen first."""
    assert adapter.serialize_properties({"v": b"he\x00llo"}) == {"v": "he\x00llo"}
    assert scrub_nul(adapter.serialize_properties({"v": b"he\x00llo"})) == {"v": "hello"}


def test_weights_are_flattened_not_encoded(adapter):
    """cognee's visualization preprocessor reads ``weight_<name>`` scalars."""
    assert adapter.serialize_properties({"weights": {"trust": 0.5}}) == {"weight_trust": 0.5}


# ----------------------------------------------------------------------
# Nulls — the A3 gate, asserted at the layer and never by round-trip
# ----------------------------------------------------------------------


def test_a_null_property_is_dropped_before_the_write():
    """🚨 THE gate, and it must be asserted HERE rather than by writing and
    reading back.

    A round-trip cannot tell the two candidate behaviours apart: on a fresh node
    "we never sent it" and "FalkorDB discarded it" both read back as absent. What
    a round-trip on an EXISTING node shows — and ``test_coercion_live.py``
    asserts — is that FalkorDB treats a null as a **delete**, not as a no-op.
    That is why the key must not leave Python at all.
    """
    assert coerce_properties({"a": 1, "b": None}) == {"a": 1}


def test_the_dropped_key_is_absent_rather_than_falsy():
    """``{"b": None}`` and ``{}`` are different writes: ``SET n += {b: null}``
    removes a stored ``b``, an absent key leaves it alone."""
    assert "b" not in coerce_properties({"b": None})


def test_a_falsy_value_that_is_not_null_survives():
    """Only ``None`` is dropped — 0, "" and [] are values FalkorDB stores."""
    assert coerce_properties({"zero": 0, "empty": "", "none_left": [], "off": False}) == {
        "zero": 0,
        "empty": "",
        "none_left": [],
        "off": False,
    }


def test_nulls_inside_an_array_are_dropped(adapter):
    """🚨 A null inside an array is NOT silently ignored — it is the same hard
    ``Property values can only be of primitive types`` ResponseError as a map,
    and it fails the whole ``UNWIND`` batch, not the one node.

    Dropped rather than JSON-encoding the array around them: encoding would turn
    ``belongs_to_set`` into a string, and ``remove_belongs_to_set_tags`` matches
    it with Cypher ``IN``.
    """
    assert adapter.serialize_properties({"belongs_to_set": ["Dev", None]}) == {
        "belongs_to_set": ["Dev"]
    }


def test_nulls_are_dropped_at_every_depth_of_an_array(adapter):
    assert adapter.serialize_properties({"v": [1, [2, None]]}) == {"v": [1, [2]]}


def test_a_null_inside_a_json_encoded_array_is_preserved(adapter):
    """Once the array is going out as JSON there is nothing to protect it from —
    ``null`` is representable, so dropping it would be gratuitous data loss."""
    out = adapter.serialize_properties({"items": [{"a": 1}, None]})
    assert json.loads(out["items"]) == [{"a": 1}, None]


# ----------------------------------------------------------------------
# NUL — the only character FalkorDB's parameter parser rejects
# ----------------------------------------------------------------------


def test_nul_is_stripped_from_a_string():
    assert scrub_nul("a\x00b") == "ab"


@pytest.mark.parametrize("code", [*range(0x01, 0x20), 0x7F])
def test_every_other_control_character_survives(code):
    """⚠ The spec and the spike's ``graph_io.scrub`` both say FalkorDB rejects C0
    control characters outright. Measured against v4.20.1 / falkordb-py 1.6.2,
    only NUL is rejected — every other C0 and DEL parses, stores and round-trips
    byte-identically, as a value, as a property key, inside an array and as a
    relationship type.

    So this layer removes exactly what breaks and keeps the extraction text
    otherwise intact.
    """
    text = f"a{chr(code)}b"
    assert scrub_nul(text) == text


def test_nul_is_stripped_from_nested_containers_and_from_keys():
    """A property key is parsed by the same parser as its value."""
    assert scrub_nul({"k\x00ey": ["a\x00b", {"deep": "c\x00d"}]}) == {
        "key": ["ab", {"deep": "cd"}]
    }


def test_scrubbing_returns_the_ORIGINAL_object_when_there_is_nothing_to_scrub():
    """Every query's params go through this, including 1,000-node write batches.
    The common path must not rebuild the whole structure."""
    params = {"nodes": [{"properties": {"name": "Germany"}}]}
    assert scrub_nul(params) is params


def test_quote_drops_nul_from_an_identifier():
    """A label or relationship type is interpolated into the query TEXT, and the
    parser truncates it at a NUL just the same."""
    assert _quote(f"is{NUL}related") == "`isrelated`"


# ----------------------------------------------------------------------
# The layers together, still without a server: what actually leaves Python
# ----------------------------------------------------------------------


async def test_a_datapoints_unset_optionals_never_reach_the_write(adapter):
    """🚨 The reason null-stripping is not cosmetic.

    A bare cognee DataPoint dumps **7 null-valued keys** — ``ontology_uri``,
    ``belongs_to_set``, ``source_pipeline``, ``source_task``, ``source_node_set``,
    ``source_user``, ``source_content_hash`` — plus one for every optional the
    subclass adds. Passed through, every re-cognify would DELETE those properties
    on a node another pipeline had filled in.
    """
    await adapter.add_nodes([_Ent(id=uuid4(), name="Germany")])

    _, params = adapter._graph.calls[-1]
    properties = params["nodes"][0]["properties"]
    assert None not in properties.values()
    assert "source_pipeline" not in properties
    assert properties["name"] == "Germany"


async def test_no_nul_reaches_the_store_on_the_write_path(adapter):
    """Extraction does emit control characters; NUL is the one that would make
    FalkorDB reject the whole batch."""
    await adapter.add_nodes([_Ent(id=uuid4(), name="Ger\x00many")])

    _, params = adapter._graph.calls[-1]
    assert params["nodes"][0]["properties"]["name"] == "Germany"


async def test_lookup_keys_are_scrubbed_the_same_way_the_write_was(adapter):
    """Write and read must scrub identically or the lookup silently misses: the
    stored value has no NUL, so neither may the parameter matching it."""
    await adapter.get_node("ab\x00cd")

    _, params = adapter._graph.calls[-1]
    assert params["node_id"] == "abcd"
