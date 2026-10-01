"""``get_existing_edge_retrieval_texts`` equals the planner's full-graph computation.

cognee 1.6.1's ``_cleanup_orphaned_edge_types`` reads ``get_graph_data()`` and
applies ``get_edge_retrieval_text(properties.get("edge_text"), edge[2])`` to each
edge, only to learn which deleted edge texts are still in use. The homelab core
patch calls the adapter method instead, so the only correct result is:

    {text for text in texts if text in {retrieval text of each get_graph_data edge}}

``_reference`` below restates that with cognee's own function, and every case
compares against it rather than against hand-written expectations.

The fixture holds the shapes where a server-side computation can differ from
Python: whitespace that FalkorDB's ``trim()`` does not remove, a blank or absent
edge_text, whitespace in the relationship type, edge_text values that are not
strings, quotes and backslashes, and edges outside the shared label.

Needs a live FalkorDB (``FALKORDB_HOST`` / ``FALKORDB_PORT``).
"""

from __future__ import annotations

import math
import os
import uuid
from uuid import uuid4

import pytest

from cognee.infrastructure.engine import DataPoint
from cognee.modules.graph.utils.prepare_edges_for_storage import get_edge_retrieval_text

import cognee_falkordb_adapter.adapter as adapter_module
from cognee_falkordb_adapter import BASE_LABEL, FalkorDBAdapter

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))
REQUIRE_SERVER = os.getenv("FALKORDB_REQUIRED", "").strip().lower() in {"1", "true", "yes"}

# Every character ``str.strip()`` removes. FalkorDB's ``trim()`` removes only the
# first of these, which is why the method must not use it.
PYTHON_WHITESPACE = [
    " ",
    *(
        chr(code_point)
        for code_point in range(0x110000)
        if chr(code_point).isspace() and code_point != 0x20
    ),
]


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


@pytest.fixture
async def adapter():
    """A throwaway graph per test, with the id indexes the adapter expects."""
    try:
        instance = FalkorDBAdapter(
            host=HOST, port=PORT, graph_database_name=f"edgetext_{uuid.uuid4().hex[:12]}"
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


async def _reference(adapter, texts):
    """The planner's computation over ``get_graph_data``, with cognee's own function."""
    _nodes, edges = await adapter.get_graph_data()
    remaining = {
        get_edge_retrieval_text((edge[3] or {}).get("edge_text"), edge[2]) for edge in edges
    }
    return {text for text in texts if text in remaining}


async def _nodes(adapter, count):
    nodes = [_Ent(id=uuid4(), name=f"n{index}") for index in range(count)]
    await adapter.add_nodes(nodes)
    return [str(node.id) for node in nodes]


# (edge_text, relationship type). ``None`` means no edge_text property at all.
EDGES = [
    ("Alice knows Bob", "knows"),
    ("Alice knows Bob", "knows_again"),  # two edges, one retrieval text
    ("  Alice fears the Queen\n", "fears"),
    ("\N{IDEOGRAPHIC SPACE}Hatter hosts Alice\N{NO-BREAK SPACE}", "hosts"),
    ("", "likes"),
    (" ", "visits"),
    ("\t\n\N{LINE SEPARATOR}", "chases"),
    (None, "follows"),
    (None, " padded type "),
    ("\N{EM SPACE}", "\tpadded tab\N{NARROW NO-BREAK SPACE}"),
    ("Dodo races", " races "),  # a non-blank edge_text wins over the type
    ('She said "hi" \\ back', "says"),
    ("\N{LEFT SINGLE QUOTATION MARK}quoted\N{RIGHT SINGLE QUOTATION MARK}", "quotes"),
    ("Аліса знає Боба", "знає"),
    ("Entity number 7 works with entity", "works_with"),
    (42, "counts"),
    (-7, "counts_down"),
    (True, "is_true"),
    (1.5, "weighs"),
    (["a", "b"], "lists"),
]

PROBES = [
    # Live retrieval texts, as the planner computes them.
    "Alice knows Bob",
    "Alice fears the Queen",
    "Hatter hosts Alice",
    "likes",
    "visits",
    "chases",
    "follows",
    "padded type",
    "padded tab",
    "Dodo races",
    'She said "hi" \\ back',
    "\N{LEFT SINGLE QUOTATION MARK}quoted\N{RIGHT SINGLE QUOTATION MARK}",
    "Аліса знає Боба",
    "42",
    "-7",
    "True",
    "1.5",
    "['a', 'b']",
    # Not retrieval texts: a type whose edge has a non-blank edge_text, raw
    # padded values, server spellings of non-strings, and absent texts.
    "knows",
    "races",
    "fears",
    "  Alice fears the Queen\n",
    " ",
    "",
    "true",
    "1.500000",
    "Nobody knows",
    "Alice knows Bob\x00",
    "Entity number 7 works with entit",
    # Edges outside the shared label.
    "Outside only",
    "outside",
    "Half outside",
]


@pytest.fixture
async def world(adapter):
    """Every shape in ``EDGES`` on the shared label, plus edges outside it."""
    ids = await _nodes(adapter, len(EDGES) + 1)
    for index, (edge_text, relationship) in enumerate(EDGES):
        properties = {} if edge_text is None else {"edge_text": edge_text}
        await adapter.add_edges([(ids[index], ids[index + 1], relationship, properties)])

    # Outside the scope of get_graph_data: neither endpoint, or one endpoint, is
    # a data node. "Alice knows Bob" is also live inside, so it stays found.
    await adapter.query(
        """
        CREATE (:Foreign {id: 'f1'})-[:outside {edge_text: 'Outside only'}]->
               (:Foreign {id: 'f2'})
        CREATE (:Foreign {id: 'f3'})-[:outside {edge_text: 'Alice knows Bob'}]->
               (:Foreign {id: 'f4'})
        """
    )
    await adapter.query(
        f"""
        MATCH (n:`{BASE_LABEL}` {{id: $id}})
        CREATE (:Foreign {{id: 'f5'}})-[:half {{edge_text: 'Half outside'}}]->(n)
        """,
        {"id": ids[0]},
    )
    return ids


async def test_result_equals_the_get_graph_data_reference(adapter, world):
    found = await adapter.get_existing_edge_retrieval_texts(PROBES)

    assert found == await _reference(adapter, PROBES)
    # The reference must not be vacuous: it holds both kinds of text.
    assert {"Alice fears the Queen", "Hatter hosts Alice", "padded tab", "1.5"} <= found
    assert not {"knows", "  Alice fears the Queen\n", "Outside only", "Half outside"} & found


async def test_whitespace_is_every_character_str_strip_removes(adapter):
    """One edge pair per whitespace character: padded text, and whitespace-only text.

    🚨 FalkorDB's ``trim()`` removes only U+0020. Each of the other 28 characters
    is a case where a ``trim()`` implementation returns a text cognee never makes.
    """
    ids = await _nodes(adapter, 2 * len(PYTHON_WHITESPACE) + 1)
    probes = []
    for index, char in enumerate(PYTHON_WHITESPACE):
        source, middle, target = ids[2 * index], ids[2 * index + 1], ids[2 * index + 2]
        padded = (source, middle, f"pad_{index}", {"edge_text": f"{char}core {index}{char}"})
        blank = (middle, target, f"blank_{index}", {"edge_text": char * 2})
        await adapter.add_edges([padded, blank])
        probes += [f"core {index}", f"blank_{index}", f"{char}core {index}{char}", char * 2]

    found = await adapter.get_existing_edge_retrieval_texts(probes)

    assert found == await _reference(adapter, probes)
    assert found == {f"core {index}" for index in range(len(PYTHON_WHITESPACE))} | {
        f"blank_{index}" for index in range(len(PYTHON_WHITESPACE))
    }


async def test_empty_input_sends_no_query(adapter, world):
    sent = []
    original = adapter._graph.query

    async def recording(query, params=None, *args, **kwargs):
        sent.append(query)
        return await original(query, params, *args, **kwargs)

    adapter._graph.query = recording
    try:
        assert await adapter.get_existing_edge_retrieval_texts([]) == set()
        assert await adapter.get_existing_edge_retrieval_texts(set()) == set()
        # No retrieval text has whitespace at a boundary, so nothing is sent.
        assert await adapter.get_existing_edge_retrieval_texts([" x", "y\n"]) == set()
    finally:
        adapter._graph.query = original
    assert sent == []


async def test_reply_has_at_most_one_row_per_requested_text(adapter, world):
    """The server sends matched texts, not edges. The only other rows are the
    raw values of non-string, non-integer edge_texts — three in this fixture."""
    replies = []
    original = adapter.query

    async def recording(query, params=None):
        rows = await original(query, params)
        replies.append(rows)
        return rows

    adapter.query = recording
    try:
        await adapter.get_existing_edge_retrieval_texts(PROBES)
    finally:
        adapter.query = original

    (rows,) = replies
    texts = [row["text"] for row in rows if row["exotic"] is None]
    raw = [row["exotic"] for row in rows if row["exotic"] is not None]
    assert len(texts) == len(set(texts)) <= len(set(PROBES))
    assert sorted(map(str, raw)) == sorted(["True", "1.5", "['a', 'b']"])


async def test_batches_give_the_same_result(adapter, world, monkeypatch):
    """Small batches and few buckets: the same set, one query per batch."""
    monkeypatch.setattr(adapter_module, "_EDGE_TEXT_BATCH", 4)
    monkeypatch.setattr(adapter_module, "_EDGE_TEXT_BUCKETS", 3)
    sent = []
    original = adapter._graph.query

    async def recording(query, params=None, *args, **kwargs):
        sent.append(query)
        return await original(query, params, *args, **kwargs)

    adapter._graph.query = recording
    try:
        found = await adapter.get_existing_edge_retrieval_texts(PROBES)
    finally:
        adapter._graph.query = original

    sendable = {text for text in PROBES if text == text.strip()}
    assert len(sent) == math.ceil(len(sendable) / 4)
    assert found == await _reference(adapter, PROBES)


async def test_an_empty_graph_has_no_texts(adapter):
    assert await adapter.get_existing_edge_retrieval_texts(["anything", "knows"]) == set()
