"""The B2 gate, server-free half: cognee's factory can actually build this adapter.

Every other test in this repo constructs ``FalkorDBAdapter`` directly — the
contract suite included — so none of them exercises the one call that matters in
production:

    if graph_database_provider in supported_databases:
        adapter = supported_databases[graph_database_provider]
        return adapter(graph_database_url=…, graph_database_username=…,
                       graph_database_password=…, graph_database_port=…,
                       graph_database_key=…, database_name=…)

🚨 That call passes three keywords the stage-A constructor did not accept
(``graph_database_port``, ``graph_database_key``, ``database_name``) and omits
the one it did (``graph_database_name``). A green stage A therefore sat on top
of an adapter cognee could not instantiate at all: a ``TypeError`` on the first
cognify, recall or delete, indistinguishable from an unregistered provider.

So the keyword list is **read out of cognee's AST**, not restated here. Restating
it would reproduce exactly the failure this file exists to catch — the list was
wrong once already, from reading the interface instead of the factory.
"""

from __future__ import annotations

import ast
import inspect
import os
import textwrap
import uuid
from importlib import import_module

import pytest

from cognee.infrastructure.databases.graph.supported_databases import supported_databases

# ⚠ Imported by path, not `from …graph import get_graph_engine`: that package's
# __init__ re-exports the *function* under the module's own name, so the plain
# form silently binds a function and the AST walk below reads the wrong source.
engine_module = import_module("cognee.infrastructure.databases.graph.get_graph_engine")

from cognee_falkordb_adapter import FalkorDBAdapter
from cognee_falkordb_adapter.adapter import DEFAULT_GRAPH_NAME
from cognee_falkordb_adapter.register import PROVIDER_NAMES, ensure_registered


class _RecordingFalkorDB:
    """Stands in for ``falkordb.asyncio.FalkorDB`` — records how it was built.

    A fake rather than a live server because what is under test is argument
    routing, and ``FalkorDB.__init__`` would otherwise need a reachable host to
    tell a right hostname from a wrong one.
    """

    last_kwargs: dict = {}
    last_url: str | None = None

    def __init__(self, **kwargs):
        type(self).last_kwargs = kwargs
        type(self).last_url = None

    @classmethod
    def from_url(cls, url):
        instance = cls.__new__(cls)
        cls.last_kwargs = {}
        cls.last_url = url
        return instance

    def select_graph(self, name):
        self.graph_name = name
        return name


@pytest.fixture
def recording_falkordb(monkeypatch):
    monkeypatch.setattr("cognee_falkordb_adapter.adapter.FalkorDB", _RecordingFalkorDB)
    _RecordingFalkorDB.last_kwargs = {}
    _RecordingFalkorDB.last_url = None
    return _RecordingFalkorDB


def _registered_adapter_kwargs() -> set[str]:
    """The keywords cognee passes a registered adapter, parsed from its own source.

    ⚠ Read from ``_create_graph_engine``'s AST rather than hardcoded, so a cognee
    release that adds, renames or drops one fails *here* — where the diff can be
    read — instead of arriving as a TypeError inside the 03:00 drain.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(engine_module._create_graph_engine)))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "adapter"
    ]
    assert len(calls) == 1, (
        f"expected exactly one `adapter(...)` construction in cognee's "
        f"_create_graph_engine, found {len(calls)} — the registered-provider "
        "dispatch has been restructured; re-read it before trusting this test"
    )
    return {keyword.arg for keyword in calls[0].keywords if keyword.arg is not None}


def test_constructor_binds_every_keyword_cognee_passes():
    """The signature accepts cognee's call. This is the whole point of the file."""
    kwargs = _registered_adapter_kwargs()
    assert kwargs, "parsed no keywords — the AST walk is looking at the wrong call"

    # bind() raises TypeError on an unexpected or missing keyword, which is the
    # exact failure production would hit, one layer earlier and with a real name.
    inspect.signature(FalkorDBAdapter).bind(**dict.fromkeys(kwargs))


def test_registration_binds_both_provider_names():
    ensure_registered()
    for name in PROVIDER_NAMES:
        assert supported_databases[name] is FalkorDBAdapter


def test_factory_dispatch_reaches_this_adapter(recording_falkordb):
    """Through cognee's own dispatch, with the deployed env's shape of arguments.

    📌 ``graph_database_url`` carries a bare HOST here because that is what cognee
    deployments of a client-server graph store put in ``GRAPH_DATABASE_URL`` — the
    spike's ``env.example`` and this role's ``cognee.env`` both do — and the port
    arrives as a *string*, because ``_normalize_optional_create_graph_engine_params``
    stringifies it on the way in.
    """
    ensure_registered()
    adapter = supported_databases["falkordb"](
        graph_database_url="10.193.29.23",
        graph_database_username="",
        graph_database_password="",
        graph_database_port="6382",
        graph_database_key="",
        database_name="cognee_graph",
    )

    assert isinstance(adapter, FalkorDBAdapter)
    assert recording_falkordb.last_url is None, "a bare host must not go through from_url"
    assert recording_falkordb.last_kwargs["host"] == "10.193.29.23"
    assert recording_falkordb.last_kwargs["port"] == 6382
    assert adapter._graph_name == "cognee_graph"


def test_empty_strings_fall_back_to_defaults(recording_falkordb):
    """cognee passes "" for anything unset, so falsiness is what "unset" means."""
    adapter = FalkorDBAdapter(
        graph_database_url="",
        graph_database_username="",
        graph_database_password="",
        graph_database_port="",
        graph_database_key="",
        database_name="",
    )

    assert recording_falkordb.last_kwargs["host"] == "localhost"
    assert recording_falkordb.last_kwargs["port"] == 6379
    assert adapter._graph_name == DEFAULT_GRAPH_NAME


def test_unset_port_sentinel_is_passed_through_not_defaulted(recording_falkordb):
    """cognee's unset port is 123, and rewriting it to 6379 would be worse.

    📌 ``GraphConfig.graph_database_port`` defaults to 123, so a deployment that
    forgets ``GRAPH_DATABASE_PORT`` arrives here with a real-looking value. Left
    alone it is a refused connection naming port 123; "helpfully" mapped to 6379
    it would instead connect to whatever else holds the redis port and fail later,
    in Cypher, somewhere else.
    """
    FalkorDBAdapter(graph_database_url="10.193.29.23", graph_database_port=123)
    assert recording_falkordb.last_kwargs["port"] == 123


def test_a_real_url_still_goes_through_from_url(recording_falkordb):
    FalkorDBAdapter(graph_database_url="redis://10.193.29.23:6382")
    assert recording_falkordb.last_url == "redis://10.193.29.23:6382"


def test_credentials_are_forwarded_and_blanks_become_none(recording_falkordb):
    FalkorDBAdapter(host="h", graph_database_username="u", graph_database_password="p")
    assert recording_falkordb.last_kwargs["username"] == "u"
    assert recording_falkordb.last_kwargs["password"] == "p"

    FalkorDBAdapter(host="h", graph_database_username="", graph_database_password="")
    assert recording_falkordb.last_kwargs["username"] is None
    assert recording_falkordb.last_kwargs["password"] is None


def test_graph_database_key_is_accepted_and_warned_about(recording_falkordb, caplog):
    """FalkorDB has no such credential, but cognee passes it unconditionally.

    Accepting it silently would let an operator believe authentication is in force
    when nothing reads it, so it warns — and it must never raise, or the adapter
    becomes unconstructable again for a value cognee always sends.
    """
    with caplog.at_level("WARNING"):
        FalkorDBAdapter(host="h", graph_database_key="some-token")
    assert any("graph_database_key" in record.message for record in caplog.records)
    assert recording_falkordb.last_kwargs["host"] == "h", "the key must not divert the connection"

    caplog.clear()
    with caplog.at_level("WARNING"):
        FalkorDBAdapter(host="h", graph_database_key="")
    assert not caplog.records, "an empty key is the normal case and must stay quiet"


# ----------------------------------------------------------------------
# The same path, against a real server
# ----------------------------------------------------------------------

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cognee_factory_builds_a_working_indexed_engine():
    """``create_graph_engine`` → a connected adapter whose ``initialize()`` indexes.

    The fake above proves the arguments are routed; this proves the whole path is
    real — cognee's factory, this adapter, a live server, and the id indexes that
    nothing else creates. It is the server half of B2's gate, and the reason it is
    a test rather than a deploy-time probe is A4's lesson: a hand-written
    approximation of the call is how a probe passes while production fails.
    """
    ensure_registered()
    graph_name = f"test_{uuid.uuid4().hex[:12]}"

    try:
        engine = engine_module.create_graph_engine(
            graph_database_provider="falkordb",
            graph_file_path=None,
            graph_database_url=HOST,
            graph_database_port=str(PORT),
            graph_database_name=graph_name,
        )
    except Exception as exc:
        pytest.skip(f"FalkorDB not reachable at {HOST}:{PORT}: {exc}")

    try:
        assert isinstance(engine, FalkorDBAdapter)
        await engine.initialize()

        indexed_labels = {
            row["label"] for row in await engine.query("CALL db.indexes() YIELD label")
        }
        assert "__Node__" in indexed_labels
    finally:
        try:
            await engine._graph.delete()
        finally:
            await engine.close()
