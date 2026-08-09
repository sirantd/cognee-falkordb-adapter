"""The A5 gate: cognee's own graph-provenance contract suite, on FalkorDB.

This is the definition of done for provenance. The suite is cognee's, not ours,
and it is backend-neutral by construction — its own docstring invites exactly
this:

    "Add a provider to ``graph_provenance_adapter`` when implementing graph
    provenance for a new graph backend."

📌 **We add that provider by importing the suite and shadowing one fixture, not
by editing cognee.** The suite ships inside the cognee wheel, so an edited copy
would live in ``site-packages``: unversioned, silently reverted by the next
``pip install``, and invisible in review. The star-import below binds the same
19 test functions into this module, where pytest collects them and resolves
``graph_provenance_adapter`` against the definition here. Adding ``falkordb`` to
cognee's own fixture params is a PR to cognee; this repo's job is to prove the
provider passes.

⚠ **The suite is only a gate while the cognee it came from is pinned.** A
hand-run suite proves today's cognee; a pinned one in CI is what catches the
interface moving. ``pyproject.toml`` pins it exactly and ``test_contract_pin.py``
enforces both that pin and the 19 names, so drift surfaces here as a failing
test instead of as a cognify failure in the 03:00 drain.
"""

from __future__ import annotations

import os
import uuid

import pytest

# The suite itself. Star-import is the mechanism: it is what puts the 19 test
# functions in this module's namespace for pytest to collect, and what lets the
# fixture below shadow the suite's by name.
from cognee.tests.integration.infrastructure.graph.test_graph_provenance_adapter_contract import *  # noqa: F401,F403

from cognee_falkordb_adapter import FalkorDBAdapter

# Overrides the ``pytestmark`` the star-import brought along (asyncio only).
# These need a server, and the marker is what keeps the server-free CI job
# server-free.
pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))

# 🚨 **A skipped gate is a green gate.** With no server reachable the fixture
# skips — right on a laptop, a false pass in CI, where 19 skips read as 19
# passes in the job summary and the one check this whole stage exists for
# quietly stops running. CI sets ``FALKORDB_REQUIRED=1`` so an unreachable
# server fails instead.
#
# 📌 The check hangs off the fixture's own connection rather than a pre-flight
# step because a pre-flight is an approximation of the thing under test, and A4
# already paid for that lesson: a hand-written approximation is how a probe
# passes while production scans.
REQUIRE_SERVER = os.getenv("FALKORDB_REQUIRED", "").strip().lower() in {"1", "true", "yes"}

# What cognee's fixture calls its ``params``. One entry, because the other three
# providers are cognee's to run — read by ``test_contract_pin.py``, which asserts
# the override actually took rather than silently inheriting ladybug/postgres/neo4j.
PROVIDERS = ["falkordb"]


@pytest.fixture(params=PROVIDERS)
async def graph_provenance_adapter(request):
    """Shadows the suite's fixture of the same name; yields a FalkorDB adapter.

    A throwaway graph per case, so every test starts from an empty store the way
    the suite's ladybug (fresh ``tmp_path``) and neo4j (``DETACH DELETE``)
    branches do. ``tmp_path`` is deliberately absent from the signature — the
    suite's fixture takes it only because ladybug needs a file path.
    """
    assert request.param == "falkordb", f"unexpected provider: {request.param}"

    try:
        adapter = FalkorDBAdapter(
            host=HOST, port=PORT, graph_database_name=f"contract_{uuid.uuid4().hex[:12]}"
        )
    except Exception as exc:
        # Constructing is network I/O — FalkorDB.__init__ issues a synchronous
        # INFO — so a failure here is connectivity, not configuration.
        unreachable = f"FalkorDB not reachable at {HOST}:{PORT}: {exc}"
        if REQUIRE_SERVER:
            pytest.fail(unreachable)
        pytest.skip(unreachable)

    # 🚨 Nothing else creates the id indexes and the failure is silent, so the
    # gate would otherwise pass over an All-Node-Scan. cognee calls this when it
    # builds the engine; the fixture stands in for that.
    await adapter.initialize()

    try:
        yield adapter
    finally:
        try:
            await adapter._graph.delete()
        except Exception:
            pass
        await adapter.close()
