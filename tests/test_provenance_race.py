"""The fold-vs-attach lost-update race, as an adapter-owned gate.

📌 **Why this file still exists.** It was written under the cognee 1.5.2 pin,
where CI could not yet see the upstream case that catches this bug. The pin is
now 1.5.4, so ``test_contract.py`` runs
``test_concurrent_folded_writes_and_attaches_keep_every_owner`` and *that* is the
authority for the node path.

What remains here is the **port delta**: the upstream case covers nodes only, and
``add_edges`` folds into its MERGE exactly as ``add_nodes`` does. The edge test
below is coverage cognee's suite does not give us. The node test is kept as the
deliberate overlap — it is what would localize a regression to this adapter
rather than to a cognee bump, since the two run against different code paths in
the same store.

**The race.** ``add_nodes(..., source_ref_key=...)`` folds the owner key into
the MERGE — one statement. ``attach_node_source_refs`` is a read-then-write
pair. A fold committing between the attach's read and its write is overwritten,
and the owner it stamped is gone: the classic lost update. Two documents of one
cognify run sharing an entity is exactly that interleave.

Needs a live FalkorDB (``FALKORDB_HOST`` / ``FALKORDB_PORT``).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from uuid import uuid4

import pytest

from cognee.infrastructure.databases.provenance import EdgeIdentity, make_source_ref_key
from cognee.infrastructure.engine import DataPoint

from cognee_falkordb_adapter import FalkorDBAdapter

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOST = os.getenv("FALKORDB_HOST", "127.0.0.1")
PORT = int(os.getenv("FALKORDB_PORT", "6379"))

# Same reasoning as ``test_contract.py``: a skipped gate is a green gate, and a
# race test that silently stops running is worse than one that never existed.
REQUIRE_SERVER = os.getenv("FALKORDB_REQUIRED", "").strip().lower() in {"1", "true", "yes"}

# Interleavings per writer. The loss is probabilistic per round — one round is
# flaky, and a flaky gate for a data-loss bug is not a gate. Measured on the
# unfixed adapter: 6 rounds x 2 writers lost keys on every one of 10 runs.
ROUNDS = 6


class _Ent(DataPoint):
    name: str
    metadata: dict = {"index_fields": ["name"]}


@pytest.fixture
async def adapter():
    """A throwaway graph per test, with the id indexes the adapter expects."""
    try:
        instance = FalkorDBAdapter(
            host=HOST, port=PORT, graph_database_name=f"race_{uuid.uuid4().hex[:12]}"
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


def _owner_keys(dataset_id, count):
    """``count`` distinct owner keys in one dataset — one per owning data item.

    📌 Still ``make_source_ref_key`` (dataset/data) rather than 1.5.4's
    chunk-scoped ``make_chunk_source_ref_key``, now by choice rather than by
    necessity: the race is about *how* owner keys are written, not how finely
    they are scoped, and the coarser builder exists in both versions — so this
    gate keeps working either side of a pin move.
    """
    return [make_source_ref_key(dataset_id, uuid4()) for _ in range(count)]


async def test_concurrent_folded_and_attached_node_writes_keep_every_owner(adapter):
    """Two documents folding and attaching against one shared entity lose nothing."""
    shared = _Ent(id=uuid4(), name="Alice")
    node_id = str(shared.id)
    dataset_id = uuid4()

    folded_a, attached_a = _owner_keys(dataset_id, ROUNDS), _owner_keys(dataset_id, ROUNDS)
    folded_b, attached_b = _owner_keys(dataset_id, ROUNDS), _owner_keys(dataset_id, ROUNDS)

    async def writer(folded, attached):
        for fold_key, attach_key in zip(folded, attached):
            await adapter.add_nodes([shared], source_ref_key=fold_key)
            await adapter.attach_node_source_refs([node_id], [attach_key])

    await asyncio.gather(writer(folded_a, attached_a), writer(folded_b, attached_b))

    snapshot = (await adapter.get_node_delete_data([node_id]))[node_id]
    expected = set(folded_a + attached_a + folded_b + attached_b)
    missing = expected - set(snapshot.source_ref_keys)
    assert not missing, f"{len(missing)} of {len(expected)} owner key(s) lost: {sorted(missing)[:3]}"


async def test_concurrent_folded_and_attached_edge_writes_keep_every_owner(adapter):
    """The edge half of the same race — not covered by cognee's own case.

    ``add_edges`` folds into its MERGE exactly as ``add_nodes`` does, and
    ``attach_edge_source_refs`` is the same read-then-write pair, so the edge
    path has the identical lost-update window.
    """
    left, right = _Ent(id=uuid4(), name="Alice"), _Ent(id=uuid4(), name="Rabbit")
    await adapter.add_nodes([left, right])

    edge = (str(left.id), str(right.id), "knows", {"relationship_name": "knows"})
    identity = EdgeIdentity(
        source_id=str(left.id), target_id=str(right.id), relationship_name="knows"
    )
    dataset_id = uuid4()

    folded_a, attached_a = _owner_keys(dataset_id, ROUNDS), _owner_keys(dataset_id, ROUNDS)
    folded_b, attached_b = _owner_keys(dataset_id, ROUNDS), _owner_keys(dataset_id, ROUNDS)

    async def writer(folded, attached):
        for fold_key, attach_key in zip(folded, attached):
            await adapter.add_edges([edge], source_ref_key=fold_key)
            await adapter.attach_edge_source_refs([identity], [attach_key])

    await asyncio.gather(writer(folded_a, attached_a), writer(folded_b, attached_b))

    snapshot = (await adapter.get_edge_delete_data([identity]))[identity]
    expected = set(folded_a + attached_a + folded_b + attached_b)
    missing = expected - set(snapshot.source_ref_keys)
    assert not missing, f"{len(missing)} of {len(expected)} owner key(s) lost: {sorted(missing)[:3]}"
