# cognee-falkordb-adapter

A provenance-complete FalkorDB graph adapter for [cognee](https://github.com/topoteretes/cognee).

Written for a homelab deployment migrating cognee's knowledge graph off ladybug
(cognee's embedded Kuzu engine) onto FalkorDB. Design, measurements and the
acceptance criteria live in the homelab repo's
`docs/specs/2026-08-09-cognee-falkordb-adapter.md`.

## Why not the community adapter

`cognee-community-hybrid-adapter-falkor` implements only the interface's abstract
surface, binds one class as *both* the graph and vector provider, and has not
had a functional commit since 2026-06-24. It was fine as spike scaffolding and is
not a production adapter.

## Status

**Production — deployed and serving.** The TrueNAS deployment pins this
adapter at commit `e39a703` (0.5.2) under cognee `1.6.1`, deployed 2026-10-05.
`main` gates cognee `1.6.3` with the same adapter code; the deployment moves to
it with the next homelab cognee bump. All 41 methods are implemented
(the burn-down,
`pytest -s`, reads `0/41`), ported from cognee's in-core Neo4j adapter with APOC
replaced, the GDS block dropped, and the two `*_node_truth_state` methods taken
from ladybug. `coercion.py` decides what reaches the store, and every rule in it
is a measurement — see [Coercion](#coercion). Every id lookup is plan-verified
index-backed — see [Indexes](#indexes).

cognee's own provenance contract suite passes **20/20** against a live FalkorDB —
now as a CI gate rather than by hand. See [The contract gate](#the-contract-gate).

## cognee constructs this, and it does not use your keyword names

A registered adapter is built by `_create_graph_engine` as:

```python
adapter(graph_database_url=…, graph_database_username=…, graph_database_password=…,
        graph_database_port=…, graph_database_key=…, database_name=…)
```

🚨 **Stage A shipped green on a constructor that could not bind three of those**
(`graph_database_port`, `graph_database_key`, `database_name`) — every test in the
repo, the contract suite included, constructed the adapter *directly*, so the only
call production ever makes was the one nothing exercised. It would have failed as
a `TypeError` on the first cognify, indistinguishable from an unregistered
provider. `tests/test_factory.py` now reads that keyword list **out of cognee's
own AST** and binds it against the signature, because restating the list by hand
is precisely the mistake it is catching.

Two more things that surprise:

- **`graph_database_url` is a host, not a URL.** `GRAPH_DATABASE_URL=localhost` is
  cognee's own documented form for a client-server graph store. A value carrying a
  scheme still goes through `FalkorDB.from_url`; anything else is a hostname.
- **cognee's unset port is `123`**, not empty (`GraphConfig.graph_database_port`).
  It is passed through rather than rewritten to 6379 — a forgotten
  `GRAPH_DATABASE_PORT` should be a refused connection naming port 123, not a
  successful connection to whatever else holds the redis port.

## The contract gate

The definition of done for provenance is cognee's own backend-neutral suite,
`cognee/tests/integration/infrastructure/graph/test_graph_provenance_adapter_contract.py`,
whose docstring invites a new backend to add itself to the
`graph_provenance_adapter` fixture. `tests/test_contract.py` does that by
**importing the suite and shadowing that one fixture** — not by editing cognee,
which ships the suite inside its wheel where an edit would be unversioned,
invisible in review and wiped by the next `pip install`.

Three things keep the gate from passing vacuously:

- **A skipped gate is a green gate.** The fixture skips when nothing answers on
  `FALKORDB_HOST:FALKORDB_PORT` — correct on a laptop, a false pass in CI, where
  20 skips read as 20 passes. CI sets `FALKORDB_REQUIRED=1` and the same fixture
  fails instead. Verified both ways against a dead port.
- **The suite is only a drift detector while cognee is pinned.** `1.6.3`, exactly,
  and `tests/test_contract_pin.py` asserts the pin is exact, that the installed
  cognee is that pin, and that the suite still holds the same **20 cases by name**
  — so an upgrade fails here first and the diff has to be read, rather than
  arriving as a cognify failure in the 03:00 drain.
- **The shadow has to actually take.** Without the override the suite runs its own
  ladybug/postgres/neo4j params; ladybug is installed, so it would pass green
  having tested a different backend. A test asserts the fixture is ours and the
  20 test functions are cognee's, unmodified.

## Indexes

`initialize()` creates `(id)` range indexes on the shared `__Node__` label and on
each of the six cognee type labels. **Nothing else creates them and the failure is
silent**, so `test_indexes.py` asserts the *execution plan* rather than the index
list. Three things make that assertion worth having:

- It excludes `Node By Label Scan`, not just `All Node Scan`. A label without an
  index still narrows the scan and still walks every node of it — and that is
  precisely what #68's probe let through while reporting success.
- It reads `str(plan)`, never the iterated `ExecutionPlan`. ⚠ Iterating yields
  each operation name **once**, so a two-endpoint MERGE with two
  `Node By Index Scan` siblings iterates as a single entry and a regression on the
  second endpoint is invisible.
- It explains the queries the adapter *actually emits*, params and all, captured
  by wrapping the driver during a real call. A hand-written approximation is how a
  probe passes while production scans.

Verified to fail as intended: with `initialize()` skipped, every shape —
shared-label lookup, type-label lookup and the loader's two-endpoint MERGE —
degrades to `Node By Label Scan`.

### Property indexes for a document prune (0.5.2)

`initialize()` also creates range indexes on `DocumentChunk.document_id` and
`TextSummary.source_chunk_id` (`PROPERTY_INDEXES` in `constants.py`). A document
prune finds the chunks of a document by `document_id`, and the summary of each
chunk by `source_chunk_id`. Without these indexes, each lookup is a
`Node By Label Scan` of every node of that label (#11).

`test_indexes.py` asserts `Node By Index Scan` for the three query shapes of the
homelab prune, and for an `IN $values` lookup on each property index. With the
0.5.1 `initialize()`, all of these tests fail with `Node By Label Scan`.

On a graph that exists, the first `initialize()` after the upgrade adds the two
indexes. `CREATE INDEX` returns at once, and FalkorDB builds the index in the
background (`db.indexes()` shows `UNDER CONSTRUCTION`). On FalkorDB v4.20.7 with
28,800 synthetic nodes for each label, each build took approximately 0.11 s.

## Scope: 41 methods, not 48

`GraphDBInterface` declares 48 public methods. Counted against cognee 1.4.1 and
**not re-counted against the current 1.6.3 pin** — `test_surface.py` is what
would catch a change in the interface, not this table:

| bucket | count | implemented here |
|---|---|---|
| `@abstractmethod` | 21 | yes — required to instantiate |
| default-raising, reached by cognee's runtime | 18 | yes |
| default-raising, reached by nothing | 8 | **no, deliberately** |
| real default (`remove_belongs_to_set_tags`) | 1 | **yes — see below** |
| `has_node` — called by the contract suite, absent from the interface | 1 | yes |

The 8 skipped methods are the feedback/frequency-weight surface. A test asserts
they stay unimplemented, so the omission is a decision rather than an oversight.

⚠ `remove_belongs_to_set_tags` was scoped as "override only if slow". It is not
optional: the interface's default is a no-op and the contract suite asserts the
tags are actually removed, so a backend that inherits it fails
`test_remove_belongs_to_set_tags_scoped_and_unscoped`. Both in-core adapters that
pass the suite implement it, and so does this one — 41 methods, not 40.

### Beyond the interface: per-document provenance lookups (0.4.0)

`find_node_source_refs_by_document(dataset_id, data_id)` and
`find_edge_source_refs_by_document(dataset_id, data_id)` are **not** on
`GraphDBInterface`. cognee 1.5.4's `delete_by_document` fetches the whole dataset
through `find_*_source_refs_by_dataset` and filters to one document in Python; on
a ~136k-node / ~700k-edge dataset one delete took >300 s and pinned FalkorDB
(2026-09-24). The homelab cognee patch calls these instead when present
(`hasattr`), so their result must **equal** by_dataset-then-filter —
`tests/test_source_refs_by_document.py` asserts exactly that against the
unpatched computation. The filter runs in Cypher (`key CONTAINS $data_id`, a
superset) and is made exact in Python by parsing each key.

Measured live (homelab `cognee_graph`, ~136k nodes / ~700k edges, FalkorDB
v4.20.6): the node label scan takes 66 ms; the same scan over every
relationship **timed out at 30 s**. So edges are not scanned. They are the union
of:

1. **anchored** — edges incident to the document's own nodes, seeded by the
   `__Node__.id` index (synthetic 136k/700k: 208 ms vs 7.4 s for the scan).
   From 0.5.1, this read is typed when the nodes have many edges — see
   [typed anchored edge reads](#many-relationship-types-typed-anchored-edge-reads-051);
2. **chunk sweep** — every `DocumentChunk -> DocumentChunk` edge (10.8 ms live).

🚨 **That is complete only under an invariant of cognee's write path: an edge
carrying document D's ref has an endpoint that also carries a D ref, or is
chunk→chunk. Re-verify it on every cognee bump.** Checked against 1.5.4, and
re-checked unchanged at 1.6.1 and 1.6.3:
`add_data_points` (nodes and edges from one model walk, same fold key),
chunk-scoped ownership (a chunk's v2 key goes on its walk's nodes and edges;
produced relationship edges join the chunk's own entities), the global context
index (parent summaries share the edge's key), `consolidate_entities`
(re-pointed edges carry no refs), and the node-id migration (restores refs on
both). The one exception is `create_chunk_associations`: it resolves both chunk
endpoints by collection-wide vector search, so the association edge can carry
D's ref between two *other* documents' chunks — which is what the chunk sweep
covers.

⚠ **Known, tested limitation:** an edge carrying D's ref whose endpoints do not
own D and that is not chunk→chunk is not returned. No 1.5.4, 1.6.1 or 1.6.3 write path
makes one; if a future one does, the edge keeps a stale ref (a leak, never an
over-delete) and `delete_by_dataset` still removes it.

### Beyond the interface: a bounded read (0.5.0)

Two cognee 1.6.1 calls to `get_graph_data()` read every node and edge with all
properties: the GRAPH_COMPLETION search and `_cleanup_orphaned_edge_types`. On
the live graph (~160k nodes / ~704k edges), these two calls caused 79 gunicorn
OOM kills (8 GiB limit) and 3 FalkorDB OOM kills (4 GiB limit). The method below
is not on `GraphDBInterface`, and it replaces the search call. For the cleanup
call, see the note at the end of this section.

**`get_id_filtered_graph_data(target_ids)`** returns the edges that touch
`target_ids` and the endpoints of those edges. The shape is that of
`get_graph_data`, without provenance. `CogneeGraph._get_full_or_id_filtered_graph`
calls it when the adapter class has it; without it, each GRAPH_COMPLETION search
reads the full graph. The semantics are those of cognee 1.6.1's neo4j and ladybug
adapters, and `tests/test_id_filtered_graph_data.py` compares the result with
them. The edge read and the node read both start from the `__Node__.id` index
(`test_indexes.py`).

⚠ The read is edge-driven, as upstream: a target with no edges is not returned.
If no target has an edge, the result is empty, and cognee then falls back to
`get_graph_data()` — the full read. This adapter keeps that upstream behaviour.

⚠ **cognee 1.6.3 adds a third full read.** The TEMPORAL search calls
`get_timestamps_in_range`, and the interface default for it reads
`get_graph_data()`. This adapter has no native version, so a TEMPORAL search
reads the full graph. The homelab deployment routes no default recall to
TEMPORAL.

⚠ **0.5.1 removes `get_existing_edge_retrieval_texts(texts)`.** 0.5.0 added it
for `_cleanup_orphaned_edge_types`: it returned the requested texts that are the
retrieval text of an edge. It scanned all edges two times, with two untyped
patterns joined by UNION. Measured on a copy of the live graph (187,105 nodes,
756,107 edges, 19,944 relationship types): one call took 40 to 55 min, and each
write waited until the call ended. A typed rewrite is not faster. One typed scan
costs approximately 105 ms for each type, also for a type with one edge, so one
pass takes approximately 2,000 s. A pattern with more types costs the same for
each type, and a pattern with more than 255 types gives wrong results (see the
255-type hazard below). Thus the homelab cognee patch makes the EdgeType cleanup
a no-op and does not call this method.

### Many relationship types: typed anchored edge reads (0.5.1)

🚨 cognee writes each relationship name from the LLM as a FalkorDB edge type. A
copy of the live graph (2026-10: 187k nodes, 756k edges) has **19,944 types**.
FalkorDB v4.20.7 finds the edges of an untyped pattern `(a)-[r]-(b)` with one
lookup in the matrix of each type, for each node pair
(`Graph_GetEdgesConnectingNodes` with `GRAPH_NO_RELATION`). Measured on the copy:
approximately 6.4 ms for each pair, in both directions. A typed pattern
`(a)-[r:A|B]-(b)` does one matrix multiplication for each type and each batch of
16 start nodes. Its cost does not increase with the degree of the start nodes.

`get_id_filtered_graph_data` and the anchored read of
`find_edge_source_refs_by_document` start from known node ids. A search target
or a node that a document owns can have many edges: an EntityType has up to
44k `is_a` edges, and a NodeSet has up to 155k `belongs_to_set` edges. These
reads first count the node pairs of the start nodes (a pattern without an edge
variable reads only the adjacency matrix: 2 ms for 900 pairs). Then:

- **Few pairs** (`pairs <= 400 + 70 x ceil(start nodes / 16)`, the measured
  break-even): one untyped query, as in 0.5.0.
- **Many pairs**: one typed query for each group of types from
  `CALL db.meta.stats()`. A type with more than 1000 edges has its own group,
  and the other types are in groups of 255. Each query is short, so writes can
  run between them. The rows are thus not one snapshot of the graph.

🚨 **A typed pattern takes 255 types or fewer — a FalkorDB bug with no error.**
FalkorDB keeps the type count of a pattern in a `uint8_t`
(`EdgeTraverseCtx.n_rels`, set in `EdgeTraverseCtx_New`). Measured on v4.20.7: a
pattern with 256 types matched every edge of the graph, and a pattern with 300
types matched the edges of only 44 types. Code that puts many relationship types
into one pattern must keep each group at 255 types or fewer. `tests/test_id_filtered_graph_data.py` has a hub
with 600 types that fails if a group has more than 255 types.

Measured on a copy of the live graph (FalkorDB v4.20.7 in Docker, 2 CPUs, Mac),
wall time of the full method call. "Write wait" is the longest time that a
one-node write, sent each second during the call, waited for the graph.

| call | start nodes | node pairs | 0.5.0 | 0.5.1 | slowest query (0.5.1) | write wait (0.5.0 / 0.5.1) |
|---|---:|---:|---:|---:|---:|---:|
| `get_id_filtered_graph_data`, 20 entities | 20 | 160 | 1.4 s | 1.2 s (untyped) | 1.1 s | 1.1 s / 0.1 s |
| same, 100 mixed search targets | 100 | 892 | 6.5 s | 5.6 s | 0.6 s | 5.7 s / 0.1 s |
| same, an EntityType with 470 edges | 1 | 446 | 3.1 s | 3.1 s (untyped) | 3.0 s | 2.0 s / 2.4 s |
| same, an EntityType with 44k edges | 1 | 41,481 | 282 s | 19 s | 8.2 s | 266 s / 0.1 s |
| same, a NodeSet with 155k edges | 1 | 151,856 | > 300 s (timeout) | 61 s | 31 s ¹ | 300 s / 0.7 s |
| `find_edge_source_refs_by_document`, document A | 12 | 692 | 4.7 s | 3.0 s | 0.5 s | 4.3 s / 0.2 s |
| same, document B | 12 | 16,928 | 103 s | 3.0 s | 0.5 s | 102 s / 0.1 s |
| same, document C | 18 | 158,699 | > 300 s (timeout) | 4.2 s | 0.6 s | 300 s / 0.0 s |

¹ The reply has 155k edges and then 152k nodes with all properties. The time is
mostly the Python parse of the reply; the method must return all of them.

Where 0.5.0 finished, both versions returned the same edges. With the production
`TIMEOUT_MAX` of 300 s, the two 0.5.0 calls marked "timeout" fail.

⚠ **A full edge scan cannot be made fast this way.** A typed scan over all nodes
costs approximately 105 ms for each type, also for a type with one edge
(187k nodes / 16 = 11.7k matrix multiplications). For the 19,104 types that have
edges, one pass takes approximately 2,000 s; the untyped full read took 2,447 s.
Thus these reads keep their untyped patterns:

- full scans: `get_graph_data`, `get_triplets_batch`, `get_graph_metrics`,
  `get_filtered_graph_data`, `find_edges_by_source_ref`,
  `find_edge_source_refs_by_dataset`, `find_edge_source_refs_by_pipeline_run`,
  and the chunk sweep of `find_edge_source_refs_by_document` (it costs per
  chunk-to-chunk pair; the copy has none);
- reads from one node or from a known edge: `get_edges`, `get_connections`,
  `get_neighborhood`, `get_nodeset_subgraph`, `has_edge`, `has_edges`,
  `get_edge_delete_data`, `delete_edge_triples`, and the provenance read and
  write of `attach_edge_source_refs` / `remove_edge_source_refs`. The last six
  know the type of each edge, but they match it with `type(r) = e.rel` on an
  untyped pattern, so each edge costs one lookup for each type. Measured:
  `get_edge_delete_data` for 200 `contains` edges takes 5.5 s; the same read
  with a typed pattern takes 0.03 s.

## Three things that will bite

**Keep `falkordb >= 1.7.0`.** falkordb-py *below* that calls `Is_Cluster()` in its
async `FalkorDB.__init__`, which copies the *async* pool's `connection_kwargs`
into a *sync* `redis.Redis(**kwargs)`. At redis 8.1.0 those carry
`himport_registry`, which sync Redis rejects — in the constructor, before any
connection, so nothing works at all. The sync client is unaffected, which is how
this gets missed.

This used to read "pin `redis < 8.1.0`", and that was the right pin until
falkordb-py 1.7.0 fixed it at the source by filtering `connection_kwargs` through
the sync constructor's signature. The real constraint was always
`falkordb >= 1.7.0 OR redis < 8.1`, so the floor is the half worth pinning — it is
where the fix lives. Drop below 1.7.0 and the trap is silently re-armed, which is
why CI now *constructs the client* rather than asserting a redis version range.

**Nothing else creates the id indexes, and the failure is silent.** An unindexed
id lookup degrades to a full scan with no error — measured at 9.6 ms versus
2.0 ms per lookup, and the difference between an 84.5 s bulk load and a 2,114 s
one. `initialize()` creates them; call it. See [Indexes](#indexes).

**A `null` property value is a DELETE.** Not a no-op, and not a silent drop —
FalkorDB follows Neo4j here, so `SET n += {k: null}` removes a stored `k`. A bare
cognee DataPoint dumps 7 null-valued keys, so passing them through would make
every re-cognify strip properties another pipeline had filled in. The coercion
layer drops the key instead; there is deliberately no "clear this property" path.

## Coercion

`coercion.py` has two jobs, separated because they fail differently: what the
**store** can hold (`coerce_properties`, reached via `serialize_properties`) and
what the **parameter parser** can carry (`scrub_nul`, applied to every query's
params). Measured against FalkorDB v4.20.1 / falkordb-py 1.6.2:

| input | behaviour | rule |
|---|---|---|
| `None` property value | **deletes** the stored property | drop the key |
| `None` inside an array | `ResponseError` — fails the whole `UNWIND` batch | drop the entry |
| map, at any depth inside a value | `ResponseError` | JSON-encode |
| nested / heterogeneous array of primitives | stored natively, exact round-trip | pass through |
| `UUID`, `bytes`, or any other unknown type | stringified *unquoted* by falkordb-py → `Failed to parse query parameter` | `str()`, `decode()` for bytes |
| `\x00` in a value, key, array item or identifier | `Failed to parse query parameter` | strip |
| every other C0, and DEL | accepted, round-trips byte-identically | keep |

⚠ The last two rows correct the spec this was built from, which said FalkorDB
rejects C0 control characters outright. Only NUL is rejected. The spike's
`graph_io.scrub` strips all of C0 — a superset, so a graph it migrated stays
readable, but there is no reason to lose the rest of the extraction text.

## Testing

```bash
pip install -e '.[test]'
pytest -s -m "not integration"           # surface, signatures, port helpers, the pin — no server
docker run -d --rm -p 6379:6379 falkordb/falkordb:v4.22.0    # the version CI uses
pytest -m integration                    # port delta, indexes and the contract suite
FALKORDB_REQUIRED=1 pytest tests/test_contract.py -v   # the gate, as CI runs it
```

`FALKORDB_REQUIRED=1` turns "no server" from a skip into a failure. Use it
whenever a green run is meant to be evidence — see
[The contract gate](#the-contract-gate).
