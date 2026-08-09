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

**Stage A4 — the adapter is code-complete.** All 41 methods are implemented (the
burn-down, `pytest -s`, reads `0/41`), ported from cognee's in-core Neo4j adapter
with APOC replaced, the GDS block dropped, and the two `*_node_truth_state`
methods taken from ladybug. `coercion.py` decides what reaches the store, and
every rule in it is a measurement — see [Coercion](#coercion). Every id lookup is
plan-verified index-backed — see [Indexes](#indexes).

cognee's own provenance contract suite passes **19/19** against a live FalkorDB.
Wiring that into CI — and pinning the cognee version it came from — is stage A5;
until then it is verified by hand, not by a gate.

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

## Scope: 41 methods, not 48

`GraphDBInterface` declares 48 public methods. Measured against cognee 1.4.1:

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

## Three things that will bite

**Pin `redis < 8.1.0`.** falkordb-py's async `FalkorDB.__init__` calls
`Is_Cluster()`, which copies the *async* pool's `connection_kwargs` into a *sync*
`redis.Redis(**kwargs)`. At redis 8.1.0 those carry `himport_registry`, which
sync Redis rejects — in the constructor, before any connection, so nothing works
at all. The sync client is unaffected, which is how this gets missed.

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
pytest -s -m "not integration"           # surface, signature drift, port helpers — no server
docker run -d --rm -p 6379:6379 falkordb/falkordb:v4.20.1
pytest -m integration                    # the port delta, against a live server
```

The real gate is cognee's own backend-neutral provenance contract suite
(`cognee/tests/integration/infrastructure/graph/test_graph_provenance_adapter_contract.py`),
whose fixture this adapter registers against. Its docstring invites exactly that.
