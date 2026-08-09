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

**Stage A2 — the port delta is resolved.** All 40 methods are implemented; the
burn-down (`pytest -s`) reads `0/40`. Ported from cognee's in-core Neo4j adapter
with APOC replaced, the GDS block dropped, and the two `*_node_truth_state`
methods taken from ladybug.

cognee's own provenance contract suite already passes **19/19** against a live
FalkorDB. Wiring that into CI — and pinning the cognee version it came from — is
stage A5; until then it is verified by hand, not by a gate.

Still outstanding: **A3** (coercion — nulls stripped, C0 scrubbed) and **A4**
(the index families are created but their `EXPLAIN` gate is only asserted for
the shared label).

## Scope: 40 methods, not 48

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

## Two things that will bite

**Pin `redis < 8.1.0`.** falkordb-py's async `FalkorDB.__init__` calls
`Is_Cluster()`, which copies the *async* pool's `connection_kwargs` into a *sync*
`redis.Redis(**kwargs)`. At redis 8.1.0 those carry `himport_registry`, which
sync Redis rejects — in the constructor, before any connection, so nothing works
at all. The sync client is unaffected, which is how this gets missed.

**Nothing else creates the id indexes, and the failure is silent.** An unindexed
id lookup degrades to an All-Node-Scan with no error — measured at 9.6 ms versus
2.0 ms per lookup, and the difference between an 84.5 s bulk load and a 2,114 s
one. `initialize()` creates them; call it.

**A `null` property value is accepted and silently dropped** (a map, by contrast,
is rejected loudly). Stripping nulls before the write is stage A3; until then a
`None` reaches the store and simply vanishes.

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
