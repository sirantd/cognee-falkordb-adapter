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

**Stage A1 — scaffold.** The connection lifecycle and the index families are
real; the rest of the surface raises `NotImplementedError` tagged with the stage
that fills it in. `pytest -s tests/test_surface.py` prints the burn-down.

## Scope: 40 methods, not 48

`GraphDBInterface` declares 48 public methods. Measured against cognee 1.4.1:

| bucket | count | implemented here |
|---|---|---|
| `@abstractmethod` | 21 | yes — required to instantiate |
| default-raising, reached by cognee's runtime | 18 | yes |
| default-raising, reached by nothing | 8 | **no, deliberately** |
| real default (`remove_belongs_to_set_tags`) | 1 | inherited |
| `has_node` — called by the contract suite, absent from the interface | 1 | yes |

The 8 skipped methods are the feedback/frequency-weight surface. A test asserts
they stay unimplemented, so the omission is a decision rather than an oversight.

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

## Testing

```bash
pip install -e '.[test]'
pytest                                   # surface + signature drift, no server needed
docker run -d --rm -p 6379:6379 falkordb/falkordb:v4.20.1
pytest -m integration                    # needs a live server
```

The real gate is cognee's own backend-neutral provenance contract suite
(`cognee/tests/integration/infrastructure/graph/test_graph_provenance_adapter_contract.py`),
whose fixture this adapter registers against. Its docstring invites exactly that.
