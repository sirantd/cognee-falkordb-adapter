"""What FalkorDB will and will not carry — measured, not assumed.

Two jobs, deliberately separate because they fail differently:

* :func:`coerce_properties` — **shape**. What the *store* can hold in a property.
  Everything here has a measured failure mode, and the failures are loud: a map
  or an array holding one raises ``Property values can only be of primitive types
  or arrays of primitive types``, which under ``UNWIND`` kills the whole batch,
  not the one node.
* :func:`scrub_nul` — **transport**. What the *parameter parser* can carry. Runs
  at the ``query()`` boundary so it covers every path into FalkorDB, including
  the ones that never touch a property map: tag lists, filter values, graph
  metadata and lookup ids.

Measured against FalkorDB **v4.20.1** with falkordb-py **1.6.2** on 2026-08-09:

| input | behaviour | rule |
|---|---|---|
| ``None`` property value | **DELETES** the stored property | drop the key |
| ``None`` inside an array | ``ResponseError`` — whole statement | drop the entry |
| map | ``ResponseError`` | JSON-encode |
| array holding a map, at any depth | ``ResponseError`` | JSON-encode |
| nested / heterogeneous array of primitives | stored natively, exact round-trip | pass through |
| ``UUID``, ``bytes``, or any other unknown type | stringified UNQUOTED by falkordb-py → ``Failed to parse query parameter`` | ``str()``, or ``decode()`` for bytes |
| ``\\x00`` in a value, key, array item or identifier | ``Failed to parse query parameter`` | strip |
| every other C0, and DEL | accepted, round-trips byte-identically | keep |

🚨 **Two of those contradict what this work was specced against**, and both
corrections make the layer more important rather than less:

1. A null is **not** "accepted and silently dropped". It is a delete. A bare
   cognee DataPoint dumps 7 null-valued keys, so passing them through would make
   every re-cognify strip ``source_pipeline`` / ``source_node_set`` /
   ``source_content_hash`` and friends off a node another pipeline had filled in.
2. FalkorDB does **not** reject C0 control characters "outright" — only NUL. The
   spike's ``graph_io.scrub`` strips all of C0 (``\\x00-\\x08``, ``\\x0b``,
   ``\\x0c``, ``\\x0e-\\x1f``); that is a superset of what is necessary, so a
   graph migrated by it stays readable, but there is no reason to lose the rest
   of the extraction text on the write path.

⚠ **There is no "clear this property" path through this layer, by design.** A
``None`` means "this DataPoint does not carry the field", which is what pydantic
emits for every unset optional — it does not mean "delete what is stored".
cognee's ladybug adapter takes the same position (``adapter.py`` omits
``truth_epoch`` from the update when it is ``None`` rather than writing a null).

⚠ **Not handled here, because it fails loudly rather than silently:** a property
*key* containing a backtick. falkordb-py raises ``ValueError`` client-side before
anything is sent, so it cannot corrupt a graph — it just kills the write.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional
from uuid import UUID

from cognee.modules.storage.utils import JSONEncoder
from cognee.shared.logging_utils import get_logger

logger = get_logger("FalkorDBAdapter")

# The one character FalkorDB's query-parameter parser rejects.
NUL = "\x00"

# Types falkordb-py stringifies correctly on its own. Anything else falls through
# to a bare `str(value)` with no quoting, which is a Cypher syntax error rather
# than a type error — so this layer converts it while it still can.
#
# ⚠ `bytes` is NOT in here, and it looks like it should be: falkordb-py's
# `quote_string` decodes bytes. It never reaches them — `stringify_param_value`
# tests `isinstance(value, str)` first and bytes fall through to `str(value)`,
# so `b"hello"` is sent as the unquoted token `b'hello'` and the statement dies
# with `Failed to parse query parameter`. Measured; decoded below instead.
_STORABLE_SCALARS = (str, int, float, bool)


def coerce_properties(properties: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return ``properties`` with every value in a shape FalkorDB will store.

    Null-valued keys are **absent** from the result, not null in it: the two are
    different writes. ``SET n += {k: null}`` removes a stored ``k``; an absent key
    leaves it alone.
    """
    coerced: Dict[str, Any] = {}
    dropped: List[str] = []

    for key, value in (properties or {}).items():
        if value is None:
            dropped.append(key)
            continue
        coerced[key] = coerce_value(value)

    if dropped:
        # Debug, not warning: unset optionals are the normal case on every single
        # DataPoint, so this is a "what did we not write" trace, not an anomaly.
        logger.debug("Dropped %d null-valued propert(ies): %s", len(dropped), ", ".join(dropped))

    return coerced


def coerce_value(value: Any) -> Any:
    """Coerce one property value. See the module docstring for the rule table.

    📌 Public, and deliberately: Stage D's bulk loader writes through raw
    parameterized Cypher rather than ``add_nodes`` (the interface folds
    provenance into that write, which a migration must not do), so it needs the
    same rules applied to the values it loads. A migrated value and a cognified
    one have to come out identically shaped or a re-cognify rewrites the graph.
    """
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        # Decoded rather than str()'d: `str(b"hi")` stores the repr `b'hi'`.
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return json.dumps(value, cls=JSONEncoder)
    if isinstance(value, (list, tuple)):
        if _contains_map(value):
            # Going out as JSON anyway, so nulls inside it are representable and
            # are kept — only a NATIVE array cannot hold one.
            return json.dumps(list(value), cls=JSONEncoder)
        return _storable_array(value)
    if isinstance(value, _STORABLE_SCALARS):
        return value
    return str(value)


def _contains_map(value: Any) -> bool:
    """True if a map lurks anywhere inside — the thing FalkorDB rejects."""
    if isinstance(value, dict):
        return True
    if isinstance(value, (list, tuple)):
        return any(_contains_map(item) for item in value)
    return False


def _storable_array(values: Any) -> List[Any]:
    """A native array: nulls removed, nested arrays kept, scalars coerced."""
    storable: List[Any] = []
    for item in values:
        if item is None:
            continue
        if isinstance(item, (list, tuple)):
            storable.append(_storable_array(item))
        else:
            storable.append(coerce_value(item))
    return storable


def scrub_nul(value: Any) -> Any:
    """Recursively remove NUL from strings, container values and mapping keys.

    Returns the **original object** when there is nothing to scrub — every query's
    parameters pass through here, including 1,000-node write batches, and the
    common path must not rebuild the whole structure to change nothing. ⚠ A tuple
    that *did* need scrubbing comes back as a list; falkordb-py stringifies the
    two identically, so nothing downstream can tell.
    """
    return _scrub(value) if _has_nul(value) else value


def _has_nul(value: Any) -> bool:
    if isinstance(value, str):
        return NUL in value
    if isinstance(value, dict):
        return any(_has_nul(key) or _has_nul(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_has_nul(item) for item in value)
    return False


def _scrub(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace(NUL, "")
    if isinstance(value, dict):
        return {_scrub(key): _scrub(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub(item) for item in value]
    return value
