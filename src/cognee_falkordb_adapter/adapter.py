"""FalkorDB graph adapter for cognee.

Seeded from cognee's in-core Neo4j adapter, which is the right starting point:
it is already Cypher, it already stores the four provenance fields as native
list properties, and its provenance-fold clause is portable Cypher
(``coalesce`` / ``CASE`` / list concatenation / ``IN``) that FalkorDB supports.

Scope is 41 methods, not the interface's full 48 — the spec's 40 plus
``remove_belongs_to_set_tags``, whose inherited default is a no-op the contract
suite fails. The feedback- and frequency-weight surface is left inheriting the
interface's raising defaults because nothing in cognee's runtime calls it.

Stage A2 resolved the port delta from that seed:

* **APOC is gone.** FalkorDB ships none of it. ``apoc.create.addLabels`` becomes
  a static ``SET n:`<Type>``` with the batch grouped by label in Python;
  ``apoc.coll.toSet`` becomes a filtered list append (both sides are deduped
  before they meet); ``apoc.merge.relationship`` becomes a static
  ``MERGE (s)-[r:`<type>`]->(t)`` with the batch grouped by relationship type.
* **GDS is gone**, and with it Neo4j's ``graph_exists`` / ``project_entire_graph``
  / ``drop_graph`` helpers — none of the three is an interface method, so they
  are not ported at all. :meth:`get_graph_metrics` computes what it can exactly
  and reports the rest as ``-1`` rather than pretending.
* ``get_node_truth_state`` / ``set_node_truth_state`` come from cognee's ladybug
  adapter, which is the only in-core adapter that implements them.

⚠ Property **coercion** is stage A3: this module serializes UUIDs and maps the
way the Neo4j seed does, but null-stripping and C0 scrubbing are not here yet.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Type, Union
from uuid import UUID

from falkordb.asyncio import FalkorDB

from cognee.infrastructure.databases.graph.graph_db_interface import GraphDBInterface
from cognee.infrastructure.databases.provenance import (
    EdgeDeleteData,
    EdgeIdentity,
    NodeDeleteData,
    get_dataset_id_from_source_ref_key,
    get_pipeline_run_id_from_source_run_ref,
    get_source_ref_key_from_source_run_ref,
)
from cognee.infrastructure.databases.provenance.source_ref_state import (
    provenance_after_attach,
    provenance_after_remove,
    provenance_attach_inputs,
)
from cognee.infrastructure.engine import DataPoint
from cognee.modules.storage.utils import JSONEncoder
from cognee.shared.logging_utils import get_logger

from .constants import (
    BASE_LABEL,
    METADATA_LABEL,
    METADATA_NODE_ID,
    NODE_TYPE_LABELS,
    PROVENANCE_COLUMNS,
)

logger = get_logger("FalkorDBAdapter")

# cognee's Node type alias, imported lazily to keep this module importable when
# the interface moves it.
Node = Any
NodeData = Dict[str, Any]
EdgeData = Tuple[str, str, str, Dict[str, Any]]

_PROVENANCE_KEY_SET = frozenset(PROVENANCE_COLUMNS)


def _strip_provenance(properties: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return ``properties`` without the four provenance list properties.

    Provenance is storage-internal: it must not reach retrieval, DataPoint
    reconstruction or the delete-data payload's ``*_properties`` field, which
    carries the four fields in their own typed slots instead.
    """
    if not properties:
        return dict(properties) if properties is not None else properties
    return {key: value for key, value in properties.items() if key not in _PROVENANCE_KEY_SET}


def _prov_list(value: Any) -> List[str]:
    """Normalize a native provenance list property (or a missing one) to ``list[str]``."""
    return list(value) if value else []


def _quote(identifier: str) -> str:
    """Backtick-quote a label or relationship type for interpolation into Cypher.

    🚨 Labels and relationship types **cannot** be parameterized in Cypher, and
    FalkorDB has no APOC to set them dynamically — so they are interpolated, and
    interpolation is the one place this adapter can be injected through. FalkorDB
    has no escape for a backtick inside a quoted identifier, so a backtick is
    dropped rather than escaped. Relationship names come from LLM extraction and
    are arbitrary text; every other character (spaces, punctuation, unicode)
    survives quoting intact, so the stored type still round-trips through
    ``type(r)`` for the provenance lookups that match it by parameter.
    """
    cleaned = identifier.replace("`", "")
    if cleaned != identifier:
        logger.warning("Dropped backtick(s) from graph identifier %r", identifier)
    return f"`{cleaned}`"


def _dedupe(values: Iterable[Any]) -> List[Any]:
    """Order-preserving dedupe — the Python half of the ``apoc.coll.toSet`` port."""
    return list(dict.fromkeys(values))


def _provenance_fold_clause(alias: str) -> str:
    """Cypher ``SET`` fragment that stamps provenance inside the artifact write.

    Appended to ``add_nodes`` / ``add_edges`` so a node/edge is created and
    stamped in one atomic statement — no read-then-write window. The run ref/id
    are appended only when the key is *not* already present (Model A).

    ⚠ **Ported with the SET-item ordering dependency removed.** Neo4j's version
    relies on a later ``SET`` item observing the pre-mutation value of a column
    an earlier item in the same list changes. Rather than assume FalkorDB
    evaluates a SET list with the same semantics, the caller binds the
    pre-mutation ``source_ref_keys`` into ``prov_keys_before`` in a preceding
    ``WITH``; every guard here reads that variable, so the clause is correct
    under either evaluation order.

    ``alias`` is the bound variable (``n`` for nodes, ``rel`` for edges) and
    ``prov_keys_before`` must already be in scope.
    """
    return f"""
            SET {alias}.source_run_refs = CASE
                    WHEN $prov_sr_key IN prov_keys_before
                    THEN coalesce({alias}.source_run_refs, [])
                    ELSE coalesce({alias}.source_run_refs, []) + $prov_add_run_refs
                END,
                {alias}.source_run_ids = CASE
                    WHEN $prov_sr_key IN prov_keys_before
                    THEN coalesce({alias}.source_run_ids, [])
                    ELSE coalesce({alias}.source_run_ids, [])
                         + [x IN $prov_add_run_ids
                            WHERE NOT x IN coalesce({alias}.source_run_ids, [])]
                END,
                {alias}.source_ref_keys = CASE
                    WHEN $prov_sr_key IN prov_keys_before
                    THEN prov_keys_before
                    ELSE prov_keys_before + $prov_add_keys
                END,
                {alias}.source_dataset_ids = CASE
                    WHEN $prov_ds_id IN coalesce({alias}.source_dataset_ids, [])
                    THEN coalesce({alias}.source_dataset_ids, [])
                    ELSE coalesce({alias}.source_dataset_ids, []) + $prov_add_dataset_ids
                END
            """


def _provenance_fold_params(source_ref_key: str, pipeline_run_id: Optional[str]) -> Dict[str, Any]:
    """Scalar query params consumed by :func:`_provenance_fold_clause`."""
    inputs = provenance_attach_inputs(source_ref_key, pipeline_run_id)
    return {
        "prov_sr_key": inputs.source_ref_key,
        "prov_add_keys": list(inputs.add_keys),
        "prov_ds_id": inputs.add_dataset_ids[0],
        "prov_add_dataset_ids": list(inputs.add_dataset_ids),
        "prov_add_run_refs": list(inputs.add_run_refs),
        "prov_add_run_ids": list(inputs.add_run_ids),
    }


def _connected_components(node_ids: Sequence[str], edges: Sequence[Tuple[str, str]]) -> List[int]:
    """Component sizes of an undirected graph, by union-find.

    Kept pure (and out of the database) because the Cypher alternatives are both
    bad: GDS does not exist on FalkorDB, and ladybug's unbounded variable-length
    traversal would not finish on a 59k-node graph inside the store's query
    timeout. Two lean id-only scans plus this is exact and cheap.
    """
    parent: Dict[str, str] = {node_id: node_id for node_id in node_ids}

    def find(item: str) -> str:
        root = item
        while parent[root] != root:
            root = parent[root]
        while parent[item] != root:  # path compression
            parent[item], item = root, parent[item]
        return root

    for source, target in edges:
        if source not in parent or target not in parent:
            continue
        source_root, target_root = find(source), find(target)
        if source_root != target_root:
            parent[source_root] = target_root

    sizes: Dict[str, int] = {}
    for node_id in parent:
        root = find(node_id)
        sizes[root] = sizes.get(root, 0) + 1
    return sorted(sizes.values(), reverse=True)


class FalkorDBAdapter(GraphDBInterface):
    """cognee graph adapter backed by a FalkorDB graph."""

    def __init__(
        self,
        graph_database_url: Optional[str] = None,
        graph_database_username: Optional[str] = None,
        graph_database_password: Optional[str] = None,
        graph_database_name: Optional[str] = None,
        host: str = "localhost",
        port: int = 6379,
        driver: Optional[Any] = None,
    ):
        """Open a FalkorDB client and select the graph.

        🚨 ``FalkorDB.__init__`` is BLOCKING and requires a reachable server: it
        calls ``Is_Cluster()``, which issues a synchronous ``INFO``. So this
        constructor performs network I/O, and constructing it on the event loop
        blocks it briefly. That is falkordb-py's design, not a choice here — but
        it means "the adapter constructed" already proves the server is up, and
        a construction failure is a connectivity failure, not a config error.
        """
        self._graph_name = graph_database_name or "cognee_graph"

        if driver is not None:  # tests inject a fake
            self._db = driver
        elif graph_database_url:
            self._db = FalkorDB.from_url(graph_database_url)
        else:
            self._db = FalkorDB(
                host=host,
                port=port,
                username=graph_database_username or None,
                password=graph_database_password or None,
            )

        self._graph = self._db.select_graph(self._graph_name)
        # Serializes the read-modify-write in explicit attach/remove source-ref
        # calls so two concurrent updates to the same artifact within this
        # adapter instance cannot overwrite each other (the atomic fold path in
        # add_nodes/add_edges does not need it).
        self._source_ref_change_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def query(self, query: str, params: Optional[dict] = None) -> List[Any]:
        """Run a Cypher query and return its rows as dicts keyed by column name.

        FalkorDB returns positional rows plus a header; Neo4j's driver returns
        dicts. Zipping them here is what lets the ported query bodies stay
        readable (``row["id"]``, not ``row[3]``) and is also the friendlier shape
        for cognee's CYPHER / NATURAL_LANGUAGE retrievers, which hand the result
        straight to an LLM. Node and relationship values are unwrapped to their
        property dicts, exactly as neo4j's ``Result.data()`` does.
        """
        result = await self._graph.query(query, params or {})
        return self._rows(result)

    @staticmethod
    def _column_names(result: Any) -> List[str]:
        names = []
        for column in getattr(result, "header", None) or []:
            name = column[1] if isinstance(column, (list, tuple)) and len(column) > 1 else column
            names.append(name.decode() if isinstance(name, bytes) else str(name))
        return names

    @classmethod
    def _cell(cls, value: Any) -> Any:
        """Unwrap a FalkorDB result value into plain Python."""
        properties = getattr(value, "properties", None)
        if properties is not None and not isinstance(value, dict):
            return dict(properties)
        if isinstance(value, dict):
            return {key: cls._cell(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._cell(item) for item in value]
        return value

    @classmethod
    def _rows(cls, result: Any) -> List[Dict[str, Any]]:
        names = cls._column_names(result)
        rows = getattr(result, "result_set", None) or []
        return [{name: cls._cell(value) for name, value in zip(names, row)} for row in rows]

    async def close(self) -> None:
        """Release the client. The contract fixture calls this after every case."""
        await self._db.aclose()

    async def initialize(self) -> None:
        """Create both id-index families.

        🚨 NOTHING ELSE CREATES THESE, AND THE FAILURE IS SILENT. Without them an
        id lookup degrades to an All-Node-Scan — no error, no warning, just a
        graph that gets slower with every node (#68 measured 9.6 ms vs 2.0 ms per
        lookup, and a 2,114 s migration instead of an 84.5 s one).

        FalkorDB accepts an index on a label that does not exist yet, so there is
        no chicken-and-egg and no reason to defer this to first write.

        Idempotent: an existing index raises, and that is the expected steady
        state on every converge after the first.
        """
        for label in (BASE_LABEL, *NODE_TYPE_LABELS):
            try:
                await self._graph.create_node_range_index(label, "id")
            except Exception as exc:  # already indexed — the normal path
                if "already indexed" not in str(exc).lower():
                    raise

    # ------------------------------------------------------------------
    # Property serialization
    # ------------------------------------------------------------------

    def serialize_properties(self, properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Convert a property dict into values FalkorDB accepts.

        FalkorDB stores primitives and arrays of primitives natively and
        **rejects a map outright** (``Property values can only be of primitive
        types or arrays of primitive types``), so maps are JSON-encoded under
        their own key — no ``_json`` suffix, unlike the Neo4j seed's edge path,
        because renaming the key would make a property unreadable by anything
        that looks for it by name.

        ⚠ Arrays are passed through rather than JSON-encoded (the seed encodes
        them on edges only, which is a Neo4j-side habit, not a requirement) and
        ⚠ ``None`` values are still passed through here — FalkorDB accepts and
        then *silently drops* them. Stripping them explicitly is stage A3.
        """
        serialized: Dict[str, Any] = {}

        for key, value in (properties or {}).items():
            if isinstance(value, UUID):
                serialized[key] = str(value)
            elif key == "weights" and isinstance(value, dict):
                # Flattened rather than JSON-encoded because cognee's own
                # visualization preprocessor reads `weight_<name>` scalars, and
                # `get_graph_from_model` already emits that shape upstream.
                for weight_name, weight_value in value.items():
                    serialized[f"weight_{weight_name}"] = weight_value
            elif isinstance(value, dict):
                serialized[key] = json.dumps(value, cls=JSONEncoder)
            elif isinstance(value, (list, tuple)) and any(
                isinstance(item, (dict, list, tuple)) for item in value
            ):
                # An array of primitives is native; a nested one is not.
                serialized[key] = json.dumps(list(value), cls=JSONEncoder)
            else:
                serialized[key] = value

        return serialized

    def _node_payload(self, node: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
        """Return ``(node_id, type_label, serialized_properties)`` for one input node.

        Accepts every shape the interface's ``Union[List[Node], List[DataPoint]]``
        allows: a DataPoint (or anything with ``model_dump``), the ``(id,
        properties)`` tuple the ``Node`` alias names, or a bare property dict.
        The seed only handled the first, and the tuple form is what a bulk loader
        naturally holds.
        """
        if isinstance(node, (tuple, list)) and len(node) == 2 and isinstance(node[1], dict):
            node_id, raw = str(node[0]), dict(node[1])
            raw.setdefault("id", node_id)
        elif hasattr(node, "model_dump"):
            raw = node.model_dump()
            node_id = str(raw.get("id", getattr(node, "id", "")))
        elif isinstance(node, dict):
            raw = dict(node)
            node_id = str(raw.get("id", ""))
        else:
            raw = dict(vars(node))
            node_id = str(raw.get("id", getattr(node, "id", "")))

        properties = self.serialize_properties(raw)
        properties["id"] = node_id
        if properties.get("belongs_to_set"):
            properties["belongs_to_set"] = _dedupe(properties["belongs_to_set"])

        declared_type = properties.get("type")
        label = str(declared_type) if declared_type else type(node).__name__
        if label in ("dict", "tuple", "list"):  # a bare payload declares no type
            label = None
        return node_id, label, properties

    # ------------------------------------------------------------------
    # Present on every cognee adapter, absent from GraphDBInterface.
    # The provenance contract suite calls it, so it is part of the target.
    # ------------------------------------------------------------------

    async def has_node(self, node_id: str) -> bool:
        """Return True when a node with this id exists."""
        rows = await self.query(
            f"MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}}) RETURN count(n) > 0 AS node_exists",
            {"node_id": str(node_id)},
        )
        return bool(rows[0]["node_exists"]) if rows else False

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    async def is_empty(self) -> bool:
        """Return True if the graph contains no data nodes.

        Scoped to the shared label so the graph-metadata marker never makes a
        data-empty graph read as non-empty — the provenance marking gate depends
        on it. ``LIMIT 1`` rather than a count: this is asked on a graph that may
        hold 59k nodes and the answer needs one row, not all of them.
        """
        rows = await self.query(f"MATCH (n:{_quote(BASE_LABEL)}) RETURN n.id AS id LIMIT 1")
        return not rows

    async def add_node(
        self, node: Union[DataPoint, str], properties: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add or update a single node.

        The interface allows a bare id plus a property dict; the seed did not,
        and cognee's runtime only ever passes a DataPoint. Both are accepted here
        because refusing a call the interface declares is a narrowing.
        """
        if isinstance(node, str):
            payload = dict(properties or {})
            payload["id"] = node
            await self.add_nodes([payload])
        else:
            await self.add_nodes([node])

    async def add_nodes(
        self,
        nodes: Union[List[Node], List[DataPoint]],
        source_ref_key: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        """Add or update many nodes, stamping provenance in the same write.

        🚨 **The APOC port lives here.** Neo4j MERGEs on the shared label and then
        calls ``apoc.create.addLabels`` to attach the node's real type. FalkorDB
        has no APOC, but it does support a *static* ``SET n:`Type``` — so the
        batch is grouped by label in Python and each group gets its own
        statement. The MERGE still keys on ``(:__Node__ {id})`` alone, which is
        what matters: MERGEing on the type label instead would miss an existing
        node whose type has changed and create a **second row with the same id** —
        the exact duplicate-id class this migration exists to leave behind.

        ``apoc.coll.toSet`` is likewise gone: the incoming tags are deduped in
        Python, the stored tags are deduped by this method's own invariant, and
        the union is a filtered list append.
        """
        fold_clause = ""
        provenance_params: Dict[str, Any] = {}
        if source_ref_key is not None:
            fold_clause = _provenance_fold_clause("n")
            provenance_params = _provenance_fold_params(source_ref_key, pipeline_run_id)

        # Dedup by node_id within the batch — UNWIND over a list with duplicates
        # visits the same node twice, and the second pass recomputes the tag
        # union from the pre-SET state, dropping any tag only the first carried.
        deduped: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            node_id, label, node_properties = self._node_payload(node)
            existing = deduped.get(node_id)
            if existing:
                merged = _dedupe(
                    list(existing["properties"].get("belongs_to_set") or [])
                    + list(node_properties.get("belongs_to_set") or [])
                )
                if merged:
                    node_properties["belongs_to_set"] = merged
                existing["properties"] = node_properties
                existing["label"] = label
            else:
                deduped[node_id] = {
                    "node_id": node_id,
                    "label": label,
                    "properties": node_properties,
                }

        by_label: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for entry in deduped.values():
            by_label.setdefault(entry["label"], []).append(entry)

        for label, batch in by_label.items():
            label_clause = f"SET n:{_quote(label)}" if label else ""
            query = f"""
            UNWIND $nodes AS node
            MERGE (n:{_quote(BASE_LABEL)} {{id: node.node_id}})
            {label_clause}
            WITH n, node,
                 coalesce(n.belongs_to_set, []) AS existing_tags,
                 coalesce(n.source_ref_keys, []) AS prov_keys_before
            WITH n, node, prov_keys_before,
                 existing_tags
                 + [tag IN coalesce(node.properties.belongs_to_set, [])
                    WHERE NOT tag IN existing_tags] AS merged_belongs_to_set
            SET n += node.properties, n.updated_at = timestamp()
            SET n.belongs_to_set = merged_belongs_to_set
            {fold_clause}
            RETURN n.id AS node_id
            """
            await self.query(query, {"nodes": batch, **provenance_params})

    async def delete_node(self, node_id: str) -> None:
        """Delete one node and its relationships."""
        await self.query(
            f"MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}}) DETACH DELETE n",
            {"node_id": str(node_id)},
        )

    async def delete_nodes(self, node_ids: List[str]) -> None:
        """Delete many nodes and their relationships."""
        if not node_ids:
            return
        await self.query(
            f"""
            UNWIND $node_ids AS wanted
            MATCH (n:{_quote(BASE_LABEL)} {{id: wanted}})
            DETACH DELETE n
            """,
            {"node_ids": [str(node_id) for node_id in node_ids]},
        )

    async def remove_belongs_to_set_tags(
        self,
        tags: List[str],
        node_ids: Optional[List[str]] = None,
    ) -> None:
        """Strip tag names from ``belongs_to_set`` and drop the stale NodeSet edges.

        📌 The spec's method census called this one "override only if slow"; it is
        not optional. The interface's default is a no-op, and the provenance
        contract suite asserts the tags are actually gone — so a backend that
        inherits the default fails ``test_remove_belongs_to_set_tags_*``. Both
        in-core adapters that pass the suite implement it.
        """
        if not tags or (node_ids is not None and not node_ids):
            return

        id_filter = "AND n.id IN $node_ids" if node_ids is not None else ""
        node_scope = "WHERE n.id IN $node_ids" if node_ids is not None else ""
        edge_scope_keyword = "AND" if node_ids is not None else "WHERE"

        # One statement, two phases bridged by `WITH count(*)`: strip the property
        # first, then prune edges to NodeSets whose name is being removed. The
        # bridge sequences the phases while keeping their match sets independent,
        # so an edge to a stale NodeSet is pruned even when the source node never
        # carried the tag in its array.
        query = f"""
        MATCH (n:{_quote(BASE_LABEL)})
        WHERE any(tag IN $tags WHERE tag IN coalesce(n.belongs_to_set, []))
        {id_filter}
        SET n.belongs_to_set = [x IN coalesce(n.belongs_to_set, []) WHERE NOT x IN $tags]
        WITH count(*) AS _bridge
        MATCH (n:{_quote(BASE_LABEL)})-[r:belongs_to_set]->(ns:NodeSet)
        {node_scope}
        {edge_scope_keyword} ns.name IN $tags
        DELETE r
        """
        params: Dict[str, Any] = {"tags": list(tags)}
        if node_ids is not None:
            params["node_ids"] = [str(node_id) for node_id in node_ids]
        await self.query(query, params)

    async def get_node(self, node_id: str) -> Optional[NodeData]:
        """Retrieve one node's properties, or None."""
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}})
            RETURN properties(n) AS properties
            """,
            {"node_id": str(node_id)},
        )
        return _strip_provenance(rows[0]["properties"]) if rows else None

    async def get_nodes(self, node_ids: List[str]) -> List[NodeData]:
        """Retrieve many nodes' properties. Missing ids are simply absent."""
        if not node_ids:
            return []
        rows = await self.query(
            f"""
            UNWIND $node_ids AS wanted
            MATCH (n:{_quote(BASE_LABEL)} {{id: wanted}})
            RETURN properties(n) AS properties
            """,
            {"node_ids": [str(node_id) for node_id in node_ids]},
        )
        return [_strip_provenance(row["properties"]) for row in rows]

    async def get_neighbors(self, node_id: str) -> List[NodeData]:
        """Every node directly connected to this one, in either direction."""
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}})--(m:{_quote(BASE_LABEL)})
            RETURN DISTINCT properties(m) AS properties
            """,
            {"node_id": str(node_id)},
        )
        return [_strip_provenance(row["properties"]) for row in rows]

    async def get_connections(
        self, node_id: Union[str, UUID]
    ) -> List[Tuple[NodeData, Dict[str, Any], NodeData]]:
        """Every (predecessor, relationship, node) and (node, relationship, successor) triple."""
        predecessors, successors = await asyncio.gather(
            self.query(
                f"""
                MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}})<-[r]-(m:{_quote(BASE_LABEL)})
                RETURN properties(m) AS other, type(r) AS rel, properties(n) AS node
                """,
                {"node_id": str(node_id)},
            ),
            self.query(
                f"""
                MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}})-[r]->(m:{_quote(BASE_LABEL)})
                RETURN properties(n) AS node, type(r) AS rel, properties(m) AS other
                """,
                {"node_id": str(node_id)},
            ),
        )

        connections: List[Tuple[NodeData, Dict[str, Any], NodeData]] = [
            (
                _strip_provenance(row["other"]),
                {"relationship_name": row["rel"]},
                _strip_provenance(row["node"]),
            )
            for row in predecessors
        ]
        connections.extend(
            (
                _strip_provenance(row["node"]),
                {"relationship_name": row["rel"]},
                _strip_provenance(row["other"]),
            )
            for row in successors
        )
        return connections

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------

    async def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create or update one edge."""
        await self.add_edges([(source_id, target_id, relationship_name, properties or {})])

    async def add_edges(
        self,
        edges: Union[List[EdgeData], List[Tuple[str, str, str, Optional[Dict[str, Any]]]]],
        source_ref_key: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        """Add or update many edges, stamping provenance in the same write.

        🚨 **The second half of the APOC port.** ``apoc.merge.relationship`` took
        the relationship type as a runtime value; Cypher cannot, so the batch is
        grouped by type in Python and each group interpolates its own quoted
        type. Properties are set after the MERGE (not via an ``ON CREATE``
        clause) so a re-cognify updates an existing edge, and ``created_at`` is
        coalesced so it keeps the first write's timestamp.
        """
        if not edges:
            return

        fold_clause = ""
        provenance_params: Dict[str, Any] = {}
        if source_ref_key is not None:
            fold_clause = _provenance_fold_clause("rel")
            provenance_params = _provenance_fold_params(source_ref_key, pipeline_run_id)

        by_type: Dict[str, List[Dict[str, Any]]] = {}
        for edge in edges:
            source, target, relationship_name = str(edge[0]), str(edge[1]), str(edge[2])
            edge_properties = edge[3] if len(edge) > 3 and edge[3] else {}
            by_type.setdefault(relationship_name, []).append(
                {
                    "from_node": source,
                    "to_node": target,
                    "properties": self.serialize_properties(
                        {
                            **edge_properties,
                            "source_node_id": source,
                            "target_node_id": target,
                        }
                    ),
                }
            )

        for relationship_name, batch in by_type.items():
            query = f"""
            UNWIND $edges AS edge
            MATCH (from_node:{_quote(BASE_LABEL)} {{id: edge.from_node}})
            MATCH (to_node:{_quote(BASE_LABEL)} {{id: edge.to_node}})
            MERGE (from_node)-[rel:{_quote(relationship_name)}]->(to_node)
            WITH rel, edge, coalesce(rel.source_ref_keys, []) AS prov_keys_before
            SET rel += edge.properties,
                rel.updated_at = timestamp(),
                rel.created_at = coalesce(rel.created_at, timestamp())
            {fold_clause}
            RETURN type(rel) AS relationship_name
            """
            await self.query(query, {"edges": batch, **provenance_params})

    async def has_edge(self, source_id: str, target_id: str, relationship_name: str) -> bool:
        """Return True when this exact edge exists.

        ⚠ Returns a **bool**, unlike the Neo4j seed, whose version returns the raw
        query result and references an unbound ``relationship`` variable. The
        interface declares a bool and ladybug returns one.
        """
        rows = await self.query(
            f"""
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = $source_id AND b.id = $target_id AND type(r) = $relationship_name
            RETURN count(r) > 0 AS edge_exists
            """,
            {
                "source_id": str(source_id),
                "target_id": str(target_id),
                "relationship_name": str(relationship_name),
            },
        )
        return bool(rows[0]["edge_exists"]) if rows else False

    async def has_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        """Return the subset of ``edges`` that exists.

        ⚠ Follows ladybug and the interface's own signature (``List[EdgeData] ->
        List[EdgeData]``) rather than the seed, which returns a list of bools and
        matches on Neo4j-internal ids that no caller has.
        """
        if not edges:
            return []
        rows = await self.query(
            f"""
            UNWIND $edges AS edge
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = edge.from_node
              AND b.id = edge.to_node
              AND type(r) = edge.relationship_name
            RETURN DISTINCT a.id AS source_id, b.id AS target_id, type(r) AS relationship_name
            """,
            {
                "edges": [
                    {
                        "from_node": str(edge[0]),
                        "to_node": str(edge[1]),
                        "relationship_name": str(edge[2]),
                    }
                    for edge in edges
                ]
            },
        )
        found = {(row["source_id"], row["target_id"], row["relationship_name"]) for row in rows}
        return [
            edge for edge in edges if (str(edge[0]), str(edge[1]), str(edge[2])) in found
        ]

    async def get_edges(self, node_id: str) -> List[EdgeData]:
        """Every edge touching this node, as ``(source, target, {relationship_name})``."""
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)} {{id: $node_id}})-[r]-(m:{_quote(BASE_LABEL)})
            RETURN n.id AS node_id, m.id AS other_id, type(r) AS relationship_name
            """,
            {"node_id": str(node_id)},
        )
        return [
            (row["node_id"], row["other_id"], {"relationship_name": row["relationship_name"]})
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Graph provenance
    #
    # The four provenance fields live in native list properties on the node /
    # relationship. attach/remove do a per-artifact read-modify-write under a
    # lock (delete/rollback is a maintenance path, not a hot path); lookups are
    # native list-membership scans. Every read normalizes a missing property to
    # []. Edges are addressed by (source id, target id, relationship type):
    # FalkorDB, like Neo4j, stores the relationship name AS the type, so every
    # provenance query matches on ``type(r)``.
    # ------------------------------------------------------------------

    @staticmethod
    def _node_identity_row(node_id: str) -> dict:
        return {"id": node_id}

    @staticmethod
    def _edge_identity_row(edge: EdgeIdentity) -> dict:
        return {"s": edge.source_id, "t": edge.target_id, "rel": edge.relationship_name}

    @staticmethod
    def _indexed_fields_from_properties(properties: Dict[str, Any]) -> List[str]:
        """Read ``metadata.index_fields`` from a node's stored properties.

        ``metadata`` is a map, so it is stored JSON-encoded (FalkorDB rejects maps
        outright) and parsed back here.
        """
        metadata = properties.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        if isinstance(metadata, dict):
            return list(metadata.get("index_fields") or [])
        return []

    async def _read_node_provenance(
        self, node_ids: List[str]
    ) -> Dict[str, Tuple[List[str], List[str]]]:
        """Return ``{node_id: (source_ref_keys, source_run_refs)}`` for existing nodes."""
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id IN $ids
            RETURN n.id AS id,
                   coalesce(n.source_ref_keys, []) AS keys,
                   coalesce(n.source_run_refs, []) AS run_refs
            """,
            {"ids": [str(node_id) for node_id in node_ids]},
        )
        return {row["id"]: (_prov_list(row["keys"]), _prov_list(row["run_refs"])) for row in rows}

    async def _write_node_provenance(self, batch: List[dict]) -> None:
        if not batch:
            return
        await self.query(
            f"""
            UNWIND $batch AS row
            MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id = row.id
            SET n.source_ref_keys = row.refs,
                n.source_dataset_ids = row.datasets,
                n.source_run_ids = row.runs,
                n.source_run_refs = row.run_refs
            """,
            {"batch": batch},
        )

    async def _read_edge_provenance(
        self, edges: List[EdgeIdentity]
    ) -> Dict[EdgeIdentity, Tuple[List[str], List[str]]]:
        """Return ``{edge: (source_ref_keys, source_run_refs)}`` for existing edges."""
        rows = await self.query(
            f"""
            UNWIND $edges AS e
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = e.s AND b.id = e.t AND type(r) = e.rel
            RETURN a.id AS s, b.id AS t, type(r) AS rel,
                   coalesce(r.source_ref_keys, []) AS keys,
                   coalesce(r.source_run_refs, []) AS run_refs
            """,
            {"edges": [self._edge_identity_row(edge) for edge in edges]},
        )
        return {
            EdgeIdentity(
                source_id=row["s"], target_id=row["t"], relationship_name=row["rel"]
            ): (_prov_list(row["keys"]), _prov_list(row["run_refs"]))
            for row in rows
        }

    async def _write_edge_provenance(self, batch: List[dict]) -> None:
        if not batch:
            return
        await self.query(
            f"""
            UNWIND $batch AS row
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = row.s AND b.id = row.t AND type(r) = row.rel
            SET r.source_ref_keys = row.refs,
                r.source_dataset_ids = row.datasets,
                r.source_run_ids = row.runs,
                r.source_run_refs = row.run_refs
            """,
            {"batch": batch},
        )

    async def _apply_source_ref_change(
        self,
        artifacts,
        read_provenance,
        write_provenance,
        identity_row,
        transition,
    ) -> None:
        """Read each artifact's provenance, apply a pure transition, write it back.

        Shared by attach/remove for both nodes and edges. The lock serializes the
        read-modify-write within one adapter instance so concurrent explicit
        attach/remove calls do not overwrite each other's provenance updates.
        """
        if not artifacts:
            return
        async with self._source_ref_change_lock:
            current = await read_provenance(artifacts)
            batch = []
            for identity, (keys, run_refs) in current.items():
                columns = transition(keys, run_refs)
                batch.append(
                    {
                        **identity_row(identity),
                        "refs": columns.source_ref_keys,
                        "datasets": columns.source_dataset_ids,
                        "runs": columns.source_run_ids,
                        "run_refs": columns.source_run_refs,
                    }
                )
            await write_provenance(batch)

    async def attach_node_source_refs(
        self,
        node_ids: list[str],
        source_ref_keys: list[str],
        pipeline_run_id: str | None = None,
    ) -> None:
        if not source_ref_keys:
            return
        add_keys = list(source_ref_keys)
        await self._apply_source_ref_change(
            node_ids,
            self._read_node_provenance,
            self._write_node_provenance,
            self._node_identity_row,
            lambda keys, run_refs: provenance_after_attach(
                keys, run_refs, add_keys, pipeline_run_id
            ),
        )

    async def attach_edge_source_refs(
        self,
        edges: list[EdgeIdentity],
        source_ref_keys: list[str],
        pipeline_run_id: str | None = None,
    ) -> None:
        if not source_ref_keys:
            return
        add_keys = list(source_ref_keys)
        await self._apply_source_ref_change(
            edges,
            self._read_edge_provenance,
            self._write_edge_provenance,
            self._edge_identity_row,
            lambda keys, run_refs: provenance_after_attach(
                keys, run_refs, add_keys, pipeline_run_id
            ),
        )

    async def remove_node_source_refs(
        self,
        node_ids: list[str],
        source_ref_keys: list[str],
    ) -> None:
        if not source_ref_keys:
            return
        remove_keys = list(source_ref_keys)
        await self._apply_source_ref_change(
            node_ids,
            self._read_node_provenance,
            self._write_node_provenance,
            self._node_identity_row,
            lambda keys, run_refs: provenance_after_remove(keys, run_refs, remove_keys),
        )

    async def remove_edge_source_refs(
        self,
        edges: list[EdgeIdentity],
        source_ref_keys: list[str],
    ) -> None:
        if not source_ref_keys:
            return
        remove_keys = list(source_ref_keys)
        await self._apply_source_ref_change(
            edges,
            self._read_edge_provenance,
            self._write_edge_provenance,
            self._edge_identity_row,
            lambda keys, run_refs: provenance_after_remove(keys, run_refs, remove_keys),
        )

    async def delete_edge_triples(self, edges: list[EdgeIdentity]) -> None:
        """Delete exactly these relationships, keeping both endpoints."""
        if not edges:
            return
        # DELETE r, never DETACH DELETE: the endpoints are other datasets' nodes.
        await self.query(
            f"""
            UNWIND $edges AS e
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = e.s AND b.id = e.t AND type(r) = e.rel
            DELETE r
            """,
            {"edges": [self._edge_identity_row(edge) for edge in edges]},
        )

    async def get_node_delete_data(self, node_ids: list[str]) -> dict[str, NodeDeleteData]:
        if not node_ids:
            return {}
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id IN $ids
            RETURN n.id AS id, properties(n) AS properties,
                   coalesce(n.source_ref_keys, []) AS srk,
                   coalesce(n.source_dataset_ids, []) AS sdi,
                   coalesce(n.source_run_ids, []) AS sri,
                   coalesce(n.source_run_refs, []) AS srr
            """,
            {"ids": [str(node_id) for node_id in node_ids]},
        )
        result: dict[str, NodeDeleteData] = {}
        for row in rows:
            properties = row["properties"] or {}
            result[row["id"]] = NodeDeleteData(
                node_id=row["id"],
                node_type=properties.get("type") or "",
                indexed_fields=self._indexed_fields_from_properties(properties),
                node_properties=_strip_provenance(properties),
                source_ref_keys=_prov_list(row["srk"]),
                source_dataset_ids=_prov_list(row["sdi"]),
                source_run_ids=_prov_list(row["sri"]),
                source_run_refs=_prov_list(row["srr"]),
            )
        return result

    async def get_edge_delete_data(
        self, edges: list[EdgeIdentity]
    ) -> dict[EdgeIdentity, EdgeDeleteData]:
        if not edges:
            return {}
        rows = await self.query(
            f"""
            UNWIND $edges AS e
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE a.id = e.s AND b.id = e.t AND type(r) = e.rel
            RETURN a.id AS s, b.id AS t, type(r) AS rel, properties(r) AS properties,
                   coalesce(r.source_ref_keys, []) AS srk,
                   coalesce(r.source_dataset_ids, []) AS sdi,
                   coalesce(r.source_run_ids, []) AS sri,
                   coalesce(r.source_run_refs, []) AS srr
            """,
            {"edges": [self._edge_identity_row(edge) for edge in edges]},
        )
        # Lazy import: this helper lives in cognee's modules layer, whose package
        # __init__ reaches the graph engine. At delete time the cycle is closed.
        from cognee.modules.graph.utils.prepare_edges_for_storage import get_edge_retrieval_text

        result: dict[EdgeIdentity, EdgeDeleteData] = {}
        for row in rows:
            edge = EdgeIdentity(
                source_id=row["s"], target_id=row["t"], relationship_name=row["rel"]
            )
            properties = row["properties"] or {}
            result[edge] = EdgeDeleteData(
                edge=edge,
                # Stored edge_text wins; fall back to the relationship name.
                edge_text=get_edge_retrieval_text(
                    properties.get("edge_text"), edge.relationship_name
                ),
                edge_properties=_strip_provenance(properties),
                source_ref_keys=_prov_list(row["srk"]),
                source_dataset_ids=_prov_list(row["sdi"]),
                source_run_ids=_prov_list(row["sri"]),
                source_run_refs=_prov_list(row["srr"]),
            )
        return result

    async def find_nodes_by_source_ref(self, source_ref_key: str) -> list[str]:
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(n.source_ref_keys, [])
            RETURN n.id AS id
            """,
            {"token": source_ref_key},
        )
        return [row["id"] for row in rows]

    async def find_edges_by_source_ref(self, source_ref_key: str) -> list[EdgeIdentity]:
        rows = await self.query(
            f"""
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(r.source_ref_keys, [])
            RETURN a.id AS s, b.id AS t, type(r) AS rel
            """,
            {"token": source_ref_key},
        )
        return [
            EdgeIdentity(source_id=row["s"], target_id=row["t"], relationship_name=row["rel"])
            for row in rows
        ]

    async def find_node_source_refs_by_dataset(self, dataset_id: str) -> dict[str, list[str]]:
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(n.source_dataset_ids, [])
            RETURN n.id AS id, coalesce(n.source_ref_keys, []) AS keys
            """,
            {"token": dataset_id},
        )
        result: dict[str, list[str]] = {}
        for row in rows:
            owned = [
                key
                for key in _prov_list(row["keys"])
                if str(get_dataset_id_from_source_ref_key(key)) == dataset_id
            ]
            if owned:
                result[row["id"]] = owned
        return result

    async def find_edge_source_refs_by_dataset(
        self, dataset_id: str
    ) -> dict[EdgeIdentity, list[str]]:
        rows = await self.query(
            f"""
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(r.source_dataset_ids, [])
            RETURN a.id AS s, b.id AS t, type(r) AS rel, coalesce(r.source_ref_keys, []) AS keys
            """,
            {"token": dataset_id},
        )
        result: dict[EdgeIdentity, list[str]] = {}
        for row in rows:
            owned = [
                key
                for key in _prov_list(row["keys"])
                if str(get_dataset_id_from_source_ref_key(key)) == dataset_id
            ]
            if owned:
                result[
                    EdgeIdentity(
                        source_id=row["s"], target_id=row["t"], relationship_name=row["rel"]
                    )
                ] = owned
        return result

    async def find_node_source_refs_by_pipeline_run(
        self, pipeline_run_id: str
    ) -> dict[str, list[str]]:
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(n.source_run_ids, [])
            RETURN n.id AS id, coalesce(n.source_run_refs, []) AS run_refs
            """,
            {"token": pipeline_run_id},
        )
        result: dict[str, list[str]] = {}
        for row in rows:
            contributed = [
                get_source_ref_key_from_source_run_ref(ref)
                for ref in _prov_list(row["run_refs"])
                if str(get_pipeline_run_id_from_source_run_ref(ref)) == pipeline_run_id
            ]
            if contributed:
                result[row["id"]] = contributed
        return result

    async def find_edge_source_refs_by_pipeline_run(
        self, pipeline_run_id: str
    ) -> dict[EdgeIdentity, list[str]]:
        rows = await self.query(
            f"""
            MATCH (a:{_quote(BASE_LABEL)})-[r]->(b:{_quote(BASE_LABEL)})
            WHERE $token IN coalesce(r.source_run_ids, [])
            RETURN a.id AS s, b.id AS t, type(r) AS rel,
                   coalesce(r.source_run_refs, []) AS run_refs
            """,
            {"token": pipeline_run_id},
        )
        result: dict[EdgeIdentity, list[str]] = {}
        for row in rows:
            contributed = [
                get_source_ref_key_from_source_run_ref(ref)
                for ref in _prov_list(row["run_refs"])
                if str(get_pipeline_run_id_from_source_run_ref(ref)) == pipeline_run_id
            ]
            if contributed:
                result[
                    EdgeIdentity(
                        source_id=row["s"], target_id=row["t"], relationship_name=row["rel"]
                    )
                ] = contributed
        return result

    async def set_graph_metadata(self, metadata: dict[str, str]) -> None:
        """Merge graph-level metadata onto the singleton marker node.

        📌 One node holding every key, not Neo4j's node-per-key: the marker is
        read as a whole on every provenance gate check, and a single MERGE is one
        round trip regardless of how many keys it carries.

        🚨 The marker deliberately does **not** carry the shared label, so
        ``is_empty()`` and ``get_graph_data()`` never see it as data.
        """
        if not metadata:
            return
        await self.query(
            f"""
            MERGE (m:{_quote(METADATA_LABEL)} {{id: $marker_id}})
            SET m += $metadata
            """,
            {
                "marker_id": METADATA_NODE_ID,
                "metadata": {str(key): str(value) for key, value in metadata.items()},
            },
        )

    async def get_graph_metadata(self) -> dict[str, str]:
        rows = await self.query(
            f"""
            MATCH (m:{_quote(METADATA_LABEL)} {{id: $marker_id}})
            RETURN properties(m) AS properties
            """,
            {"marker_id": METADATA_NODE_ID},
        )
        if not rows:
            return {}
        return {
            key: value for key, value in (rows[0]["properties"] or {}).items() if key != "id"
        }

    # ------------------------------------------------------------------
    # Truth state — ported from cognee's ladybug adapter, the only in-core
    # adapter that implements it. Ladybug packs everything into one JSON
    # `properties` blob and so has to read-modify-write the whole node; native
    # properties make both sides a direct read and a direct SET.
    # ------------------------------------------------------------------

    async def get_node_truth_state(self, node_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Return ``{node_id: {"truth_alignment": [...], "truth_epoch": int|None}}``."""
        valid_ids = [str(node_id) for node_id in node_ids or [] if isinstance(node_id, str) and node_id]
        if not valid_ids:
            return {}
        rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id IN $ids
            RETURN n.id AS id, n.truth_alignment AS alignment, n.truth_epoch AS epoch
            """,
            {"ids": valid_ids},
        )
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            alignment = row["alignment"]
            try:
                truth_epoch = int(row["epoch"]) if row["epoch"] is not None else None
            except (TypeError, ValueError):
                truth_epoch = None
            result[row["id"]] = {
                "truth_alignment": list(alignment) if isinstance(alignment, (list, tuple)) else [],
                "truth_epoch": truth_epoch,
            }
        return result

    async def set_node_truth_state(
        self, node_truth_state: Dict[str, Dict[str, Any]]
    ) -> Dict[str, bool]:
        """Persist truth state per node; returns ``{node_id: updated}``."""
        if not node_truth_state:
            return {}
        node_ids = list(node_truth_state.keys())

        items = []
        for node_id, state in node_truth_state.items():
            if not isinstance(node_id, str) or not node_id:
                continue
            epoch = (state or {}).get("truth_epoch")
            items.append(
                {
                    "node_id": node_id,
                    "alignment": list((state or {}).get("truth_alignment") or []),
                    # ⚠ Written only when present: FalkorDB accepts a null
                    # property value and then silently drops it, so writing None
                    # would leave a stale epoch in place while looking like a
                    # successful clear.
                    "epoch": int(epoch) if epoch is not None else None,
                }
            )
        if not items:
            return {node_id: False for node_id in node_ids}

        updated: Set[str] = set()
        with_epoch = [item for item in items if item["epoch"] is not None]
        without_epoch = [item for item in items if item["epoch"] is None]

        if with_epoch:
            rows = await self.query(
                f"""
                UNWIND $items AS item
                MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id = item.node_id
                SET n.truth_alignment = item.alignment,
                    n.truth_epoch = item.epoch,
                    n.updated_at = timestamp()
                RETURN n.id AS node_id
                """,
                {"items": with_epoch},
            )
            updated |= {row["node_id"] for row in rows}

        if without_epoch:
            rows = await self.query(
                f"""
                UNWIND $items AS item
                MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id = item.node_id
                SET n.truth_alignment = item.alignment,
                    n.updated_at = timestamp()
                RETURN n.id AS node_id
                """,
                {"items": without_epoch},
            )
            updated |= {row["node_id"] for row in rows}

        return {node_id: node_id in updated for node_id in node_ids}

    # ------------------------------------------------------------------
    # Whole-graph reads
    # ------------------------------------------------------------------

    async def delete_graph(self) -> None:
        """Remove every node and edge, leaving the indexes in place.

        📌 Not ``GRAPH.DELETE``: dropping the key would take the id indexes with
        it, and nothing on the write path would recreate them — an unindexed
        graph is the silent All-Node-Scan regression again.
        """
        await self.query("MATCH (n) DETACH DELETE n")

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        """Every data node and edge, in the ``(id, properties)`` / 4-tuple shape."""
        node_rows = await self.query(
            f"MATCH (n:{_quote(BASE_LABEL)}) RETURN properties(n) AS properties"
        )
        nodes = [
            (properties["id"], properties)
            for properties in (_strip_provenance(row["properties"]) for row in node_rows)
        ]

        edge_rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})-[r]->(m:{_quote(BASE_LABEL)})
            RETURN n.id AS source, m.id AS target, type(r) AS type, properties(r) AS properties
            """
        )
        edges = []
        for row in edge_rows:
            properties = _strip_provenance(row["properties"]) or {}
            # ⚠ Fall back to the matched endpoints. The seed indexes
            # `properties["source_node_id"]` directly and raises on an edge that
            # lacks it — which is every edge written by anything other than
            # `add_edges`, including the bulk loader that migrates this graph.
            edges.append(
                (
                    properties.get("source_node_id", row["source"]),
                    properties.get("target_node_id", row["target"]),
                    row["type"],
                    properties,
                )
            )

        logger.info("Retrieved %d nodes and %d edges", len(nodes), len(edges))
        return (nodes, edges)

    async def get_neighborhood(
        self,
        node_ids: List[str],
        depth: int = 1,
        edge_types: Optional[List[str]] = None,
    ) -> Tuple[List[Node], List[EdgeData]]:
        """The k-hop subgraph around a set of seed nodes, in ``get_graph_data`` shape."""
        if not node_ids:
            logger.warning("No node IDs provided for neighborhood retrieval.")
            return [], []

        seed_ids = [str(node_id) for node_id in node_ids]
        hops = max(1, int(depth))

        if edge_types:
            path_query = f"""
            MATCH path = (seed:{_quote(BASE_LABEL)})-[*1..{hops}]-(neighbor)
            WHERE seed.id IN $node_ids
              AND ALL(r IN relationships(path) WHERE type(r) IN $edge_types)
            RETURN DISTINCT neighbor.id AS id
            """
            params: Dict[str, Any] = {"node_ids": seed_ids, "edge_types": list(edge_types)}
        else:
            path_query = f"""
            MATCH (seed:{_quote(BASE_LABEL)})-[*1..{hops}]-(neighbor)
            WHERE seed.id IN $node_ids
            RETURN DISTINCT neighbor.id AS id
            """
            params = {"node_ids": seed_ids}

        rows = await self.query(path_query, params)
        all_ids = list({*seed_ids, *(row["id"] for row in rows if row["id"] is not None)})

        node_rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)}) WHERE n.id IN $ids
            RETURN properties(n) AS properties
            """,
            {"ids": all_ids},
        )
        nodes = [
            (properties["id"], properties)
            for properties in (_strip_provenance(row["properties"]) for row in node_rows)
        ]

        edge_rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})-[r]->(m:{_quote(BASE_LABEL)})
            WHERE n.id IN $ids AND m.id IN $ids
            RETURN n.id AS source, m.id AS target, type(r) AS type, properties(r) AS properties
            """,
            {"ids": all_ids},
        )
        edges = []
        for row in edge_rows:
            properties = _strip_provenance(row["properties"]) or {}
            edges.append(
                (
                    properties.get("source_node_id", row["source"]),
                    properties.get("target_node_id", row["target"]),
                    row["type"],
                    properties,
                )
            )

        return (nodes, edges)

    async def get_nodeset_subgraph(
        self, node_type: Type[Any], node_name: List[str], node_name_filter_operator: str = "OR"
    ) -> Tuple[List[Tuple[int, dict]], List[Tuple[int, int, str, dict]]]:
        """The named nodes of one type, their neighbours, and every edge among them.

        ⚠ Rewritten from the seed as four lean queries rather than one statement
        that builds nested maps in its RETURN. FalkorDB accepts a map as a
        returned value, but the seed's version leans on `collect`/`UNWIND`
        gymnastics whose only purpose was to fold it all into a single round
        trip; four indexed queries are easier to read and to explain.
        """
        if not node_name:
            return [], []

        label = _quote(node_type.__name__)
        names = list(node_name)

        primary_rows = await self.query(
            f"MATCH (n:{label}) WHERE n.name IN $names RETURN n.id AS id",
            {"names": names},
        )
        primary_ids = [row["id"] for row in primary_rows]
        if not primary_ids:
            return [], []

        if node_name_filter_operator == "OR":
            neighbor_rows = await self.query(
                f"""
                MATCH (n:{label})--(nbr)
                WHERE n.name IN $names
                RETURN DISTINCT nbr.id AS id
                """,
                {"names": names},
            )
        else:
            # AND: only neighbours adjacent to EVERY matched primary node.
            neighbor_rows = await self.query(
                f"""
                MATCH (n:{label})--(nbr)
                WHERE n.name IN $names
                WITH nbr, count(DISTINCT n) AS matched
                WHERE matched = $primary_count
                RETURN nbr.id AS id
                """,
                {"names": names, "primary_count": len(primary_ids)},
            )

        all_ids = list({*primary_ids, *(row["id"] for row in neighbor_rows if row["id"])})

        node_rows = await self.query(
            "MATCH (n) WHERE n.id IN $ids RETURN properties(n) AS properties",
            {"ids": all_ids},
        )
        nodes = [
            (properties["id"], properties)
            for properties in (_strip_provenance(row["properties"]) for row in node_rows)
        ]

        edge_rows = await self.query(
            """
            MATCH (a)-[r]->(b)
            WHERE a.id IN $ids AND b.id IN $ids
            RETURN a.id AS source, b.id AS target, type(r) AS type, properties(r) AS properties
            """,
            {"ids": all_ids},
        )
        edges = []
        for row in edge_rows:
            properties = _strip_provenance(row["properties"]) or {}
            edges.append(
                (
                    properties.get("source_node_id", row["source"]),
                    properties.get("target_node_id", row["target"]),
                    row["type"],
                    properties,
                )
            )

        return nodes, edges

    async def get_filtered_graph_data(
        self, attribute_filters: List[Dict[str, List[Union[str, int]]]]
    ) -> Tuple[List[Node], List[EdgeData]]:
        """Nodes whose attributes match every filter, and the edges between them.

        ⚠ Values are **parameterized**, unlike the seed, which interpolates them
        into the query text with hand-rolled quoting — one apostrophe in an
        entity name and that query is a syntax error at best.
        """
        if not attribute_filters:
            return [], []

        params: Dict[str, Any] = {}
        node_clauses = []
        edge_clauses = []
        for index, (attribute, values) in enumerate(attribute_filters[0].items()):
            param = f"filter_{index}"
            params[param] = list(values)
            attribute_ref = _quote(attribute)
            node_clauses.append(f"n.{attribute_ref} IN ${param}")
            edge_clauses.append(f"n.{attribute_ref} IN ${param} AND m.{attribute_ref} IN ${param}")

        node_rows = await self.query(
            f"""
            MATCH (n)
            WHERE {" AND ".join(node_clauses)}
            RETURN properties(n) AS properties
            """,
            params,
        )
        nodes = [
            (properties["id"], properties)
            for properties in (_strip_provenance(row["properties"]) for row in node_rows)
        ]

        edge_rows = await self.query(
            f"""
            MATCH (n)-[r]->(m)
            WHERE {" AND ".join(edge_clauses)}
            RETURN n.id AS source, m.id AS target, type(r) AS type, properties(r) AS properties
            """,
            params,
        )
        edges = []
        for row in edge_rows:
            properties = _strip_provenance(row["properties"]) or {}
            edges.append(
                (
                    properties.get("source_node_id", row["source"]),
                    properties.get("target_node_id", row["target"]),
                    row["type"],
                    properties,
                )
            )

        return (nodes, edges)

    async def get_graph_metrics(self, include_optional: bool = False) -> Dict[str, Any]:
        """Graph statistics, with the GDS-only ones honestly reported as unavailable.

        🚨 The seed computes connectivity through `gds.graph.project` and friends.
        FalkorDB has no GDS and the spec drops those three helpers entirely, so
        components are computed here instead: two id-only scans plus union-find,
        which is exact and finishes on a 59k-node graph. Ladybug's alternative —
        an unbounded variable-length traversal per node — would not.

        ⚠ ``diameter``, ``avg_shortest_path_length`` and ``avg_clustering`` stay
        ``-1``: they are all-pairs computations, infeasible at this scale on any
        backend, and ``-1`` is this dict's existing "not computed" convention.
        Do not read a ``-1`` as a measurement.
        """
        node_rows = await self.query(f"MATCH (n:{_quote(BASE_LABEL)}) RETURN n.id AS id")
        edge_rows = await self.query(
            f"""
            MATCH (n:{_quote(BASE_LABEL)})-[r]->(m:{_quote(BASE_LABEL)})
            RETURN n.id AS source, m.id AS target
            """
        )
        node_ids = [row["id"] for row in node_rows]
        edge_pairs = [(row["source"], row["target"]) for row in edge_rows]

        num_nodes = len(node_ids)
        num_edges = len(edge_pairs)
        component_sizes = _connected_components(node_ids, edge_pairs)

        metrics: Dict[str, Any] = {
            "num_nodes": num_nodes,
            "num_edges": num_edges,
            "mean_degree": (2 * num_edges) / num_nodes if num_nodes else None,
            "edge_density": num_edges / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 0,
            "num_connected_components": len(component_sizes),
            "sizes_of_connected_components": component_sizes,
            "num_selfloops": -1,
            "diameter": -1,
            "avg_shortest_path_length": -1,
            "avg_clustering": -1,
        }

        if include_optional:
            metrics["num_selfloops"] = sum(
                1 for source, target in edge_pairs if source == target
            )

        return metrics

    async def get_triplets_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """A page of ``(start_node, relationship_properties, end_node)`` triplets."""
        return await self.query(
            f"""
            MATCH (start_node:{_quote(BASE_LABEL)})-[relationship]->(end_node:{_quote(BASE_LABEL)})
            RETURN properties(start_node) AS start_node,
                   properties(relationship) AS relationship_properties,
                   properties(end_node) AS end_node
            SKIP $offset LIMIT $limit
            """,
            {"offset": int(offset), "limit": int(limit)},
        )
