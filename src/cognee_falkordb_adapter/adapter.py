"""FalkorDB graph adapter for cognee.

Seeded from cognee's in-core Neo4j adapter, which is the right starting point:
it is already Cypher, it already stores the four provenance fields as native
list properties, and its provenance-fold clause is portable Cypher
(``coalesce`` / ``CASE`` / list concatenation / ``IN``) that FalkorDB supports.

Scope is 40 methods, not the interface's full 48 — see the spec. The
feedback- and frequency-weight surface is left inheriting the interface's
raising defaults because nothing in cognee's runtime calls it.

⚠ Stage A1 is the scaffold: connection lifecycle and the index families are
real, everything else raises ``NotImplementedError`` tagged with the stage that
fills it in. ``tests/test_surface.py`` reports the burn-down.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

from falkordb.asyncio import FalkorDB

from cognee.infrastructure.databases.graph.graph_db_interface import GraphDBInterface
from cognee.infrastructure.databases.provenance import (
    EdgeDeleteData,
    EdgeIdentity,
    NodeDeleteData,
)
from cognee.infrastructure.engine import DataPoint
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

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def query(self, query: str, params: Optional[dict] = None) -> List[Any]:
        """Run a Cypher query and return its result set as a list of rows."""
        result = await self._graph.query(query, params or {})
        return result.result_set

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
    # Present on every cognee adapter, absent from GraphDBInterface.
    # The provenance contract suite calls it, so it is part of the target.
    # ------------------------------------------------------------------

    async def has_node(self, node_id: str) -> bool:
        """TODO(A2) — contract-suite surface, not declared on the interface."""
        raise NotImplementedError("has_node: stage A2")

    # ------------------------------------------------------------------
    # Interface surface — generated from GraphDBInterface so the signatures
    # cannot drift. Regenerate rather than hand-edit a signature.
    # ------------------------------------------------------------------

    async def is_empty(self) -> bool:
        """TODO(A2) — abstract."""
        raise NotImplementedError("is_empty: stage A2")

    async def add_node(self, node: Union[DataPoint, str], properties: Optional[Dict[str, Any]]=None) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("add_node: stage A2")

    async def add_nodes(self, nodes: Union[List[Node], List[DataPoint]], source_ref_key: Optional[str]=None, pipeline_run_id: Optional[str]=None) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("add_nodes: stage A2")

    async def delete_node(self, node_id: str) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("delete_node: stage A2")

    async def delete_nodes(self, node_ids: List[str]) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("delete_nodes: stage A2")

    async def attach_node_source_refs(self, node_ids: list[str], source_ref_keys: list[str], pipeline_run_id: str | None=None) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("attach_node_source_refs: stage A2")

    async def attach_edge_source_refs(self, edges: list[EdgeIdentity], source_ref_keys: list[str], pipeline_run_id: str | None=None) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("attach_edge_source_refs: stage A2")

    async def remove_node_source_refs(self, node_ids: list[str], source_ref_keys: list[str]) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("remove_node_source_refs: stage A2")

    async def remove_edge_source_refs(self, edges: list[EdgeIdentity], source_ref_keys: list[str]) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("remove_edge_source_refs: stage A2")

    async def delete_edge_triples(self, edges: list[EdgeIdentity]) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("delete_edge_triples: stage A2")

    async def get_node_delete_data(self, node_ids: list[str]) -> dict[str, NodeDeleteData]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("get_node_delete_data: stage A2")

    async def get_edge_delete_data(self, edges: list[EdgeIdentity]) -> dict[EdgeIdentity, EdgeDeleteData]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("get_edge_delete_data: stage A2")

    async def find_nodes_by_source_ref(self, source_ref_key: str) -> list[str]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_nodes_by_source_ref: stage A2")

    async def find_edges_by_source_ref(self, source_ref_key: str) -> list[EdgeIdentity]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_edges_by_source_ref: stage A2")

    async def find_node_source_refs_by_dataset(self, dataset_id: str) -> dict[str, list[str]]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_node_source_refs_by_dataset: stage A2")

    async def find_edge_source_refs_by_dataset(self, dataset_id: str) -> dict[EdgeIdentity, list[str]]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_edge_source_refs_by_dataset: stage A2")

    async def find_node_source_refs_by_pipeline_run(self, pipeline_run_id: str) -> dict[str, list[str]]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_node_source_refs_by_pipeline_run: stage A2")

    async def find_edge_source_refs_by_pipeline_run(self, pipeline_run_id: str) -> dict[EdgeIdentity, list[str]]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("find_edge_source_refs_by_pipeline_run: stage A2")

    async def set_graph_metadata(self, metadata: dict[str, str]) -> None:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("set_graph_metadata: stage A2")

    async def get_graph_metadata(self) -> dict[str, str]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("get_graph_metadata: stage A2")

    async def get_node(self, node_id: str) -> Optional[NodeData]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_node: stage A2")

    async def get_nodes(self, node_ids: List[str]) -> List[NodeData]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_nodes: stage A2")

    async def add_edge(self, source_id: str, target_id: str, relationship_name: str, properties: Optional[Dict[str, Any]]=None) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("add_edge: stage A2")

    async def add_edges(self, edges: Union[List[EdgeData], List[Tuple[str, str, str, Optional[Dict[str, Any]]]]], source_ref_key: Optional[str]=None, pipeline_run_id: Optional[str]=None) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("add_edges: stage A2")

    async def delete_graph(self) -> None:
        """TODO(A2) — abstract."""
        raise NotImplementedError("delete_graph: stage A2")

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_graph_data: stage A2")

    async def get_graph_metrics(self, include_optional: bool=False) -> Dict[str, Any]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_graph_metrics: stage A2")

    async def has_edge(self, source_id: str, target_id: str, relationship_name: str) -> bool:
        """TODO(A2) — abstract."""
        raise NotImplementedError("has_edge: stage A2")

    async def has_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("has_edges: stage A2")

    async def get_edges(self, node_id: str) -> List[EdgeData]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_edges: stage A2")

    async def get_neighbors(self, node_id: str) -> List[NodeData]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_neighbors: stage A2")

    async def get_nodeset_subgraph(self, node_type: Type[Any], node_name: List[str], node_name_filter_operator: str='OR') -> Tuple[List[Tuple[int, dict]], List[Tuple[int, int, str, dict]]]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_nodeset_subgraph: stage A2")

    async def get_connections(self, node_id: Union[str, UUID]) -> List[Tuple[NodeData, Dict[str, Any], NodeData]]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_connections: stage A2")

    async def get_neighborhood(self, node_ids: List[str], depth: int=1, edge_types: Optional[List[str]]=None) -> Tuple[List[Node], List[EdgeData]]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_neighborhood: stage A2")

    async def get_filtered_graph_data(self, attribute_filters: List[Dict[str, List[Union[str, int]]]]) -> Tuple[List[Node], List[EdgeData]]:
        """TODO(A2) — abstract."""
        raise NotImplementedError("get_filtered_graph_data: stage A2")

    async def get_node_truth_state(self, node_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """TODO(A2*) — runtime-reached."""
        raise NotImplementedError("get_node_truth_state: stage A2*")

    async def set_node_truth_state(self, node_truth_state: Dict[str, Dict[str, Any]]) -> Dict[str, bool]:
        """TODO(A2*) — runtime-reached."""
        raise NotImplementedError("set_node_truth_state: stage A2*")

    async def get_triplets_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """TODO(A2) — runtime-reached."""
        raise NotImplementedError("get_triplets_batch: stage A2")

