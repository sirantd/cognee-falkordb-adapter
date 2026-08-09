"""Labels, index families and provenance column names.

Kept apart from the adapter so the migration loader and the ansible role can
import the same names the adapter writes — a bulk loader that groups MERGEs by a
label the adapter does not set produces a graph that looks fine and is unusable.
"""

from __future__ import annotations

# Every node carries TWO labels: this shared one and its real type. The shared
# label is what makes an id lookup index-backed regardless of node type, which is
# the whole fix for the All-Node-Scan defect (#68: 9.6 ms unlabeled vs 2.0 ms
# index-backed, per lookup, multiplied by every node a cognify touches).
#
# Name matches cognee's own Neo4j adapter (BASE_LABEL) so a graph written by
# either is readable by the other.
BASE_LABEL = "__Node__"

# The four provenance fields, stored as NATIVE list properties. FalkorDB stores
# arrays of primitives natively (verified in #68, including nested arrays), so
# these need none of the delimiter encoding ladybug/Kuzu require.
PROVENANCE_COLUMNS = (
    "source_ref_keys",
    "source_dataset_ids",
    "source_run_ids",
    "source_run_refs",
)

# Node types cognee creates. The bulk edge loader groups MERGEs by each
# endpoint's real type label, so each of these needs its own (id) index or the
# load degrades to an All-Node-Scan per edge — the difference #68 measured
# between an 84.5 s migration and a 2,114 s one.
NODE_TYPE_LABELS = (
    "Entity",
    "EntityType",
    "DocumentChunk",
    "TextDocument",
    "TextSummary",
    "NodeSet",
)

# Graph-level metadata (provenance version / delete mode) has nowhere natural to
# live in a property graph, so it goes on a singleton node carrying this label.
# Excluded from every domain query by construction — it never gets BASE_LABEL.
METADATA_LABEL = "__GraphMetadata__"
METADATA_NODE_ID = "__cognee_graph_metadata__"
