"""Register the adapter with cognee as a GRAPH provider — and only a graph one.

🚨 This module must never bind a vector adapter. The community FalkorDB package
binds one class as both the graph and the vector provider at import time, which
would put embeddings in FalkorDB. This deployment's vectors live in pgvector on
an external Postgres, with the column width LOCKED at 1024 by ``baai/bge-m3``;
moving them is a re-embedding of the whole corpus, not a config change.

Importing this module is the whole registration. It is idempotent.
"""

from __future__ import annotations

import os

from cognee.infrastructure.databases.graph.use_graph_adapter import use_graph_adapter

from .adapter import FalkorDBAdapter

# Both spellings: the ansible role and cognee's own config have historically
# disagreed about which one names this backend, and a typo'd provider silently
# falls back to the default rather than failing.
PROVIDER_NAMES = ("falkor", "falkordb")


def ensure_registered(assert_vector_provider: bool = True) -> None:
    """Bind FalkorDBAdapter under both provider names.

    ``assert_vector_provider`` fails loudly if something has pointed the VECTOR
    store at FalkorDB. That is a guard against the exact failure mode above, and
    it belongs here rather than in a deploy check because by the time cognee is
    running it is already too late to notice cheaply.
    """
    for name in PROVIDER_NAMES:
        use_graph_adapter(name, FalkorDBAdapter)

    if assert_vector_provider:
        vector_provider = os.getenv("VECTOR_DB_PROVIDER", "").strip().lower()
        if vector_provider and vector_provider not in ("pgvector", "postgres"):
            raise RuntimeError(
                "cognee-falkordb-adapter is a GRAPH adapter only, but "
                f"VECTOR_DB_PROVIDER is {vector_provider!r}. Embeddings must stay "
                "on pgvector — this deployment's vector column is locked at 1024 "
                "dimensions by baai/bge-m3."
            )


ensure_registered()
