"""A provenance-complete FalkorDB graph adapter for cognee.

Importing this package does NOT register the adapter — import
``cognee_falkordb_adapter.register`` for that, so registration is an explicit
act rather than an import side effect.
"""

from .adapter import FalkorDBAdapter
from .constants import BASE_LABEL, NODE_TYPE_LABELS, PROVENANCE_COLUMNS

__all__ = ["FalkorDBAdapter", "BASE_LABEL", "NODE_TYPE_LABELS", "PROVENANCE_COLUMNS"]
__version__ = "0.1.0"
