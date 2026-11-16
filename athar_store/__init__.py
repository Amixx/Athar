"""Persistent baseline / approval / review-history store for Athar.

Turns one-shot semantic diffs into an accumulating per-project record:
content-addressed accepted baselines, immutable approval records, and an
append-only review log. Integration layer only; never imported by the core
engine.
"""

from athar_store.pack import ArtifactPack, PackError
from athar_store.store import (
    BaselineStore,
    EngineError,
    ReviewStateError,
    StaleBaselineError,
    StoreConflictError,
    StoreError,
    UnknownBaselineError,
    UnknownReviewError,
)

__all__ = [
    "ArtifactPack",
    "BaselineStore",
    "EngineError",
    "PackError",
    "ReviewStateError",
    "StaleBaselineError",
    "StoreConflictError",
    "StoreError",
    "UnknownBaselineError",
    "UnknownReviewError",
]
