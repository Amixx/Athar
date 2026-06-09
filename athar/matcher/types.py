"""Data contracts for matcher."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MatchedPair:
    old_step: int
    new_step: int
    score: float
    reason: str
