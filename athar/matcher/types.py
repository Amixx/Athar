"""Data contracts for Phase 1 matcher."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CandidatePair:
    old_step: int
    new_step: int
    reason: str


@dataclass(frozen=True)
class ScoredCandidate:
    old_step: int
    new_step: int
    score: float
    reason: str


@dataclass(frozen=True)
class MatchedPair:
    old_step: int
    new_step: int
    score: float
    reason: str

