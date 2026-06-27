"""Buyer-facing QA presentation for Athar policy gates and review history.

Turns the structured verdicts from :mod:`athar.check` and the event log from
:mod:`athar_store` into human-readable pass/fail reports a BIM reviewer can act
on. Presentation only — it imports engine/integration output, never the reverse.
"""

from .render import render_check_report, render_history

__all__ = ["render_check_report", "render_history"]
