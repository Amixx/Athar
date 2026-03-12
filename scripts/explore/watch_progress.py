"""Tail a benchmark progress sidecar JSON and print concise live status lines."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import time
from typing import Any


def _load_progress(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _format_progress_line(snapshot: dict[str, Any]) -> str:
    parts: list[str] = [f"[{datetime.now().strftime('%H:%M:%S')}]"]
    state = snapshot.get("state")
    if isinstance(state, str):
        parts.append(f"state={state}")

    completed = snapshot.get("completed_cases")
    total = snapshot.get("total_cases")
    if isinstance(completed, int) and isinstance(total, int):
        parts.append(f"cases={completed}/{total}")

    current_case = snapshot.get("current_case")
    if isinstance(current_case, dict):
        name = current_case.get("name")
        metric = current_case.get("metric")
        phase = current_case.get("phase")
        if isinstance(name, str):
            parts.append(f"case={name}")
        if isinstance(metric, str):
            parts.append(f"metric={metric}")
        if isinstance(phase, str):
            parts.append(f"phase={phase}")
        probe = current_case.get("probe")
        if isinstance(probe, dict):
            stage = probe.get("stage")
            status = probe.get("status")
            if isinstance(stage, str):
                parts.append(f"stage={stage}")
            if isinstance(status, str):
                parts.append(f"status={status}")
        progress = current_case.get("progress_fraction")
        if isinstance(progress, (int, float)):
            parts.append(f"progress~{float(progress) * 100.0:.1f}%")
        eta_text = current_case.get("eta_text")
        if isinstance(eta_text, str):
            parts.append(f"eta~{eta_text}")

    error = snapshot.get("error")
    if isinstance(error, str) and error:
        parts.append(f"error={error}")

    return " ".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch benchmark progress sidecar JSON.")
    parser.add_argument("--file", required=True, help="Path to progress sidecar JSON.")
    parser.add_argument("--interval-s", type=float, default=2.0, help="Polling interval in seconds.")
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Keep watching after terminal state (completed/failed) until interrupted.",
    )
    args = parser.parse_args()

    if args.interval_s <= 0:
        raise ValueError("--interval-s must be > 0")

    path = Path(args.file)
    last_rendered: str | None = None
    while True:
        snapshot = _load_progress(path)
        if snapshot is not None:
            rendered = json.dumps(snapshot, sort_keys=True)
            if rendered != last_rendered:
                print(_format_progress_line(snapshot), flush=True)
                last_rendered = rendered
            state = snapshot.get("state")
            if state in {"completed", "failed"} and not args.follow:
                break
        time.sleep(args.interval_s)


if __name__ == "__main__":
    main()
