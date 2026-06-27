"""Quick Stage-A microbenchmark: native vs pure-Python Merkle + WL gossip.

Parses one IFC once, then times compute_merkle_hashes and
compute_topology_hashes through the native accelerator and again with it
forced off. Reports wall time per stage and the speedup, and asserts the two
paths agree. Exploratory only — not part of the test suite.

Usage:
    python scripts/explore/bench_native_stage_a.py <file.ifc>
"""

from __future__ import annotations

import sys
import time

import athar.bottom.merkle as merkle_mod
import athar.bottom.wl_gossip as wl_mod
from athar.bottom._native import native_available
from athar.bottom.edge_policy import build_edge_set
from athar.bottom.merkle import compute_merkle_hashes
from athar.bottom.parser import parse_ifc
from athar.bottom.wl_gossip import compute_topology_hashes


def _timed(label, fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"  {label:28s} {elapsed:8.3f}s")
    return result, elapsed


def main(path: str) -> None:
    if not native_available():
        sys.exit("athar_native not built; run `make native-build` first")

    print(f"parsing {path} ...")
    parsed, parse_s = _timed("parse_ifc", lambda: parse_ifc(path))
    edges = build_edge_set(parsed)
    print(f"  entities={len(parsed.entities)} edges={len(edges)}")

    print("native:")
    merkle_native, m_nat = _timed("merkle", lambda: compute_merkle_hashes(parsed, edges))
    topo_native, t_nat = _timed(
        "topology", lambda: compute_topology_hashes(parsed, edges, merkle_native)
    )

    merkle_mod.native = lambda: None
    wl_mod.native = lambda: None

    print("pure python:")
    merkle_py, m_py = _timed("merkle", lambda: compute_merkle_hashes(parsed, edges))
    topo_py, t_py = _timed(
        "topology", lambda: compute_topology_hashes(parsed, edges, merkle_py)
    )

    assert merkle_native == merkle_py, "merkle mismatch!"
    assert topo_native == topo_py, "topology mismatch!"
    print("parity: OK (native == python)")
    print(
        f"speedup: merkle {m_py / m_nat:5.2f}x   topology {t_py / t_nat:5.2f}x   "
        f"stage-A total {(m_py + t_py) / (m_nat + t_nat):5.2f}x"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
