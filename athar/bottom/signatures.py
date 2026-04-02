"""Phase 1 signature assembly pipeline."""

from __future__ import annotations

from .constants import CANON_VERSION
from .edge_policy import DOMAIN_DATA, DOMAIN_GEOMETRY, edge_stats, build_edge_set
from .merkle import compute_merkle_hashes
from .parser import parse_ifc
from .spatial import build_spatial_features
from .types import SignatureBundle, SignatureVector
from .wl_gossip import compute_topology_hashes


def build_signature_bundle(filepath: str) -> SignatureBundle:
    """Run bottom-layer extraction and return first-class signatures."""
    parsed = parse_ifc(filepath)
    edges = build_edge_set(parsed)
    merkle_hashes = compute_merkle_hashes(parsed, edges)
    topology_hashes = compute_topology_hashes(parsed, edges, merkle_hashes)
    spatial_features = build_spatial_features(parsed, edges)

    signatures: dict[int, SignatureVector] = {}
    for step_id, entity in sorted(parsed.entities.items()):
        if not (entity.is_product or entity.is_spatial):
            continue
        spatial = spatial_features.get(step_id)
        signatures[step_id] = SignatureVector(
            step_id=step_id,
            guid=entity.global_id,
            entity_type=entity.entity_type,
            canonical_class=entity.canonical_class,
            vh_geometry=merkle_hashes.get(step_id, {}).get(DOMAIN_GEOMETRY, ""),
            vh_data=merkle_hashes.get(step_id, {}).get(DOMAIN_DATA, ""),
            vh_topology=topology_hashes.get(step_id, ""),
            placement=spatial.placement if spatial else None,
            centroid=spatial.centroid if spatial else None,
            aabb=spatial.aabb if spatial else None,
            canon_version=CANON_VERSION,
        )

    return SignatureBundle(
        filepath=filepath,
        schema=parsed.schema,
        canon_version=CANON_VERSION,
        signatures=signatures,
        diagnostics=parsed.diagnostics,
        edge_stats=edge_stats(edges),
    )

