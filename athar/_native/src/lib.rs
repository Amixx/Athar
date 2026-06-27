//! Native acceleration for Athar's bottom signature pipeline.
//!
//! Two entry points:
//!
//!   * Stage A — `compute_merkle_hashes` / `compute_topology_hashes`: drop-in,
//!     byte-identical ports of the two CPU-bound hashing stages, fed by the
//!     Python pipeline.
//!   * Stage B — `build_signature_bundle`: the full native parse path. Rust
//!     tokenizes the STEP bytes, canonicalizes against a schema descriptor map
//!     (built once by Python from ifcopenshell, file-independently), classifies
//!     edges, and runs Merkle + WL + spatial, returning only the product/
//!     spatial signature vectors. The ~1M mesh-primitive entities never become
//!     Python objects (the memory win) and ifcopenshell never parses the file
//!     (the time win). Stage B is intentionally NOT byte-identical with the old
//!     Python output; it defines its own canonical form.

use std::collections::{HashMap, HashSet, VecDeque};

use pyo3::prelude::*;
use sha2::{Digest, Sha256};

mod canon;
mod descriptor;
mod edges;
mod spatial;
mod step;

/// Lowercase hex sha256, matching `hashlib.sha256(...).hexdigest()`.
fn sha256_hex(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Merkle pass
// ---------------------------------------------------------------------------

/// Recursive per-domain entity hash. Mirrors `merkle._hash_entity`.
///
/// `cache` memoizes completed hashes; `visiting` is the active DFS stack used
/// for cycle detection; `cycles` records `(domain, step_id)` back-edge events
/// in traversal order so Python can reproduce the diagnostics counters.
#[allow(clippy::too_many_arguments)]
fn hash_entity(
    step_id: i64,
    domain: &str,
    classes: &HashMap<i64, String>,
    parts: &HashMap<i64, Vec<String>>,
    adj: &HashMap<i64, Vec<(String, i64)>>,
    cache: &mut HashMap<i64, String>,
    visiting: &mut HashSet<i64>,
    cycles: &mut Vec<(String, i64)>,
) -> String {
    if let Some(cached) = cache.get(&step_id) {
        return cached.clone();
    }

    if visiting.contains(&step_id) {
        cycles.push((domain.to_string(), step_id));
        let digest = sha256_hex(format!("cycle:{domain}:{step_id}").as_bytes());
        cache.insert(step_id, digest.clone());
        return digest;
    }

    let class = match classes.get(&step_id) {
        Some(class) => class,
        None => {
            // Adjacency referenced a step id with no parsed entity.
            let digest = sha256_hex(format!("missing:{domain}:{step_id}").as_bytes());
            cache.insert(step_id, digest.clone());
            return digest;
        }
    };

    visiting.insert(step_id);

    let mut payload_parts: Vec<String> = Vec::new();
    payload_parts.push(format!("class={class}"));
    if let Some(entity_parts) = parts.get(&step_id) {
        payload_parts.extend(entity_parts.iter().cloned());
    }

    let mut child_entries: Vec<(String, String)> = Vec::new();
    if let Some(children) = adj.get(&step_id) {
        for (label, child_step) in children {
            let child_hash =
                hash_entity(*child_step, domain, classes, parts, adj, cache, visiting, cycles);
            child_entries.push((label.clone(), child_hash));
        }
    }
    // Re-sort by (label, child_hash); identical to Python's
    // child_entries.sort(key=lambda item: (item[0], item[1])).
    child_entries.sort();
    for (label, child_hash) in &child_entries {
        payload_parts.push(format!("edge={label}:{child_hash}"));
    }

    let payload = payload_parts.join("\u{1f}");
    let digest = sha256_hex(payload.as_bytes());
    cache.insert(step_id, digest.clone());
    visiting.remove(&step_id);
    digest
}

fn merkle_compute(
    classes: &HashMap<i64, String>,
    geom_parts: &HashMap<i64, Vec<String>>,
    data_parts: &HashMap<i64, Vec<String>>,
    geom_adj: &HashMap<i64, Vec<(String, i64)>>,
    data_adj: &HashMap<i64, Vec<(String, i64)>>,
) -> (HashMap<i64, (String, String)>, Vec<(String, i64)>) {
    let mut sorted_ids: Vec<i64> = classes.keys().copied().collect();
    sorted_ids.sort_unstable();

    let mut geom_cache: HashMap<i64, String> = HashMap::with_capacity(classes.len());
    let mut data_cache: HashMap<i64, String> = HashMap::with_capacity(classes.len());
    let mut geom_visiting: HashSet<i64> = HashSet::new();
    let mut data_visiting: HashSet<i64> = HashSet::new();
    let mut cycles: Vec<(String, i64)> = Vec::new();
    let mut out: HashMap<i64, (String, String)> = HashMap::with_capacity(classes.len());

    // Interleave geometry then data per step id so the cycle-event order
    // matches the Python outer loop exactly.
    for &step_id in &sorted_ids {
        let geom = hash_entity(
            step_id,
            "geometry",
            classes,
            geom_parts,
            geom_adj,
            &mut geom_cache,
            &mut geom_visiting,
            &mut cycles,
        );
        let data = hash_entity(
            step_id,
            "data",
            classes,
            data_parts,
            data_adj,
            &mut data_cache,
            &mut data_visiting,
            &mut cycles,
        );
        out.insert(step_id, (geom, data));
    }

    (out, cycles)
}

/// Compute per-entity `(vh_geometry, vh_data)` Merkle hashes.
///
/// Inputs (all keyed by STEP id):
///   * `classes`    — canonical class string per parsed entity.
///   * `geom_parts` / `data_parts` — domain-filtered, JSON-encoded attribute
///     parts (`attr=Name:encoded`), already produced by Python so the
///     canonicalization is byte-identical. Entities with no parts may be
///     omitted from these maps.
///   * `geom_adj` / `data_adj` — include-edge adjacency, `step -> [(label,
///     child)]`.
///
/// Returns `(hashes, cycles)` where `hashes[step] = (vh_geometry, vh_data)`
/// and `cycles` is the ordered list of `(domain, step)` back-edge events.
#[pyfunction]
fn compute_merkle_hashes(
    py: Python<'_>,
    classes: HashMap<i64, String>,
    geom_parts: HashMap<i64, Vec<String>>,
    data_parts: HashMap<i64, Vec<String>>,
    geom_adj: HashMap<i64, Vec<(String, i64)>>,
    data_adj: HashMap<i64, Vec<(String, i64)>>,
) -> PyResult<(HashMap<i64, (String, String)>, Vec<(String, i64)>)> {
    // The compute is pure Rust over owned data, so release the GIL.
    let result = py.allow_threads(|| {
        merkle_compute(&classes, &geom_parts, &data_parts, &geom_adj, &data_adj)
    });
    Ok(result)
}

// ---------------------------------------------------------------------------
// WL topology gossip
// ---------------------------------------------------------------------------

/// BFS neighborhood up to `depth` hops (inclusive of `start`).
/// Mirrors `wl_gossip._neighbors_within_k`.
fn neighbors_within_k(adj: &HashMap<i64, Vec<i64>>, start: i64, depth: i64) -> HashSet<i64> {
    if depth <= 0 {
        let mut single = HashSet::with_capacity(1);
        single.insert(start);
        return single;
    }
    let mut seen: HashSet<i64> = HashSet::new();
    seen.insert(start);
    let mut queue: VecDeque<(i64, i64)> = VecDeque::new();
    queue.push_back((start, 0));
    while let Some((node, d)) = queue.pop_front() {
        if d >= depth {
            continue;
        }
        if let Some(neighbors) = adj.get(&node) {
            for &next in neighbors {
                if seen.contains(&next) {
                    continue;
                }
                seen.insert(next);
                queue.push_back((next, d + 1));
            }
        }
    }
    seen
}

fn topology_compute(
    seeds: &HashMap<i64, String>,
    context_adj: &HashMap<i64, Vec<i64>>,
    spatial_adj: &HashMap<i64, Vec<i64>>,
    context_k: i64,
    spatial_k: i64,
) -> HashMap<i64, String> {
    let mut sorted_ids: Vec<i64> = seeds.keys().copied().collect();
    sorted_ids.sort_unstable();

    let mut out: HashMap<i64, String> = HashMap::with_capacity(seeds.len());
    for &step_id in &sorted_ids {
        let context_neighbors = neighbors_within_k(context_adj, step_id, context_k);
        let spatial_neighbors = neighbors_within_k(spatial_adj, step_id, spatial_k);

        let mut tokens: Vec<String> = Vec::new();
        for neigh in &context_neighbors {
            if *neigh == step_id {
                continue;
            }
            let seed = seeds.get(neigh).map(String::as_str).unwrap_or("");
            tokens.push(format!("context:{seed}"));
        }
        for neigh in &spatial_neighbors {
            if *neigh == step_id {
                continue;
            }
            let seed = seeds.get(neigh).map(String::as_str).unwrap_or("");
            tokens.push(format!("spatial:{seed}"));
        }
        tokens.sort();

        let self_seed = seeds.get(&step_id).map(String::as_str).unwrap_or("");
        let mut payload_parts: Vec<String> = Vec::with_capacity(tokens.len() + 1);
        payload_parts.push(format!("self:{self_seed}"));
        payload_parts.extend(tokens);
        let payload = payload_parts.join("\u{1f}");
        out.insert(step_id, sha256_hex(payload.as_bytes()));
    }
    out
}

/// Compute `vh_topology` per entity from context + spatial neighbor gossip.
///
/// `seeds[step] = sha256("class|vh_geometry|vh_data")` is precomputed in
/// Python (cheap, one hash each); Rust does the expensive BFS expansion and
/// token assembly. `*_adj` are undirected neighbor lists.
#[pyfunction]
#[pyo3(signature = (seeds, context_adj, spatial_adj, context_k=1, spatial_k=2))]
fn compute_topology_hashes(
    py: Python<'_>,
    seeds: HashMap<i64, String>,
    context_adj: HashMap<i64, Vec<i64>>,
    spatial_adj: HashMap<i64, Vec<i64>>,
    context_k: i64,
    spatial_k: i64,
) -> PyResult<HashMap<i64, String>> {
    let result = py.allow_threads(|| {
        topology_compute(&seeds, &context_adj, &spatial_adj, context_k, spatial_k)
    });
    Ok(result)
}

// ---------------------------------------------------------------------------
// Stage B: full native signature bundle
// ---------------------------------------------------------------------------

/// One product/spatial signature, flattened for the FFI boundary:
/// (step_id, guid, name, entity_type, canonical_class, vh_geometry, vh_data,
///  vh_topology, placement, centroid, aabb, data_facts).
type SigTuple = (
    i64,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
    String,
    Option<Vec<i64>>,
    Option<Vec<f64>>,
    Option<Vec<f64>>,
    Vec<(String, String)>,
);

struct NativeBundle {
    signatures: Vec<SigTuple>,
    edge_stats: HashMap<String, i64>,
    dangling_refs: i64,
    cycle_breaks: i64,
    warnings: Vec<String>,
}

fn build_bundle(
    path: &str,
    schema_json: &str,
    unit_factors: &HashMap<String, f64>,
) -> Result<NativeBundle, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    let records = step::Parser::new(&bytes).parse()?;
    let schema = descriptor::parse_schema(schema_json)?;
    let parsed = canon::canonicalize(&records, &schema, unit_factors);
    let edge_list = edges::build_edges(&parsed.entities, &parsed.id_to_keyword);

    // ---- Merkle inputs -----------------------------------------------------
    let mut classes: HashMap<i64, String> = HashMap::with_capacity(parsed.entities.len());
    let mut geom_parts: HashMap<i64, Vec<String>> = HashMap::new();
    let mut data_parts: HashMap<i64, Vec<String>> = HashMap::new();
    for e in &parsed.entities {
        classes.insert(e.step_id, e.canonical_class.clone());
        if !e.geom_parts.is_empty() {
            geom_parts.insert(e.step_id, e.geom_parts.clone());
        }
        if !e.data_parts.is_empty() {
            data_parts.insert(e.step_id, e.data_parts.clone());
        }
    }
    let mut geom_adj: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
    let mut data_adj: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
    let mut geometry_adj: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut data_children: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut context_adj: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut spatial_adj: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut edge_stats: HashMap<String, i64> = HashMap::new();
    for edge in &edge_list {
        *edge_stats
            .entry(format!("{}:{}", edge.classification, edge.domain))
            .or_insert(0) += 1;
        if edge.classification == edges::INCLUDE {
            match edge.domain {
                edges::GEOMETRY => {
                    geom_adj
                        .entry(edge.source)
                        .or_default()
                        .push((edge.label.clone(), edge.target));
                    geometry_adj.entry(edge.source).or_default().push(edge.target);
                }
                edges::DATA => {
                    data_adj
                        .entry(edge.source)
                        .or_default()
                        .push((edge.label.clone(), edge.target));
                    data_children.entry(edge.source).or_default().push(edge.target);
                }
                edges::SPATIAL => {
                    spatial_adj.entry(edge.source).or_default().insert(edge.target);
                    spatial_adj.entry(edge.target).or_default().insert(edge.source);
                }
                _ => {}
            }
        } else if edge.classification == edges::CONTEXT && edge.domain == edges::TOPOLOGY {
            context_adj.entry(edge.source).or_default().insert(edge.target);
            context_adj.entry(edge.target).or_default().insert(edge.source);
        }
    }
    for v in geom_adj.values_mut() {
        v.sort();
    }
    for v in data_adj.values_mut() {
        v.sort();
    }
    for v in geometry_adj.values_mut() {
        v.sort();
    }

    let (merkle_out, cycles) = merkle_compute(&classes, &geom_parts, &data_parts, &geom_adj, &data_adj);

    // ---- WL topology -------------------------------------------------------
    let mut seeds: HashMap<i64, String> = HashMap::with_capacity(parsed.entities.len());
    for e in &parsed.entities {
        let vh_geom = merkle_out
            .get(&e.step_id)
            .map(|(g, _)| g.as_str())
            .unwrap_or("");
        seeds.insert(
            e.step_id,
            sha256_hex(format!("{}|{}", e.canonical_class, vh_geom).as_bytes()),
        );
    }
    let context_vec: HashMap<i64, Vec<i64>> = sorted_adj(context_adj);
    let spatial_vec: HashMap<i64, Vec<i64>> = sorted_adj(spatial_adj);
    let topo = topology_compute(&seeds, &context_vec, &spatial_vec, 1, 2);

    // ---- Spatial -----------------------------------------------------------
    let by_id: HashMap<i64, &canon::Entity> =
        parsed.entities.iter().map(|e| (e.step_id, e)).collect();
    // Data-closure children are ordered by (target class, target), matching
    // signatures.build_data_facts.
    for v in data_children.values_mut() {
        v.sort_by(|&a, &b| {
            let ca = by_id.get(&a).map(|e| e.canonical_class.as_str()).unwrap_or("");
            let cb = by_id.get(&b).map(|e| e.canonical_class.as_str()).unwrap_or("");
            ca.cmp(cb).then(a.cmp(&b))
        });
    }
    let ctx = spatial::SpatialCtx {
        by_id: &by_id,
        geometry_adj: &geometry_adj,
        point_coords: &parsed.point_coords,
        dir_ratios: &parsed.dir_ratios,
    };
    let spatial_feats = spatial::build_spatial_features(&parsed.entities, &ctx);

    // ---- Assemble signatures ----------------------------------------------
    let mut signatures: Vec<SigTuple> = Vec::new();
    for e in &parsed.entities {
        if !(e.is_product || e.is_spatial) {
            continue;
        }
        let (vh_geometry, vh_data) = merkle_out
            .get(&e.step_id)
            .cloned()
            .unwrap_or_default();
        let vh_topology = topo.get(&e.step_id).cloned().unwrap_or_default();
        let feat = spatial_feats.get(&e.step_id);
        let placement = feat.and_then(|f| f.placement.clone());
        let centroid = feat.and_then(|f| f.centroid.map(|c| c.to_vec()));
        let aabb = feat.and_then(|f| f.aabb.map(|a| a.to_vec()));
        let data_facts = data_facts_for_root(e.step_id, &data_children, &by_id);
        signatures.push((
            e.step_id,
            e.guid.clone(),
            e.name.clone(),
            e.keyword.clone(),
            e.canonical_class.clone(),
            vh_geometry,
            vh_data,
            vh_topology,
            placement,
            centroid,
            aabb,
            data_facts,
        ));
    }
    signatures.sort_by_key(|s| s.0);

    // ---- Diagnostics -------------------------------------------------------
    let mut dangling_refs: i64 = 0;
    for e in &parsed.entities {
        for r in &e.refs {
            if !parsed.id_to_keyword.contains_key(&r.target) {
                dangling_refs += 1;
            }
        }
    }
    let mut warnings: Vec<String> = parsed
        .unknown_keywords
        .iter()
        .map(|kw| format!("No schema descriptor for {kw}; entities of this class were skipped."))
        .collect();
    warnings.sort();

    Ok(NativeBundle {
        signatures,
        edge_stats,
        dangling_refs,
        cycle_breaks: cycles.len() as i64,
        warnings,
    })
}

/// Assemble the deduped data facts for one signature root by walking its data
/// include-closure. Mirrors `signatures.build_data_facts` per root.
fn data_facts_for_root(
    root: i64,
    data_children: &HashMap<i64, Vec<i64>>,
    by_id: &HashMap<i64, &canon::Entity>,
) -> Vec<(String, String)> {
    // Pre-order DFS over the data closure (preserves child order).
    fn visit(
        step: i64,
        data_children: &HashMap<i64, Vec<i64>>,
        seen: &mut HashSet<i64>,
        ordered: &mut Vec<i64>,
    ) {
        if !seen.insert(step) {
            return;
        }
        ordered.push(step);
        if let Some(children) = data_children.get(&step) {
            for &c in children {
                visit(c, data_children, seen, ordered);
            }
        }
    }
    let mut ordered: Vec<i64> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::new();
    visit(root, data_children, &mut seen, &mut ordered);

    let mut facts: Vec<(String, String)> = Vec::new();
    for step in ordered {
        if let Some(ent) = by_id.get(&step) {
            let label = fact_label(ent);
            for (attr, value) in &ent.data_facts {
                facts.push((format!("{label}.{attr}"), value.clone()));
            }
        }
    }
    dedupe_fact_paths(facts)
}

fn fact_label(ent: &canon::Entity) -> String {
    match &ent.guid {
        Some(g) => format!("{}[GlobalId={}]", ent.canonical_class, g),
        None => ent.canonical_class.clone(),
    }
}

/// Sort + disambiguate repeated paths with `[n]` suffixes, matching
/// `signatures._dedupe_fact_paths`.
fn dedupe_fact_paths(mut facts: Vec<(String, String)>) -> Vec<(String, String)> {
    facts.sort();
    let mut counts: HashMap<&str, i32> = HashMap::new();
    for (path, _) in &facts {
        *counts.entry(path.as_str()).or_insert(0) += 1;
    }
    let mut seen: HashMap<String, i32> = HashMap::new();
    let mut out: Vec<(String, String)> = Vec::with_capacity(facts.len());
    for (path, value) in &facts {
        if counts.get(path.as_str()).copied().unwrap_or(0) == 1 {
            out.push((path.clone(), value.clone()));
        } else {
            let n = seen.entry(path.clone()).or_insert(0);
            *n += 1;
            out.push((format!("{path}[{n}]"), value.clone()));
        }
    }
    out
}

fn sorted_adj(adj: HashMap<i64, HashSet<i64>>) -> HashMap<i64, Vec<i64>> {
    adj.into_iter()
        .map(|(k, set)| {
            let mut v: Vec<i64> = set.into_iter().collect();
            v.sort_unstable();
            (k, v)
        })
        .collect()
}

/// Full native parse + signature build for one IFC file.
///
/// `schema_json` is the per-class descriptor map (built once by Python from
/// ifcopenshell); `unit_factors` maps unit types to scale factors. Returns
/// `(signatures, edge_stats, (dangling_refs, cycle_breaks, warnings))`.
#[pyfunction]
fn build_signature_bundle(
    py: Python<'_>,
    path: String,
    schema_json: String,
    unit_factors: HashMap<String, f64>,
) -> PyResult<(Vec<SigTuple>, HashMap<String, i64>, (i64, i64, Vec<String>))> {
    let bundle = py
        .allow_threads(|| build_bundle(&path, &schema_json, &unit_factors))
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    Ok((
        bundle.signatures,
        bundle.edge_stats,
        (bundle.dangling_refs, bundle.cycle_breaks, bundle.warnings),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classes() -> HashMap<i64, String> {
        let mut c = HashMap::new();
        c.insert(1i64, "IfcWall".to_string());
        c.insert(2i64, "IfcPropertySet".to_string());
        c
    }

    #[test]
    fn merkle_is_deterministic_and_content_sensitive() {
        let classes = classes();
        let geom_parts: HashMap<i64, Vec<String>> = HashMap::new();
        let geom_adj: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
        let mut data_parts: HashMap<i64, Vec<String>> = HashMap::new();
        data_parts.insert(2, vec!["attr=Name:s'Pset".to_string()]);
        let mut data_adj: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
        data_adj.insert(1, vec![("IFCRELDEFINESBYPROPERTIES".to_string(), 2)]);

        let (a, _) = merkle_compute(&classes, &geom_parts, &data_parts, &geom_adj, &data_adj);
        let (b, _) = merkle_compute(&classes, &geom_parts, &data_parts, &geom_adj, &data_adj);
        assert_eq!(a, b, "merkle must be deterministic");

        // Editing a child's data part must change the parent's vh_data.
        let mut data_parts2 = data_parts.clone();
        data_parts2.insert(2, vec!["attr=Name:s'Changed".to_string()]);
        let (c, _) = merkle_compute(&classes, &geom_parts, &data_parts2, &geom_adj, &data_adj);
        assert_ne!(a.get(&1).unwrap().1, c.get(&1).unwrap().1);
    }

    #[test]
    fn topology_is_deterministic_and_neighbor_order_independent() {
        let mut seeds: HashMap<i64, String> = HashMap::new();
        seeds.insert(1, "s1".to_string());
        seeds.insert(2, "s2".to_string());
        seeds.insert(3, "s3".to_string());
        let spatial: HashMap<i64, Vec<i64>> = HashMap::new();

        let mut ctx_a: HashMap<i64, Vec<i64>> = HashMap::new();
        ctx_a.insert(1, vec![2, 3]);
        let mut ctx_b: HashMap<i64, Vec<i64>> = HashMap::new();
        ctx_b.insert(1, vec![3, 2]); // reversed neighbor order

        let h_a = topology_compute(&seeds, &ctx_a, &spatial, 1, 2);
        let h_b = topology_compute(&seeds, &ctx_b, &spatial, 1, 2);
        let h_a2 = topology_compute(&seeds, &ctx_a, &spatial, 1, 2);

        assert_eq!(h_a, h_a2, "topology must be deterministic");
        assert_eq!(h_a.get(&1), h_b.get(&1), "neighbor token order must not matter");
    }
}

#[pymodule]
fn athar_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(compute_merkle_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(compute_topology_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(build_signature_bundle, m)?)?;
    Ok(())
}
