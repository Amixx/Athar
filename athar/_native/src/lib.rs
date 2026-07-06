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
// Per-stage memory/time profiler
// ---------------------------------------------------------------------------
//
// `build_bundle` is one opaque call from Python's perspective, so the
// `profile_memory.py` phase view bottoms out at "native_build". This breaks that
// stage open from the inside.
//
// Rust's std has no RSS API, and shelling to `ps` would measure OS resident
// pages — which include the lingering ifcopenshell C++ model and freed-but-
// unreturned pages, i.e. exactly the noise that made our RSS readings jump ~20%
// run-to-run. Instead we install a tracking global allocator: two atomics count
// live Rust heap bytes and a high-water mark. This measures the native
// pipeline's *own* working set precisely and deterministically (Python's heap
// uses its own allocator and is not counted). The OS-level RSS picture still
// comes from `profile_memory.py`; this is the Rust-internal breakdown.
//
// The counting allocator is always active (a global allocator cannot be toggled
// at runtime) and adds two relaxed atomic ops per allocation. The per-stage
// report is opt-in via ATHAR_NATIVE_PROFILE=1 (stderr only, no FFI change).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
// Churn: cumulative bytes ever handed out and number of alloc events. Compared
// against peak live, these answer "how much transient work are we doing" — a
// high total-allocated / peak-live ratio means we rebuild/copy our working set
// many times over (a better "are we doing the right thing" signal than peak).
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            let now = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK_BYTES.fetch_max(now, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            if new_size >= layout.size() {
                let grow = new_size - layout.size();
                let now = LIVE_BYTES.fetch_add(grow, Ordering::Relaxed) + grow;
                PEAK_BYTES.fetch_max(now, Ordering::Relaxed);
                ALLOC_BYTES.fetch_add(grow, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn live_mb() -> f64 {
    LIVE_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

/// Reset the high-water mark to the current live total, so the next interval's
/// peak reflects only that stage.
fn reset_peak() {
    PEAK_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn peak_mb() -> f64 {
    PEAK_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

fn alloc_total_mb() -> f64 {
    ALLOC_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

fn alloc_count() -> u64 {
    ALLOC_COUNT.load(Ordering::Relaxed) as u64
}

struct StageRow {
    name: String,
    secs: f64,
    live_mb: f64,
    peak_mb: f64,
    churn_mb: f64, // bytes allocated during this stage
    allocs: u64,   // alloc events during this stage
}

struct StageProfiler {
    enabled: bool,
    last: std::time::Instant,
    prev_alloc_mb: f64,
    prev_allocs: u64,
    rows: Vec<StageRow>,
}

impl StageProfiler {
    fn new() -> Self {
        let enabled = std::env::var("ATHAR_NATIVE_PROFILE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        reset_peak();
        Self {
            enabled,
            last: std::time::Instant::now(),
            prev_alloc_mb: alloc_total_mb(),
            prev_allocs: alloc_count(),
            rows: Vec::new(),
        }
    }

    /// Record the stage that just finished: wall time, live heap now, peak live
    /// during it, and the allocation churn (bytes + events) it produced.
    fn mark(&mut self, name: &str) {
        if !self.enabled {
            return;
        }
        let now = std::time::Instant::now();
        let secs = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        let alloc_mb = alloc_total_mb();
        let allocs = alloc_count();
        self.rows.push(StageRow {
            name: name.to_string(),
            secs,
            live_mb: live_mb(),
            peak_mb: peak_mb(),
            churn_mb: alloc_mb - self.prev_alloc_mb,
            allocs: allocs - self.prev_allocs,
        });
        self.prev_alloc_mb = alloc_mb;
        self.prev_allocs = allocs;
        reset_peak();
    }

    fn report(&self) {
        if !self.enabled {
            return;
        }
        eprintln!("[native-profile] Rust heap (tracking allocator), per internal stage:");
        eprintln!(
            "[native-profile] {:<20}{:>8}{:>11}{:>11}{:>11}{:>11}{:>12}",
            "stage", "sec", "live_mb", "peak_mb", "Δlive_mb", "churn_mb", "allocs"
        );
        let mut prev = 0.0_f64;
        for r in &self.rows {
            eprintln!(
                "[native-profile] {:<20}{:>8.3}{:>11.1}{:>11.1}{:>+11.1}{:>11.1}{:>12}",
                r.name, r.secs, r.live_mb, r.peak_mb, r.live_mb - prev, r.churn_mb, r.allocs
            );
            prev = r.live_mb;
        }
        // Efficiency proxies: total churn vs the peak working set it built.
        let total_churn = alloc_total_mb();
        let total_allocs = alloc_count();
        let peak_live = self
            .rows
            .iter()
            .map(|r| r.peak_mb)
            .fold(0.0_f64, f64::max);
        let churn_factor = if peak_live > 0.0 { total_churn / peak_live } else { 0.0 };
        // Deterministic memory-cost: integrate live heap (exact, allocator-noise
        // free) over stage wall time. Trapezoidal between marks. Unlike the
        // RSS-based GB·s in profile_memory.py, the memory factor here does not
        // vary run-to-run — only the time factor does, so it is a far cleaner
        // "GB·s" proxy for the native pipeline.
        let mut heap_mb_s = 0.0_f64;
        let mut prev_live = 0.0_f64;
        for r in &self.rows {
            heap_mb_s += (prev_live + r.live_mb) / 2.0 * r.secs;
            prev_live = r.live_mb;
        }
        eprintln!(
            "[native-profile] TOTAL  churn={:.0} MB  allocs={}  peak_live={:.0} MB  churn_factor={:.1}x  heap={:.1} GB·s",
            total_churn, total_allocs, peak_live, churn_factor, heap_mb_s / 1024.0
        );
    }
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

    // Collect child hashes first (recursion); borrow labels rather than clone.
    let mut child_entries: Vec<(&str, String)> = Vec::new();
    if let Some(children) = adj.get(&step_id) {
        for (label, child_step) in children {
            let child_hash =
                hash_entity(*child_step, domain, classes, parts, adj, cache, visiting, cycles);
            child_entries.push((label.as_str(), child_hash));
        }
    }
    // Re-sort by (label, child_hash); identical to Python's
    // child_entries.sort(key=lambda item: (item[0], item[1])).
    child_entries.sort();

    // Stream the canonical payload directly into the hasher instead of building
    // a `Vec<String>` and joining on U+001F. Byte-identical to
    // `parts.join("\u{1f}")` — same bytes, same digest — but without the
    // per-part `format!`/clone and the joined `String`, which were the bulk of
    // the merkle stage's allocation churn.
    let mut hasher = Sha256::new();
    hasher.update(b"class=");
    hasher.update(class.as_bytes());
    if let Some(entity_parts) = parts.get(&step_id) {
        for part in entity_parts {
            hasher.update([0x1f]);
            hasher.update(part.as_bytes());
        }
    }
    for (label, child_hash) in &child_entries {
        hasher.update([0x1f]);
        hasher.update(b"edge=");
        hasher.update(label.as_bytes());
        hasher.update(b":");
        hasher.update(child_hash.as_bytes());
    }
    let digest = hex::encode(hasher.finalize());
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

/// BFS neighborhood up to `depth` hops (inclusive of `start`), written into
/// caller-owned scratch buffers. Mirrors `wl_gossip._neighbors_within_k`. The
/// result is left in `seen`; `queue` is scratch. Both are cleared on entry so
/// topology_compute can reuse one pair across ~1M nodes instead of allocating a
/// fresh `HashSet` + `VecDeque` per call (the dominant WL allocation source).
fn neighbors_within_k_into(
    adj: &HashMap<i64, Vec<i64>>,
    start: i64,
    depth: i64,
    seen: &mut HashSet<i64>,
    queue: &mut VecDeque<(i64, i64)>,
) {
    seen.clear();
    queue.clear();
    seen.insert(start);
    if depth <= 0 {
        return;
    }
    queue.push_back((start, 0));
    while let Some((node, d)) = queue.pop_front() {
        if d >= depth {
            continue;
        }
        if let Some(neighbors) = adj.get(&node) {
            for &next in neighbors {
                // `insert` returns false if already present — one lookup instead
                // of contains + insert.
                if seen.insert(next) {
                    queue.push_back((next, d + 1));
                }
            }
        }
    }
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

    // Scratch buffers reused across every node (cleared per use), so the BFS
    // does not allocate per call. `tokens` is also reused; its `&str`s borrow
    // `seeds`, which outlives the loop.
    let mut seen: HashSet<i64> = HashSet::new();
    let mut queue: VecDeque<(i64, i64)> = VecDeque::new();
    let mut tokens: Vec<(&str, &str)> = Vec::new();

    let mut out: HashMap<i64, String> = HashMap::with_capacity(seeds.len());
    for &step_id in &sorted_ids {
        tokens.clear();

        // Tokens are (prefix, seed) tuples rather than `format!("context:{seed}")`
        // strings. Sorting tuples by (prefix, seed) is byte-identical to sorting
        // the concatenated strings (the fixed prefixes "context:" < "spatial:"
        // decide cross-prefix order; equal prefixes fall through to the seed) —
        // but with no per-neighbour String allocation. Context tokens are read
        // out of `seen` before it is reused for the spatial BFS.
        neighbors_within_k_into(context_adj, step_id, context_k, &mut seen, &mut queue);
        for neigh in seen.iter() {
            if *neigh == step_id {
                continue;
            }
            let seed = seeds.get(neigh).map(String::as_str).unwrap_or("");
            tokens.push(("context:", seed));
        }
        neighbors_within_k_into(spatial_adj, step_id, spatial_k, &mut seen, &mut queue);
        for neigh in seen.iter() {
            if *neigh == step_id {
                continue;
            }
            let seed = seeds.get(neigh).map(String::as_str).unwrap_or("");
            tokens.push(("spatial:", seed));
        }
        tokens.sort();

        let self_seed = seeds.get(&step_id).map(String::as_str).unwrap_or("");
        // Stream "self:<seed>" + sorted tokens (U+001F separated) into the
        // hasher; byte-identical to the join, without the payload_parts/join.
        let mut hasher = Sha256::new();
        hasher.update(b"self:");
        hasher.update(self_seed.as_bytes());
        for (prefix, seed) in &tokens {
            hasher.update([0x1f]);
            hasher.update(prefix.as_bytes());
            hasher.update(seed.as_bytes());
        }
        out.insert(step_id, hex::encode(hasher.finalize()));
    }
    out
}

/// Compute `vh_topology` per entity from context + spatial neighbor gossip.
///
/// `seeds[step] = sha256(canonical_class)` is precomputed by the caller;
/// Rust does the BFS expansion and token assembly. `*_adj` are undirected
/// neighbor lists.
#[pyfunction]
#[pyo3(signature = (seeds, context_adj, spatial_adj, context_k=1, spatial_k=1))]
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
    let mut prof = StageProfiler::new();
    prof.mark("enter");
    let bytes = std::fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    prof.mark("read_bytes");
    let records = step::Parser::new(&bytes).parse()?;
    // STEP tokens are owned (Record holds Strings), so the raw file bytes are
    // dead the moment tokenization finishes; free them before the heavier stages.
    drop(bytes);
    prof.mark("tokenize");
    let schema = descriptor::parse_schema(schema_json)?;
    let mut parsed = canon::canonicalize(&records, &schema, unit_factors);
    // canonicalize() copied everything it needs into `parsed`; the token records
    // (the second-largest intermediate) are dead through the rest of the pipeline.
    drop(records);
    prof.mark("canonicalize");
    let edge_list = edges::build_edges(&parsed.entities, &parsed.id_to_keyword);
    prof.mark("build_edges");

    // ---- Merkle inputs -----------------------------------------------------
    let mut classes: HashMap<i64, String> = HashMap::with_capacity(parsed.entities.len());
    let mut geom_parts: HashMap<i64, Vec<String>> = HashMap::new();
    let mut data_parts: HashMap<i64, Vec<String>> = HashMap::new();
    // `geom_parts`/`data_parts` are not read off the entities again after this,
    // so move them into the merkle maps instead of cloning (avoids duplicating
    // the per-entity attribute strings — the bulk of the old +900 MB here).
    // `canonical_class` is still needed downstream (WL seeds, assemble), so it
    // stays a (small) clone.
    for e in &mut parsed.entities {
        classes.insert(e.step_id, e.canonical_class.clone());
        if !e.geom_parts.is_empty() {
            geom_parts.insert(e.step_id, std::mem::take(&mut e.geom_parts));
        }
        if !e.data_parts.is_empty() {
            data_parts.insert(e.step_id, std::mem::take(&mut e.data_parts));
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
    // The flat edge list has been fully projected into the adjacency maps above;
    // it is dead for the rest of the pipeline.
    drop(edge_list);
    for v in geom_adj.values_mut() {
        v.sort();
    }
    for v in data_adj.values_mut() {
        v.sort();
    }
    for v in geometry_adj.values_mut() {
        v.sort();
    }

    prof.mark("merkle_inputs");
    let (merkle_out, cycles) = merkle_compute(&classes, &geom_parts, &data_parts, &geom_adj, &data_adj);
    // Merkle consumed these; only `geometry_adj`, `data_children`, `context_adj`,
    // and `spatial_adj` are still needed (WL + spatial). Free the rest — the
    // moved attribute-part strings (`geom_parts`/`data_parts`) and the
    // domain-labelled adjacency are the bulk of it.
    drop(classes);
    drop(geom_parts);
    drop(data_parts);
    drop(geom_adj);
    drop(data_adj);
    prof.mark("merkle");

    // ---- WL topology -------------------------------------------------------
    let mut class_seeds: HashMap<&str, String> = HashMap::new();
    for e in &parsed.entities {
        class_seeds
            .entry(e.canonical_class.as_str())
            .or_insert_with(|| {
                let mut hasher = Sha256::new();
                hasher.update(e.canonical_class.as_bytes());
                hex::encode(hasher.finalize())
            });
    }
    let mut seeds: HashMap<i64, String> = HashMap::with_capacity(parsed.entities.len());
    for e in &parsed.entities {
        seeds.insert(e.step_id, class_seeds[e.canonical_class.as_str()].clone());
    }
    let context_vec: HashMap<i64, Vec<i64>> = sorted_adj(context_adj);
    let spatial_vec: HashMap<i64, Vec<i64>> = sorted_adj(spatial_adj);
    let topo = topology_compute(&seeds, &context_vec, &spatial_vec, 1, 1);
    prof.mark("wl_topology");

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
    prof.mark("spatial");

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
    prof.mark("assemble_signatures");

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
    prof.mark("diagnostics");
    prof.report();

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
/// `signatures._dedupe_fact_paths`. Once sorted, equal paths are adjacent, so a
/// single pass over runs reproduces the per-path numbering without the `counts`
/// / `seen` maps — and moves each `(path, value)` through instead of cloning it
/// (the old version cloned every fact a second time into `out`).
fn dedupe_fact_paths(mut facts: Vec<(String, String)>) -> Vec<(String, String)> {
    facts.sort();
    let mut out: Vec<(String, String)> = Vec::with_capacity(facts.len());
    let mut iter = facts.into_iter().peekable();
    while let Some((path, value)) = iter.next() {
        if iter.peek().map_or(true, |(next, _)| *next != path) {
            // Unique path: move through untouched.
            out.push((path, value));
        } else {
            // Run of duplicates: emit "path[1]", "path[2]", ... in order.
            let mut n = 1;
            out.push((format!("{path}[{n}]"), value));
            while iter.peek().map_or(false, |(next, _)| *next == path) {
                let (_, dup_value) = iter.next().unwrap();
                n += 1;
                out.push((format!("{path}[{n}]"), dup_value));
            }
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
