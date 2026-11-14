//! Athar's bottom signature pipeline.
//!
//! `build_signature_bundle` tokenizes the STEP bytes, canonicalizes them
//! against a schema descriptor map (built by Python from ifcopenshell, once per
//! schema), classifies edges, and runs Merkle + WL + spatial. It returns the
//! signature vectors of the roots (products, spatial elements, the project)
//! and an account of what no signature examined.

use std::collections::{BTreeMap, HashMap, HashSet};

use pyo3::prelude::*;
use sha2::{Digest, Sha256};

mod canon;
mod descriptor;
mod edges;
mod spatial;
mod step;

#[pyfunction]
fn rekey_step_file(input: &str, output: &str, mapping: HashMap<i64, i64>) -> PyResult<()> {
    let bytes = std::fs::read(input)?;
    let rewritten = step::Parser::new(&bytes)
        .rekey(&mapping)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    std::fs::write(output, rewritten)?;
    Ok(())
}

/// Lowercase hex sha256.
fn sha256_hex(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Per-stage memory/time profiler
// ---------------------------------------------------------------------------
//
// `build_bundle` is one call from Python's side; this reports its stages from
// the inside. A tracking global allocator counts live Rust heap bytes and a
// high-water mark, which measures the native pipeline's own working set
// deterministically (Python's heap is not counted). The allocator is always
// installed and costs two relaxed atomic ops per allocation; the per-stage
// report is opt-in via ATHAR_NATIVE_PROFILE=1 (stderr only).

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
        // RSS-based GB-seconds in profile_memory.py, the memory factor here does not
        // vary run-to-run — only the time factor does, so it is a far cleaner
        // "GB-seconds" proxy for the native pipeline.
        let mut heap_mb_s = 0.0_f64;
        let mut prev_live = 0.0_f64;
        for r in &self.rows {
            heap_mb_s += (prev_live + r.live_mb) / 2.0 * r.secs;
            prev_live = r.live_mb;
        }
        eprintln!(
            "[native-profile] TOTAL  churn={:.0} MB  allocs={}  peak_live={:.0} MB  churn_factor={:.1}x  heap={:.1} GB-seconds",
            total_churn, total_allocs, peak_live, churn_factor, heap_mb_s / 1024.0
        );
    }
}

// ---------------------------------------------------------------------------
// Merkle pass
// ---------------------------------------------------------------------------

/// One hashed child of an entity: the edge label, its position, and the
/// child's digest (64 bytes when a data edge carries a shape entity's
/// geometry and data digests together).
struct Child<'a> {
    label: &'a str,
    ord: u32,
    cyclic: bool,
    /// Member of a SET: children of equal label and digest are one member.
    per_content: bool,
    /// Data digest of a shape child reached through a geometry edge.
    via_geometry: bool,
    hash: [u8; 64],
    len: usize,
}

impl Child<'_> {
    fn digest(&self) -> &[u8] {
        &self.hash[..self.len]
    }
}

/// Renumber every closed-loop sibling group from its lexicographically
/// smallest rotation, keeping the direction.
fn canonicalize_loops(children: &mut [Child]) {
    if !children.iter().any(|c| c.cyclic) {
        return;
    }
    children.sort_by(|a, b| (a.via_geometry, a.label, a.ord).cmp(&(b.via_geometry, b.label, b.ord)));
    let mut start = 0;
    while start < children.len() {
        let mut end = start + 1;
        while end < children.len()
            && children[end].via_geometry == children[start].via_geometry
            && children[end].label == children[start].label
        {
            end += 1;
        }
        if children[start].cyclic {
            let group = &mut children[start..end];
            let n = group.len();
            let best = (0..n)
                .min_by(|&a, &b| {
                    (0..n)
                        .map(|k| group[(a + k) % n].digest())
                        .cmp((0..n).map(|k| group[(b + k) % n].digest()))
                })
                .unwrap_or(0);
            group.rotate_left(best);
            for (position, child) in group.iter_mut().enumerate() {
                child.ord = position as u32;
            }
        }
        start = end;
    }
}

fn entity_digest(domain: u8, class: &str, parts: &[String], children: &mut Vec<Child>) -> [u8; 32] {
    canonicalize_loops(children);
    children.sort_by(|a, b| {
        (a.via_geometry, a.label, a.ord, a.digest()).cmp(&(b.via_geometry, b.label, b.ord, b.digest()))
    });
    children.dedup_by(|b, a| {
        a.per_content
            && b.per_content
            && (a.via_geometry, a.label, a.digest()) == (b.via_geometry, b.label, b.digest())
    });
    let mut hasher = Sha256::new();
    hasher.update([domain]);
    hasher.update(class.as_bytes());
    for part in parts {
        hasher.update([0x1f]);
        hasher.update(part.as_bytes());
    }
    for child in children.iter() {
        hasher.update([0x1f]);
        hasher.update(if child.via_geometry { &b"via="[..] } else { &b"edge="[..] });
        hasher.update(child.label.as_bytes());
        if child.ord != canon::UNORDERED {
            hasher.update(format!("#{}", child.ord).as_bytes());
        }
        hasher.update(b":");
        hasher.update(child.digest());
    }
    hasher.finalize().into()
}

/// Starting colour of an entity inside an include cycle. It names the class,
/// never the STEP id, so hashes do not depend on file numbering.
fn cycle_seed(domain: &str, class: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(format!("cycle:{domain}:{class}").as_bytes());
    hasher.finalize().into()
}

/// Strongly connected components of the include graph restricted to the
/// edges `keep` accepts, children before parents (Tarjan, iterative).
/// Component `c` is `order[bounds[c]..bounds[c + 1]]`.
struct Components {
    order: Vec<u32>,
    bounds: Vec<u32>,
}

fn components(graph: &edges::Graph, n: usize, keep: impl Fn(&edges::Include) -> bool) -> Components {
    const UNSEEN: u32 = u32::MAX;
    let mut index = vec![UNSEEN; n];
    let mut low = vec![0u32; n];
    let mut on_stack = vec![false; n];
    let mut stack: Vec<u32> = Vec::new();
    let mut calls: Vec<(u32, u32)> = Vec::new();
    let mut next = 0u32;
    let mut order: Vec<u32> = Vec::with_capacity(n);
    let mut bounds: Vec<u32> = vec![0];

    for start in 0..n as u32 {
        if index[start as usize] != UNSEEN {
            continue;
        }
        index[start as usize] = next;
        low[start as usize] = next;
        next += 1;
        stack.push(start);
        on_stack[start as usize] = true;
        calls.push((start, graph.offsets[start as usize]));
        while let Some(&mut (v, ref mut position)) = calls.last_mut() {
            let end = graph.offsets[v as usize + 1];
            let mut descend = None;
            while *position < end {
                let edge = &graph.includes[*position as usize];
                *position += 1;
                if !keep(edge) {
                    continue;
                }
                let w = edge.target;
                if index[w as usize] == UNSEEN {
                    descend = Some(w);
                    break;
                }
                if on_stack[w as usize] {
                    low[v as usize] = low[v as usize].min(index[w as usize]);
                }
            }
            if let Some(w) = descend {
                index[w as usize] = next;
                low[w as usize] = next;
                next += 1;
                stack.push(w);
                on_stack[w as usize] = true;
                calls.push((w, graph.offsets[w as usize]));
                continue;
            }
            calls.pop();
            if let Some(&(parent, _)) = calls.last() {
                low[parent as usize] = low[parent as usize].min(low[v as usize]);
            }
            if low[v as usize] == index[v as usize] {
                loop {
                    let w = stack.pop().expect("component member on the stack");
                    on_stack[w as usize] = false;
                    order.push(w);
                    if w == v {
                        break;
                    }
                }
                bounds.push(order.len() as u32);
            }
        }
    }
    Components { order, bounds }
}

fn geometry_edge(edge: &edges::Include) -> bool {
    edge.domain == edges::Domain::Geometry
}

/// Per-entity geometry and data digests over the include graph.
///
/// The geometry digest of an entity covers its geometry parts and its
/// geometry-edge children. The data digest covers its data parts, the full
/// content of its data-edge children (for a shape child: geometry and data
/// digests), and the data digests of its geometry-edge shape children, so text
/// attributes and data attachments inside a shape tree reach `vh_data` of the
/// owner. A shape entity with no data content anywhere below it has no data
/// digest and contributes nothing to its parent's.
///
/// Entities on an include cycle are hashed as one strongly connected
/// component: every member starts from its class seed and is refined together
/// with the others for twice the component size, so each digest covers the
/// whole component and does not depend on which member a traversal meets
/// first.
struct Merkle<'a> {
    entities: &'a [canon::Entity],
    graph: &'a edges::Graph,
    geometry: Vec<[u8; 32]>,
    data: Vec<[u8; 32]>,
    /// No shape content: a non-shape, non-root entity without geometry parts
    /// or geometry children.
    shapeless: Vec<bool>,
    /// No data content: a shape entity without data parts or data children.
    dataless: Vec<bool>,
    cyclic_components: i64,
}

impl<'a> Merkle<'a> {
    fn new(entities: &'a [canon::Entity], graph: &'a edges::Graph) -> Self {
        let n = entities.len();
        let mut merkle = Self {
            entities,
            graph,
            geometry: vec![[0; 32]; n],
            data: vec![[0; 32]; n],
            shapeless: vec![false; n],
            dataless: vec![false; n],
            cyclic_components: 0,
        };
        merkle.hash_domain(b'G');
        merkle.hash_domain(b'D');
        merkle
    }

    fn data_edge(&self, edge: &edges::Include) -> bool {
        match edge.domain {
            edges::Domain::Data => true,
            edges::Domain::Geometry => self.entities[edge.target as usize].is_geometric,
            _ => false,
        }
    }

    fn hash_domain(&mut self, domain: u8) {
        let graph = self.graph;
        let n = self.entities.len();
        let parts = if domain == b'G' {
            components(graph, n, geometry_edge)
        } else {
            components(graph, n, |edge| self.data_edge(edge))
        };
        for window in parts.bounds.windows(2) {
            let members = &parts.order[window[0] as usize..window[1] as usize];
            let first = members[0];
            let self_loop = members.len() == 1
                && graph.children(first).iter().any(|edge| {
                    edge.target == first
                        && if domain == b'G' { geometry_edge(edge) } else { self.data_edge(edge) }
                });
            if members.len() == 1 && !self_loop {
                let (digest, empty) = self.digest(domain, first, false);
                self.store(domain, first, digest, empty);
                continue;
            }

            self.cyclic_components += 1;
            let seed_domain = if domain == b'G' { "geometry" } else { "data" };
            for &m in members {
                let seed = cycle_seed(seed_domain, &self.entities[m as usize].canonical_class);
                self.store(domain, m, seed, false);
            }
            let mut colours: Vec<[u8; 32]> = Vec::with_capacity(members.len());
            for _ in 0..2 * members.len() {
                colours.clear();
                for &m in members {
                    colours.push(self.digest(domain, m, true).0);
                }
                for (&m, colour) in members.iter().zip(&colours) {
                    self.store(domain, m, *colour, false);
                }
            }
        }
    }

    fn store(&mut self, domain: u8, entity: u32, digest: [u8; 32], empty: bool) {
        let idx = entity as usize;
        if domain == b'G' {
            self.geometry[idx] = digest;
            self.shapeless[idx] = empty;
        } else {
            self.data[idx] = digest;
            self.dataless[idx] = empty;
        }
    }

    /// Digest of one entity in `domain` from the stored digests of its
    /// children, and whether the entity has no content in that domain. A
    /// member of a cycle always has content.
    fn digest(&self, domain: u8, entity: u32, in_cycle: bool) -> ([u8; 32], bool) {
        let ent = &self.entities[entity as usize];
        let graph = self.graph;
        let mut children: Vec<Child<'a>> = Vec::new();
        for edge in graph.children(entity) {
            let t = edge.target as usize;
            let (via_geometry, hash, len) = if domain == b'G' {
                if edge.domain != edges::Domain::Geometry || self.shapeless[t] {
                    continue;
                }
                let mut hash = [0u8; 64];
                hash[..32].copy_from_slice(&self.geometry[t]);
                (false, hash, 32)
            } else {
                match self.data_child(edge) {
                    Some(child) => child,
                    None => continue,
                }
            };
            children.push(Child {
                label: &edge.label,
                ord: edge.ord,
                cyclic: edge.cyclic,
                per_content: edge.membership == edges::Membership::PerContent,
                via_geometry,
                hash,
                len,
            });
        }
        let (parts, empty) = if domain == b'G' {
            let empty = !in_cycle
                && !ent.is_geometric
                && !ent.is_root()
                && ent.geom_parts.is_empty()
                && children.is_empty();
            (&ent.geom_parts, empty)
        } else {
            let empty = !in_cycle && ent.is_geometric && ent.data_parts.is_empty() && children.is_empty();
            (&ent.data_parts, empty)
        };
        // A root's class is no part of its shape.
        let class = if domain == b'G' && ent.is_root() { "" } else { &*ent.canonical_class };
        (entity_digest(domain, class, parts, &mut children), empty)
    }

    /// What the target of `edge` contributes to the data digest of its
    /// source: whether it is reached through a geometry edge, and its digest
    /// bytes with their length.
    fn data_child(&self, edge: &edges::Include) -> Option<(bool, [u8; 64], usize)> {
        let t = edge.target as usize;
        let geometric = self.entities[t].is_geometric;
        let mut hash = [0u8; 64];
        match edge.domain {
            edges::Domain::Data if geometric => {
                hash[..32].copy_from_slice(&self.geometry[t]);
                if !self.dataless[t] {
                    hash[32..].copy_from_slice(&self.data[t]);
                }
                Some((false, hash, 64))
            }
            edges::Domain::Data => {
                hash[..32].copy_from_slice(&self.data[t]);
                Some((false, hash, 32))
            }
            edges::Domain::Geometry if geometric && !self.dataless[t] => {
                hash[..32].copy_from_slice(&self.data[t]);
                Some((true, hash, 32))
            }
            _ => None,
        }
    }

    /// Per include edge: a SET member whose data contribution equals that of
    /// an earlier member of the same attribute.
    fn repeated_set_members(&self) -> Vec<bool> {
        let graph = self.graph;
        let mut repeated = vec![false; graph.includes.len()];
        let mut seen: HashSet<(bool, &str, [u8; 64])> = HashSet::new();
        for entity in 0..self.entities.len() as u32 {
            seen.clear();
            for i in graph.child_range(entity) {
                let edge = &graph.includes[i];
                if edge.membership != edges::Membership::PerContent {
                    continue;
                }
                if let Some((via_geometry, hash, _)) = self.data_child(edge) {
                    repeated[i] = !seen.insert((via_geometry, &edge.label, hash));
                }
            }
        }
        repeated
    }

    fn geometry(&self, entity: u32) -> [u8; 32] {
        self.geometry[entity as usize]
    }

    fn data(&self, entity: u32) -> Option<[u8; 32]> {
        (!self.dataless[entity as usize]).then(|| self.data[entity as usize])
    }

    /// Geometry and data digests of one entity together.
    fn content(&self, entity: u32) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.geometry(entity));
        hasher.update(self.data(entity).unwrap_or([0; 32]));
        hasher.finalize().into()
    }
}

// ---------------------------------------------------------------------------
// Coverage accounting
// ---------------------------------------------------------------------------

/// What the signatures of one file did not examine.
struct Coverage {
    /// Relationship instances whose class has no policy entry, by class.
    unknown_relationships: BTreeMap<String, i64>,
    /// Entities no signature root reaches, by class: instance count and an
    /// order-independent digest of their content. A relationship counts when
    /// it projects nothing.
    unreached: BTreeMap<String, (i64, String)>,
    /// Entities excluded on purpose, by class: provenance reached only through
    /// `OwnerHistory`, and relationships the policy table ignores.
    ignored_by_design: BTreeMap<String, i64>,
}

fn coverage(
    entities: &[canon::Entity],
    index: &HashMap<i64, u32>,
    graph: &edges::Graph,
    merkle: &Merkle,
) -> Coverage {
    let n = entities.len();

    let mut reached = vec![false; n];
    let mut stack: Vec<u32> = (0..n as u32).filter(|&i| entities[i as usize].is_root()).collect();
    for &root in &stack {
        reached[root as usize] = true;
    }
    while let Some(current) = stack.pop() {
        for edge in graph.children(current) {
            if !reached[edge.target as usize] {
                reached[edge.target as usize] = true;
                stack.push(edge.target);
            }
        }
    }

    let mut provenance = vec![false; n];
    for ent in entities {
        for r in &ent.refs {
            if &*r.attr_name == "OwnerHistory" {
                if let Some(&target) = index.get(&r.target) {
                    if !provenance[target as usize] {
                        provenance[target as usize] = true;
                        stack.push(target);
                    }
                }
            }
        }
    }
    while let Some(current) = stack.pop() {
        for r in &entities[current as usize].refs {
            if let Some(&target) = index.get(&r.target) {
                if !provenance[target as usize] {
                    provenance[target as usize] = true;
                    stack.push(target);
                }
            }
        }
    }

    let known_relationships: HashSet<&str> =
        edges::RELATIONSHIPS.iter().map(|(kw, _)| *kw).collect();
    let mut ignored_by_design = graph.ignored_relationships.clone();
    let mut digests: BTreeMap<&str, Vec<[u8; 32]>> = BTreeMap::new();
    for (i, ent) in entities.iter().enumerate() {
        if reached[i] {
            continue;
        }
        // A relationship that projected is examined through its edges.
        if ent.is_relationship
            && known_relationships.contains(&*ent.keyword)
            && !graph.unprojected_relationships.contains(&(i as u32))
        {
            continue;
        }
        if provenance[i] {
            *ignored_by_design.entry(ent.canonical_class.to_string()).or_insert(0) += 1;
            continue;
        }
        digests.entry(&ent.canonical_class).or_default().push(merkle.content(i as u32));
    }
    let unreached = digests
        .into_iter()
        .map(|(class, mut instances)| {
            instances.sort_unstable();
            let mut hasher = Sha256::new();
            for digest in &instances {
                hasher.update(digest);
            }
            (class.to_string(), (instances.len() as i64, hex::encode(hasher.finalize())))
        })
        .collect();

    Coverage {
        unknown_relationships: graph.unknown_relationships.clone(),
        unreached,
        ignored_by_design,
    }
}

// ---------------------------------------------------------------------------
// WL topology
// ---------------------------------------------------------------------------

fn neighbour_lists(pairs: &HashSet<(u32, u32)>) -> HashMap<u32, Vec<u32>> {
    let mut out: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(a, b) in pairs {
        out.entry(a).or_default().push(b);
        out.entry(b).or_default().push(a);
    }
    out
}

/// `vh_topology` of every signature root: its class seed plus the class seeds
/// of its direct context and spatial neighbours (one WL round).
fn topology_hashes(entities: &[canon::Entity], graph: &edges::Graph) -> HashMap<u32, String> {
    let mut class_seeds: HashMap<&str, String> = HashMap::new();
    for e in entities {
        class_seeds
            .entry(&e.canonical_class)
            .or_insert_with(|| sha256_hex(e.canonical_class.as_bytes()));
    }
    let seed = |entity: u32| class_seeds[&*entities[entity as usize].canonical_class].as_str();
    let context = neighbour_lists(&graph.context);
    let spatial = neighbour_lists(&graph.spatial);

    let mut out: HashMap<u32, String> = HashMap::new();
    let mut tokens: Vec<(&str, &str)> = Vec::new();
    for (i, e) in entities.iter().enumerate() {
        if !e.is_root() {
            continue;
        }
        let entity = i as u32;
        tokens.clear();
        for (prefix, adjacency) in [("context:", &context), ("spatial:", &spatial)] {
            if let Some(neighbours) = adjacency.get(&entity) {
                tokens.extend(neighbours.iter().map(|&n| (prefix, seed(n))));
            }
        }
        tokens.sort();

        let mut hasher = Sha256::new();
        hasher.update(b"self:");
        hasher.update(seed(entity).as_bytes());
        for (prefix, neighbour_seed) in &tokens {
            hasher.update([0x1f]);
            hasher.update(prefix.as_bytes());
            hasher.update(neighbour_seed.as_bytes());
        }
        out.insert(entity, hex::encode(hasher.finalize()));
    }
    out
}

// ---------------------------------------------------------------------------
// Signature bundle
// ---------------------------------------------------------------------------

/// `vh_geometry` of a root that has feature elements also covers, per feature,
/// the feature's shape together with its position relative to the root.
///
/// One root signature, flattened for the FFI boundary:
/// (step_id, guid, name, entity_type, canonical_class, vh_geometry, vh_data,
///  vh_topology, (placement, placement_unresolved, local_placement,
///  placement_parent), centroid, aabb, (data_facts, relation_targets)).
///
/// `placement_parent` is the step id of the root whose object placement the
/// local placement is relative to.
///
/// A relation target is `(index into the bundle's relation labels, step id of
/// the named entity)`.
type SigTuple = (
    i64,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
    String,
    (Option<Vec<i64>>, Option<String>, Option<Vec<i64>>, Option<i64>),
    Option<Vec<f64>>,
    Option<Vec<f64>>,
    (Vec<(String, String)>, Vec<(u32, i64)>),
);

/// `(GlobalId, canonical class, name, content digest, GlobalId carried by
/// another entity of the file)` of an entity.
type EntityLabel = (Option<String>, String, Option<String>, String, bool);

struct NativeBundle {
    signatures: Vec<SigTuple>,
    edge_stats: BTreeMap<String, i64>,
    dangling_refs: i64,
    cycle_breaks: i64,
    warnings: Vec<String>,
    coverage: Coverage,
    /// `(relationship class, role attribute)` per relation label index.
    relation_labels: Vec<(String, String)>,
    /// Relation targets that have no signature, by step id.
    related_entities: HashMap<i64, EntityLabel>,
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
    drop(bytes);
    prof.mark("tokenize");
    let schema = descriptor::parse_schema(schema_json)?;
    let mut parsed = canon::canonicalize(&records, &schema, unit_factors);
    drop(records);
    prof.mark("canonicalize");
    let graph = edges::build_edges(&parsed.entities, &parsed.index);
    prof.mark("build_edges");

    let merkle = Merkle::new(&parsed.entities, &graph);
    let mut root_hashes: HashMap<u32, ([u8; 32], String)> = HashMap::new();
    for (i, e) in parsed.entities.iter().enumerate() {
        if e.is_root() {
            let entity = i as u32;
            let geometry = merkle.geometry(entity);
            let data = merkle.data(entity).unwrap_or([0; 32]);
            root_hashes.insert(entity, (geometry, hex::encode(data)));
        }
    }
    let placement_content: HashMap<u32, [u8; 32]> = graph
        .includes
        .iter()
        .filter(|edge| {
            edge.domain == edges::Domain::Placement
                && matches!(&*edge.label, "ObjectPlacement" | "PlacementRelTo")
                && &*parsed.entities[edge.target as usize].keyword != "IFCLOCALPLACEMENT"
        })
        .map(|edge| edge.target)
        .collect::<HashSet<u32>>()
        .into_iter()
        .map(|target| (target, merkle.content(target)))
        .collect();
    prof.mark("merkle");
    let coverage = coverage(&parsed.entities, &parsed.index, &graph, &merkle);
    let cycle_breaks = merkle.cyclic_components;
    let shape_facts = shape_facts_of_data_reached_entities(&parsed.entities, &graph, &merkle);
    let repeated = merkle.repeated_set_members();
    let mut related_guid_uses: HashMap<&str, u32> = HashMap::new();
    for &(_, _, target) in &graph.relation_targets {
        let named = &parsed.entities[target as usize];
        if let (false, Some(guid)) = (named.is_root(), &named.guid) {
            related_guid_uses.insert(guid, 0);
        }
    }
    for entity in &parsed.entities {
        if let Some(uses) = entity.guid.as_deref().and_then(|guid| related_guid_uses.get_mut(guid)) {
            *uses += 1;
        }
    }
    let mut related_entities: HashMap<i64, EntityLabel> = HashMap::new();
    for &(_, _, target) in &graph.relation_targets {
        let named = &parsed.entities[target as usize];
        if !named.is_root() {
            related_entities.entry(named.step_id).or_insert_with(|| {
                (
                    named.guid.clone(),
                    named.canonical_class.to_string(),
                    named.name.clone(),
                    hex::encode(merkle.content(target)),
                    named.guid.as_deref().map_or(false, |guid| related_guid_uses[guid] > 1),
                )
            });
        }
    }
    let dataless = merkle.dataless;
    for e in &mut parsed.entities {
        e.geom_parts = Vec::new();
        e.data_parts = Vec::new();
    }
    for (entity, facts) in shape_facts {
        parsed.entities[entity as usize].geom_facts.extend(facts);
    }
    prof.mark("coverage");

    let topology = topology_hashes(&parsed.entities, &graph);
    prof.mark("wl_topology");

    let ctx = spatial::SpatialCtx {
        entities: &parsed.entities,
        index: &parsed.index,
        graph: &graph,
        point_coords: &parsed.point_coords,
        placement_axes: &parsed.placement_axes,
        placement_content: &placement_content,
    };
    let spatial_feats = spatial::build_spatial_features(&ctx)?;
    prof.mark("spatial");

    let mut relations = graph.relation_targets.iter().peekable();
    let mut facts = FactCollector::new(&parsed.entities, &graph, &dataless, &repeated);
    let mut signatures: Vec<SigTuple> = Vec::new();
    for (i, e) in parsed.entities.iter().enumerate() {
        if !e.is_root() {
            continue;
        }
        let entity = i as u32;
        let (geometry, vh_data) = root_hashes
            .get(&entity)
            .cloned()
            .unwrap_or_else(|| panic!("no content hashes for the root #{}", e.step_id));
        let vh_topology = topology
            .get(&entity)
            .cloned()
            .unwrap_or_else(|| panic!("no topology hash for the root #{}", e.step_id));
        let feat = spatial_feats
            .get(&entity)
            .unwrap_or_else(|| panic!("no spatial features for the root #{}", e.step_id));
        let vh_geometry = hex::encode(match feat.feature_positions.as_slice() {
            [] => geometry,
            positions => {
                let mut placed: Vec<[u8; 32]> = positions
                    .iter()
                    .map(|(feature, position)| {
                        let mut hasher = Sha256::new();
                        let (shape, _) = root_hashes.get(feature).unwrap_or_else(|| {
                            panic!("no content hashes for the feature #{}", parsed.entities[*feature as usize].step_id)
                        });
                        hasher.update(shape);
                        hasher.update(position);
                        hasher.finalize().into()
                    })
                    .collect();
                placed.sort_unstable();
                let mut hasher = Sha256::new();
                hasher.update(b"features");
                hasher.update(geometry);
                for feature in &placed {
                    hasher.update(feature);
                }
                hasher.finalize().into()
            }
        });
        let placement = feat.placement.clone();
        let placement_unresolved = feat.placement_unresolved.clone();
        let local_placement = feat.local_placement.clone();
        let placement_parent = feat.placement_parent.map(|parent| parsed.entities[parent as usize].step_id);
        let centroid = feat.centroid.map(|c| c.to_vec());
        let aabb = feat.aabb.map(|a| a.to_vec());
        let data_facts = facts.collect(entity);
        let mut relation_targets: Vec<(u32, i64)> = Vec::new();
        while let Some(&(_, label, target)) = relations.next_if(|relation| relation.0 == entity) {
            relation_targets.push((label, parsed.entities[target as usize].step_id));
        }
        signatures.push((
            e.step_id,
            e.guid.clone(),
            e.name.clone(),
            e.keyword.to_string(),
            e.canonical_class.to_string(),
            vh_geometry,
            vh_data,
            vh_topology,
            (placement, placement_unresolved, local_placement, placement_parent),
            centroid,
            aabb,
            (data_facts, relation_targets),
        ));
    }
    signatures.sort_by_key(|s| s.0);
    prof.mark("assemble_signatures");

    let mut dangling_refs: i64 = 0;
    for e in &parsed.entities {
        for r in &e.refs {
            if !parsed.index.contains_key(&r.target) && !parsed.skipped_ids.contains(&r.target) {
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
        edge_stats: graph.stats(),
        dangling_refs,
        cycle_breaks,
        warnings,
        coverage,
        relation_labels: graph
            .relation_labels
            .iter()
            .map(|(class, role)| (class.to_string(), role.to_string()))
            .collect(),
        related_entities,
    })
}

/// How much of an entity `vh_data` of the root covers.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Scope {
    /// Data and geometry content: the entity is reached over a data edge.
    Full,
    /// Data content only: a shape entity reached over geometry edges.
    DataOnly,
}

/// Facts for the geometry content of every shape entity some data edge
/// points at, so a data change caused by shape content (a context's
/// `TrueNorth`, a material profile's profile) can be named: the aggregate
/// attributes and frame in their canonical encoding (a digest when long), a
/// triangle count and digest, and one `(shape)` digest standing for the
/// geometry below the entity.
fn shape_facts_of_data_reached_entities(
    entities: &[canon::Entity],
    graph: &edges::Graph,
    merkle: &Merkle,
) -> Vec<(u32, Vec<(std::rc::Rc<str>, String)>)> {
    let targets: HashSet<u32> = graph
        .includes
        .iter()
        .filter(|edge| edge.domain == edges::Domain::Data && entities[edge.target as usize].is_geometric)
        .map(|edge| edge.target)
        .collect();
    let mut out = Vec::with_capacity(targets.len());
    for entity in targets {
        let ent = &entities[entity as usize];
        let mut facts: Vec<(std::rc::Rc<str>, String)> = Vec::new();
        let mut triangles = Sha256::new();
        let mut triangle_count = 0usize;
        for part in &ent.geom_parts {
            if part.starts_with("tri:") {
                triangles.update(part.as_bytes());
                triangle_count += 1;
                continue;
            }
            let (name, value) = match part.strip_prefix("attr=").and_then(|rest| rest.split_once(':')) {
                Some(pair) => pair,
                None => part.split_once('=').unwrap_or(("(part)", part)),
            };
            if !ent.geom_facts.iter().any(|(scalar, _)| &**scalar == name) {
                facts.push((name.into(), canon::bounded_fact(value)));
            }
        }
        if triangle_count > 0 {
            let digest = hex::encode(triangles.finalize());
            facts.push(("CoordIndex".into(), format!("{triangle_count} triangles, sha256:{}", &digest[..12])));
        }
        if graph.children(entity).iter().any(geometry_edge) {
            facts.push(("(shape)".into(), hex::encode(&merkle.geometry(entity)[..6])));
        }
        out.push((entity, facts));
    }
    out
}

/// Readable facts of everything `vh_data` of a root covers, so a data change
/// can name what differs. A path is the owners and the positions of the
/// ordered lists passed over data edges on the way, then the class and
/// attribute of the entity holding the value; repeated paths are numbered.
///
/// An owner is an entity with a GlobalId below the root (a property set, a
/// type object). `GlobalId` enters no hash, so an owner is labelled by class
/// and `Name`; the GlobalId is added only to owners of one root whose labels
/// would otherwise be equal.
struct FactCollector<'a> {
    entities: &'a [canon::Entity],
    graph: &'a edges::Graph,
    dataless: &'a [bool],
    /// Per include edge: a SET member equal to an earlier one, listed once.
    repeated: &'a [bool],
    prefixes: Vec<String>,
    seen: HashSet<(u32, Scope, u32)>,
    on_path: HashSet<u32>,
    /// Owner label without GlobalId -> the first owner carrying it.
    owners: HashMap<String, u32>,
    /// Owners sharing a label with another owner of the current root.
    collided: HashSet<u32>,
}

/// Delimits an owner's entity index inside a path until its label is known.
const OWNER_MARK: char = '\0';

impl<'a> FactCollector<'a> {
    fn new(
        entities: &'a [canon::Entity],
        graph: &'a edges::Graph,
        dataless: &'a [bool],
        repeated: &'a [bool],
    ) -> Self {
        Self {
            entities,
            graph,
            dataless,
            repeated,
            prefixes: vec![String::new()],
            seen: HashSet::new(),
            on_path: HashSet::new(),
            owners: HashMap::new(),
            collided: HashSet::new(),
        }
    }

    fn owner_label(&self, owner: u32, with_guid: bool) -> String {
        let ent = &self.entities[owner as usize];
        match (&ent.name, &ent.guid, with_guid) {
            (Some(name), Some(guid), true) => format!("[Name={name},GlobalId={guid}]"),
            (None, Some(guid), true) => format!("[GlobalId={guid}]"),
            (Some(name), _, _) => format!("[Name={name}]"),
            (None, _, _) => String::new(),
        }
    }

    /// `path` with every marked owner index replaced by `label(owner)`.
    fn with_owner_labels(path: &str, label: impl Fn(u32) -> String) -> String {
        let mut out = String::with_capacity(path.len());
        for (i, part) in path.split(OWNER_MARK).enumerate() {
            if i % 2 == 0 {
                out.push_str(part);
            } else {
                out.push_str(&label(part.parse().expect("owner index")));
            }
        }
        out
    }

    fn collect(&mut self, root: u32) -> Vec<(String, String)> {
        self.prefixes.truncate(1);
        self.seen.clear();
        self.on_path.clear();
        self.owners.clear();
        self.collided.clear();
        let mut facts: Vec<(String, String)> = Vec::new();
        // (entity, scope, prefix, leaving)
        let mut stack: Vec<(u32, Scope, u32, bool)> = vec![(root, Scope::Full, 0, false)];
        while let Some((current, scope, prefix, leaving)) = stack.pop() {
            if leaving {
                self.on_path.remove(&current);
                continue;
            }
            if self.on_path.contains(&current) || !self.seen.insert((current, scope, prefix)) {
                continue;
            }
            self.on_path.insert(current);
            stack.push((current, scope, prefix, true));

            let ent = &self.entities[current as usize];
            let label = match &ent.guid {
                Some(g) if current == root => {
                    format!("{}{}[GlobalId={}]", self.prefixes[prefix as usize], ent.canonical_class, g)
                }
                Some(_) => {
                    let label = format!(
                        "{}{}{OWNER_MARK}{current}{OWNER_MARK}",
                        self.prefixes[prefix as usize], ent.canonical_class
                    );
                    let plain = Self::with_owner_labels(&label, |owner| self.owner_label(owner, false));
                    let first = *self.owners.entry(plain).or_insert(current);
                    if first != current {
                        self.collided.insert(first);
                        self.collided.insert(current);
                    }
                    label
                }
                None => format!("{}{}", self.prefixes[prefix as usize], ent.canonical_class),
            };
            for (attr, value) in &ent.data_facts {
                facts.push((format!("{label}.{attr}"), value.clone()));
            }
            if scope == Scope::Full {
                for (attr, value) in &ent.geom_facts {
                    facts.push((format!("{label}.{attr}"), value.clone()));
                }
            }
            // Content below an owner is listed under that owner.
            let mut owned_prefix: Option<u32> = None;
            let graph = self.graph;
            for i in graph.child_range(current) {
                if self.repeated[i] {
                    continue;
                }
                let edge = &graph.includes[i];
                let target = &self.entities[edge.target as usize];
                let child_scope = match edge.domain {
                    edges::Domain::Data => Scope::Full,
                    edges::Domain::Geometry if target.is_geometric && !self.dataless[edge.target as usize] => {
                        Scope::DataOnly
                    }
                    _ => continue,
                };
                let base = if current == root || ent.guid.is_none() || child_scope != Scope::Full {
                    prefix
                } else {
                    *owned_prefix.get_or_insert_with(|| {
                        self.prefixes.push(format!("{label}/"));
                        self.prefixes.len() as u32 - 1
                    })
                };
                let positioned = child_scope == Scope::Full && edge.ord != canon::UNORDERED && !edge.cyclic;
                let child_prefix = if !positioned {
                    base
                } else {
                    let text = format!("{}{}[{}]/", self.prefixes[base as usize], edge.label, edge.ord + 1);
                    self.prefixes.push(text);
                    self.prefixes.len() as u32 - 1
                };
                stack.push((edge.target, child_scope, child_prefix, false));
            }
        }
        for (path, _) in facts.iter_mut() {
            if path.contains(OWNER_MARK) {
                *path = Self::with_owner_labels(path, |owner| {
                    self.owner_label(owner, self.collided.contains(&owner))
                });
            }
        }
        number_repeated_paths(facts)
    }
}

/// Sort facts and suffix repeated paths with `[1]`, `[2]`, ... in value order.
fn number_repeated_paths(mut facts: Vec<(String, String)>) -> Vec<(String, String)> {
    facts.sort();
    let mut out: Vec<(String, String)> = Vec::with_capacity(facts.len());
    let mut iter = facts.into_iter().peekable();
    while let Some((path, value)) = iter.next() {
        if iter.peek().map_or(true, |(next, _)| *next != path) {
            out.push((path, value));
        } else {
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

type CoverageTuple = (BTreeMap<String, i64>, BTreeMap<String, (i64, String)>, BTreeMap<String, i64>);

/// Full native parse + signature build for one IFC file.
///
/// `schema_json` is the per-class descriptor map; `unit_factors` maps unit
/// types to scale factors. Returns `(signatures, edge_stats, (dangling_refs,
/// cycle_breaks, warnings), (unknown_relationships, unreached,
/// ignored_by_design), (relation_labels, related_entities))`.
#[pyfunction]
fn build_signature_bundle(
    py: Python<'_>,
    path: String,
    schema_json: String,
    unit_factors: HashMap<String, f64>,
) -> PyResult<(
    Vec<SigTuple>,
    BTreeMap<String, i64>,
    (i64, i64, Vec<String>),
    CoverageTuple,
    (Vec<(String, String)>, HashMap<i64, EntityLabel>),
)> {
    let bundle = py
        .allow_threads(|| build_bundle(&path, &schema_json, &unit_factors))
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let coverage = bundle.coverage;
    Ok((
        bundle.signatures,
        bundle.edge_stats,
        (bundle.dangling_refs, bundle.cycle_breaks, bundle.warnings),
        (coverage.unknown_relationships, coverage.unreached, coverage.ignored_by_design),
        (bundle.relation_labels, bundle.related_entities),
    ))
}

type PolicyRows = Vec<(String, String, String)>;

/// The relationship policy table: per STEP keyword, either its links as
/// `(source_attr, target_attr, kind)` or the reason it is ignored.
#[pyfunction]
fn relationship_policy() -> Vec<(String, PolicyRows, Option<String>)> {
    edges::RELATIONSHIPS
        .iter()
        .map(|(keyword, policy)| match policy {
            edges::Policy::Project(links) => (
                keyword.to_string(),
                links
                    .iter()
                    .map(|l| {
                        (l.source_attr.to_string(), l.target_attr.to_string(), l.kind.name().to_string())
                    })
                    .collect(),
                None,
            ),
            edges::Policy::Ignored(reason) => (keyword.to_string(), Vec::new(), Some(reason.to_string())),
        })
        .collect()
}

/// The explicit exclusion and attachment tables, each as
/// `(STEP keyword or "*", attribute, reason)` rows keyed by table name.
#[pyfunction]
fn reference_policy() -> HashMap<String, PolicyRows> {
    let rows = |table: &[(&str, &str, &str)]| -> PolicyRows {
        table.iter().map(|(k, a, r)| (k.to_string(), a.to_string(), r.to_string())).collect()
    };
    HashMap::from([
        ("unhashed_attributes".to_string(), rows(canon::UNHASHED_ATTRIBUTES)),
        ("denied_references".to_string(), rows(edges::DENIED_REFERENCES)),
        ("unordered_lists".to_string(), rows(edges::UNORDERED_LISTS)),
        (
            "reverse_attachments".to_string(),
            edges::REVERSE_ATTACHMENTS
                .iter()
                .map(|(k, a)| (k.to_string(), a.to_string(), String::new()))
                .collect(),
        ),
        (
            "cyclic_lists".to_string(),
            edges::CYCLIC_LISTS.iter().map(|(k, a)| (k.to_string(), a.to_string(), String::new())).collect(),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edges::tests::{ent, graph, Flags};

    fn shape() -> Flags {
        Flags { geometric: true, ..Flags::default() }
    }

    fn ordered(entity: &mut canon::Entity) {
        for (position, r) in entity.refs.iter_mut().enumerate() {
            r.ord = position as u32;
        }
    }

    fn point(step_id: i64, x: &str) -> canon::Entity {
        let mut p = ent(step_id, "IFCCARTESIANPOINT", shape(), &[]);
        p.geom_parts.push(format!("attr=Coordinates:[{x}]"));
        p
    }

    fn loop_digest(order: &[i64]) -> [u8; 32] {
        let refs: Vec<(&str, i64)> = order.iter().map(|&t| ("Polygon", t)).collect();
        let mut polyloop = ent(10, "IFCPOLYLOOP", shape(), &refs);
        ordered(&mut polyloop);
        let entities = vec![point(1, "a"), point(2, "b"), point(3, "c"), polyloop];
        let g = graph(&entities);
        Merkle::new(&entities, &g).geometry(3)
    }

    #[test]
    fn closed_loop_ignores_start_vertex_but_not_direction() {
        assert_eq!(loop_digest(&[1, 2, 3]), loop_digest(&[2, 3, 1]));
        assert_ne!(loop_digest(&[1, 2, 3]), loop_digest(&[3, 2, 1]));
    }

    fn polyline_digest(points: Vec<canon::Entity>, order: &[i64]) -> [u8; 32] {
        let refs: Vec<(&str, i64)> = order.iter().map(|&t| ("Points", t)).collect();
        let mut polyline = ent(10, "IFCPOLYLINE", shape(), &refs);
        ordered(&mut polyline);
        let mut entities = points;
        entities.push(polyline);
        let g = graph(&entities);
        Merkle::new(&entities, &g).geometry(entities.len() as u32 - 1)
    }

    #[test]
    fn ordered_children_keep_position_and_ignore_sharing() {
        let three = || vec![point(1, "a"), point(2, "b"), point(3, "c")];
        let base = polyline_digest(three(), &[1, 2, 3, 1]);
        assert_ne!(base, polyline_digest(three(), &[1, 3, 2, 1]));
        let mut unshared = three();
        unshared.push(point(4, "a"));
        assert_eq!(base, polyline_digest(unshared, &[1, 2, 3, 4]));
    }

    fn set_digest(keyword: &str, members: &[i64], in_set: bool) -> Option<[u8; 32]> {
        let refs: Vec<(&str, i64)> = members.iter().map(|&t| ("Members", t)).collect();
        let mut owner = ent(10, keyword, Flags::default(), &refs);
        for r in &mut owner.refs {
            r.in_set = in_set;
        }
        let entities = vec![approval(1, "a", 99), approval(2, "a", 99), approval(3, "b", 99), owner];
        let g = graph(&entities);
        Merkle::new(&entities, &g).data(3)
    }

    #[test]
    fn set_members_of_equal_content_are_one_member() {
        let single = set_digest("IFCOWNER", &[1, 3], true);
        assert_eq!(single, set_digest("IFCOWNER", &[1, 2, 3], true));
        assert_eq!(single, set_digest("IFCOWNER", &[1, 1, 3], true));
        assert_ne!(single, set_digest("IFCOWNER", &[1], true));
    }

    #[test]
    fn members_outside_a_set_all_count() {
        let single = set_digest("IFCOWNER", &[1, 3], false);
        assert_ne!(single, set_digest("IFCOWNER", &[1, 2, 3], false));
        assert_ne!(single, set_digest("IFCOWNER", &[1, 1, 3], false));
    }

    #[test]
    fn data_digest_carries_shape_content_across_a_data_edge() {
        let digest = |dim: &str| {
            let mut profile = ent(2, "IFCRECTANGLEPROFILEDEF", shape(), &[]);
            profile.geom_parts.push(format!("attr=XDim:{dim}"));
            let entities = vec![ent(1, "IFCMATERIALPROFILE", Flags::default(), &[("Profile", 2)]), profile];
            let g = graph(&entities);
            Merkle::new(&entities, &g).data(0)
        };
        assert_ne!(digest("r1"), digest("r2"));
    }

    #[test]
    fn text_inside_a_shape_tree_reaches_the_owner_data_digest_only() {
        let digests = |identifier: &str| {
            let mut representation = ent(2, "IFCSHAPEREPRESENTATION", shape(), &[]);
            representation.data_parts.push(format!("attr=RepresentationIdentifier:{identifier}"));
            let entities = vec![
                ent(1, "IFCWALL", Flags { product: true, ..Flags::default() }, &[("Representation", 2)]),
                representation,
            ];
            let g = graph(&entities);
            let merkle = Merkle::new(&entities, &g);
            (merkle.geometry(0), merkle.data(0))
        };
        let (body_geometry, body_data) = digests("s'Body");
        let (axis_geometry, axis_data) = digests("s'Axis");
        assert_eq!(body_geometry, axis_geometry);
        assert_ne!(body_data, axis_data);
    }

    fn approval(step_id: i64, name: &str, next: i64) -> canon::Entity {
        let mut approval = ent(step_id, "IFCAPPROVAL", Flags::default(), &[("Next", next)]);
        approval.data_parts.push(format!("attr=Name:s'{name}"));
        approval
    }

    #[test]
    fn cycle_digests_do_not_depend_on_step_ids_or_record_order() {
        let forward = vec![approval(1, "a", 2), approval(2, "b", 3), approval(3, "c", 1)];
        let shuffled = vec![approval(40, "c", 90), approval(7, "b", 40), approval(90, "a", 7)];
        let g = graph(&forward);
        let first = Merkle::new(&forward, &g);
        assert_eq!(first.cyclic_components, 1);
        let g = graph(&shuffled);
        let second = Merkle::new(&shuffled, &g);
        assert_eq!(first.data(0), second.data(2));
        assert_eq!(first.data(1), second.data(1));
        assert_eq!(first.data(2), second.data(0));
        assert_ne!(first.data(0), first.data(1));
    }

    #[test]
    fn every_member_of_a_cycle_sees_a_change_anywhere_in_it() {
        let digest = |far: &str| {
            let entities = vec![approval(1, "a", 2), approval(2, "b", 3), approval(3, far, 1)];
            let g = graph(&entities);
            let merkle = Merkle::new(&entities, &g);
            (merkle.data(0), merkle.data(1))
        };
        assert_ne!(digest("c").0, digest("d").0);
        assert_ne!(digest("c").1, digest("d").1);
    }

    #[test]
    fn topology_ignores_neighbour_order() {
        let build = |first: i64, second: i64| {
            let product = || Flags { product: true, ..Flags::default() };
            let relationship = || Flags { relationship: true, ..Flags::default() };
            let entities = vec![
                ent(1, "IFCWALL", product(), &[]),
                ent(2, "IFCDOOR", product(), &[]),
                ent(3, "IFCSLAB", product(), &[]),
                ent(4, "IFCRELCONNECTSELEMENTS", relationship(), &[("RelatingElement", 1), ("RelatedElement", first)]),
                ent(5, "IFCRELCONNECTSELEMENTS", relationship(), &[("RelatingElement", 1), ("RelatedElement", second)]),
            ];
            let g = graph(&entities);
            topology_hashes(&entities, &g)[&0].clone()
        };
        assert_eq!(build(2, 3), build(3, 2));
    }
}

#[pymodule]
fn athar_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(rekey_step_file, m)?)?;
    m.add_function(wrap_pyfunction!(build_signature_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(relationship_policy, m)?)?;
    m.add_function(wrap_pyfunction!(reference_policy, m)?)?;
    Ok(())
}
