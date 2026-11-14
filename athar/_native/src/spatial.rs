//! Resolve `ObjectPlacement` chains into quantized world matrices, and compute
//! centroid + AABB from each root's geometry point cloud.

use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};

use crate::canon::Entity;
use crate::edges::{Domain, Graph};

const PLACEMENT_SCALE: f64 = 1_000_000.0;

pub struct SpatialFeature {
    /// Quantized world matrix; `None` without a placement or when the chain
    /// is unresolved.
    pub placement: Option<Vec<i64>>,
    /// Digest standing in for a placement chain that has no matrix: it passes
    /// through a placement the engine does not evaluate (`IfcGridPlacement`)
    /// or ends in a `PlacementRelTo` cycle.
    pub placement_unresolved: Option<String>,
    /// World-space centroid and bounding box; `None` for an unresolved chain.
    pub centroid: Option<[f64; 3]>,
    pub aabb: Option<[f64; 6]>,
    /// For every feature element of this root (entity index): a digest of
    /// where the feature sits relative to the root.
    pub feature_positions: Vec<(u32, [u8; 32])>,
}

type Matrix = [[f64; 4]; 4];

fn identity() -> Matrix {
    [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

#[derive(Clone, Copy)]
enum Placed {
    At(Matrix),
    /// `local` is the chain below the unevaluated placement or the placement
    /// cycle `base` stands for.
    Unresolved { base: [u8; 32], local: Matrix },
}

impl Placed {
    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        let matrix = match self {
            Placed::At(matrix) => matrix,
            Placed::Unresolved { base, local } => {
                hasher.update(base);
                local
            }
        };
        for value in quantize_matrix(*matrix) {
            hasher.update(value.to_le_bytes());
        }
        hasher.finalize().into()
    }
}

pub struct SpatialCtx<'a> {
    pub entities: &'a [Entity],
    pub index: &'a HashMap<i64, u32>,
    pub graph: &'a Graph,
    pub point_coords: &'a HashMap<i64, [f64; 3]>,
    pub placement_axes: &'a HashMap<i64, ([f64; 3], [f64; 3])>,
    /// Content digest of every object placement that is not an
    /// `IfcLocalPlacement`, by entity index.
    pub placement_content: &'a HashMap<u32, [u8; 32]>,
}

impl SpatialCtx<'_> {
    fn entity(&self, step_id: i64) -> Option<&Entity> {
        self.index.get(&step_id).map(|&i| &self.entities[i as usize])
    }
}

struct Resolver<'a> {
    ctx: &'a SpatialCtx<'a>,
    cache: HashMap<i64, Placed>,
    /// IfcGridAxis entity index -> (ObjectPlacement step id of its IfcGrid,
    /// step id of the grid).
    grid_placement_of_axis: HashMap<u32, (i64, i64)>,
    /// Unevaluated placements whose stand-in digest is being computed.
    in_progress: HashSet<i64>,
}

impl<'a> Resolver<'a> {
    fn new(ctx: &'a SpatialCtx<'a>) -> Self {
        let mut grid_placement_of_axis = HashMap::new();
        for grid in ctx.entities.iter().filter(|e| &*e.keyword == "IFCGRID") {
            let Some(placement) = first_ref_target(grid, "ObjectPlacement") else { continue };
            for r in &grid.refs {
                if matches!(&*r.attr_name, "UAxes" | "VAxes" | "WAxes") {
                    if let Some(&axis) = ctx.index.get(&r.target) {
                        grid_placement_of_axis.insert(axis, (placement, grid.step_id));
                    }
                }
            }
        }
        Self { ctx, cache: HashMap::new(), grid_placement_of_axis, in_progress: HashSet::new() }
    }

    /// World placement of the placement record `placement_step`, which the
    /// record `referrer` names as its placement; `None` when it is missing.
    ///
    /// Local placements on a `PlacementRelTo` cycle have no base to start
    /// from: each is unresolved, standing on a digest of the relative
    /// placements met from it around the cycle.
    fn resolve(&mut self, placement_step: i64, referrer: i64) -> Result<Option<Placed>, String> {
        let ctx = self.ctx;
        // Local placements without a cached result, from `placement_step` up.
        let mut chain: Vec<i64> = Vec::new();
        let mut position: HashMap<i64, usize> = HashMap::new();
        let mut current = placement_step;
        let mut referrer = referrer;
        let mut cacheable = true;
        let mut placed = loop {
            if let Some(placed) = self.cache.get(&current) {
                break *placed;
            }
            let Some(placement) = ctx.entity(current) else {
                if chain.is_empty() {
                    return Ok(None);
                }
                break Placed::At(identity());
            };
            if placement.is_root() {
                return Err(format!(
                    "#{referrer} is placed by #{current} {}, which is an element and not an object placement",
                    placement.keyword
                ));
            }
            if &*placement.keyword != "IFCLOCALPLACEMENT" {
                if !self.in_progress.insert(current) {
                    // The stand-in of `current` is being computed and reaches
                    // back to itself through a grid: nothing below is final.
                    if chain.is_empty() {
                        return Ok(None);
                    }
                    cacheable = false;
                    break Placed::At(identity());
                }
                let base = self.unresolved_base(current);
                self.in_progress.remove(&current);
                let placed = Placed::Unresolved { base: base?, local: identity() };
                self.cache.insert(current, placed);
                break placed;
            }
            if let Some(&start) = position.get(&current) {
                let cycle = chain.split_off(start);
                let locals: Vec<Vec<i64>> =
                    cycle.iter().map(|&member| quantize_matrix(self.relative_matrix(member))).collect();
                for (k, &member) in cycle.iter().enumerate() {
                    let mut hasher = Sha256::new();
                    hasher.update(b"placement cycle");
                    for offset in 0..cycle.len() {
                        for value in &locals[(k + offset) % cycle.len()] {
                            hasher.update(value.to_le_bytes());
                        }
                    }
                    let base = hasher.finalize().into();
                    self.cache.insert(member, Placed::Unresolved { base, local: identity() });
                }
                break self.cache[&current];
            }
            position.insert(current, chain.len());
            chain.push(current);
            match first_ref_target(placement, "PlacementRelTo") {
                Some(parent) => {
                    referrer = current;
                    current = parent;
                }
                None => break Placed::At(identity()),
            }
        };
        for &member in chain.iter().rev() {
            let local = self.relative_matrix(member);
            placed = match placed {
                Placed::At(matrix) => Placed::At(matmul(&matrix, &local)),
                Placed::Unresolved { base, local: above } => {
                    Placed::Unresolved { base, local: matmul(&above, &local) }
                }
            };
            if cacheable {
                self.cache.insert(member, placed);
            }
        }
        Ok(Some(placed))
    }

    fn relative_matrix(&self, local_placement: i64) -> Matrix {
        self.ctx
            .entity(local_placement)
            .and_then(|placement| first_ref_target(placement, "RelativePlacement"))
            .map_or_else(identity, |step| axis2placement_matrix(step, self.ctx))
    }

    /// Stand-in for a placement the engine does not evaluate: its own content
    /// (intersections, axes, offsets) and where the grids owning its axes are.
    fn unresolved_base(&mut self, placement_step: i64) -> Result<[u8; 32], String> {
        let ctx = self.ctx;
        let entity = ctx.index[&placement_step];
        // (placement of a grid, the grid)
        let mut grid_placements: Vec<(i64, i64)> = Vec::new();
        let mut visited: HashSet<u32> = HashSet::from([entity]);
        let mut stack = vec![entity];
        while let Some(current) = stack.pop() {
            if let Some(&grid_placement) = self.grid_placement_of_axis.get(&current) {
                grid_placements.push(grid_placement);
            }
            for edge in ctx.graph.children(current) {
                if visited.insert(edge.target) {
                    stack.push(edge.target);
                }
            }
        }
        grid_placements.sort_unstable();
        grid_placements.dedup();
        let mut grids: Vec<[u8; 32]> = Vec::with_capacity(grid_placements.len());
        for (step, grid) in grid_placements {
            if let Some(placed) = self.resolve(step, grid)? {
                grids.push(placed.digest());
            }
        }
        grids.sort_unstable();

        let content = ctx
            .placement_content
            .get(&entity)
            .unwrap_or_else(|| panic!("no content digest for the placement #{placement_step}"));
        let mut hasher = Sha256::new();
        hasher.update(content);
        for grid in &grids {
            hasher.update(grid);
        }
        Ok(hasher.finalize().into())
    }
}

/// Spatial features of every signature root, keyed by entity index. An error
/// names a record whose placement reference cannot be a placement.
pub fn build_spatial_features(ctx: &SpatialCtx) -> Result<HashMap<u32, SpatialFeature>, String> {
    let mut out = HashMap::new();
    let mut resolver = Resolver::new(ctx);
    let mut visited: Vec<u32> = vec![u32::MAX; ctx.entities.len()];

    for (i, ent) in ctx.entities.iter().enumerate() {
        if !ent.is_root() {
            continue;
        }
        let placed = match first_ref_target(ent, "ObjectPlacement") {
            Some(step) => resolver.resolve(step, ent.step_id)?,
            None => None,
        };
        let (matrix, placement_unresolved) = match placed {
            Some(Placed::At(matrix)) => (Some(matrix), None),
            Some(unresolved) => (None, Some(hex::encode(unresolved.digest()))),
            None => (None, None),
        };
        let placement_q = matrix.map(quantize_matrix);

        let mut feature_positions = Vec::new();
        for edge in ctx.graph.children(i as u32) {
            let feature = &ctx.entities[edge.target as usize];
            if edge.domain != Domain::Geometry || !feature.is_root() {
                continue;
            }
            let feature_placed = match first_ref_target(feature, "ObjectPlacement") {
                Some(step) => resolver.resolve(step, feature.step_id)?,
                None => None,
            };
            feature_positions.push((edge.target, relative_position(&edge.label, placed, feature_placed)));
        }

        // Local points have no world position under an unresolved placement.
        let (mut centroid, mut aabb) = (None, None);
        if placement_unresolved.is_none() {
            let points = collect_geometry_points(i as u32, ctx, &mut visited);
            let world_points: Vec<[f64; 3]> = match &matrix {
                Some(m) => points.iter().map(|p| transform_point(m, *p)).collect(),
                None => points,
            };
            centroid = compute_centroid(&world_points);
            aabb = compute_aabb(&world_points);
        }
        if centroid.is_none() {
            if let Some(m) = &matrix {
                centroid = Some([m[0][3], m[1][3], m[2][3]]);
            }
        }
        if aabb.is_none() {
            if let Some(c) = centroid {
                aabb = Some([c[0], c[1], c[2], c[0], c[1], c[2]]);
            }
        }
        out.insert(
            i as u32,
            SpatialFeature {
                placement: placement_q,
                placement_unresolved,
                centroid,
                aabb,
                feature_positions,
            },
        );
    }
    Ok(out)
}

/// Digest of where a feature element sits in its host, for the relationship
/// class `label`: the feature's frame in the host's frame, quantized like a
/// placement, when both chains resolve; the two unresolved-chain digests when
/// either does not; which of the two has a placement when one has none.
fn relative_position(label: &str, host: Option<Placed>, feature: Option<Placed>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(label.as_bytes());
    match (host, feature) {
        (Some(Placed::At(host)), Some(Placed::At(feature))) => {
            hasher.update(b":frame");
            for value in quantize_matrix(matmul(&rigid_inverse(&host), &feature)) {
                hasher.update(value.to_le_bytes());
            }
        }
        (Some(host), Some(feature)) => {
            hasher.update(b":unresolved");
            hasher.update(host.digest());
            hasher.update(feature.digest());
        }
        (host, feature) => {
            hasher.update(b":unplaced");
            hasher.update([host.is_some() as u8, feature.is_some() as u8]);
        }
    }
    hasher.finalize().into()
}

/// Inverse of a rotation-plus-translation matrix.
fn rigid_inverse(m: &Matrix) -> Matrix {
    let mut out = identity();
    for i in 0..3 {
        for j in 0..3 {
            out[i][j] = m[j][i];
        }
    }
    for i in 0..3 {
        out[i][3] = -(out[i][0] * m[0][3] + out[i][1] * m[1][3] + out[i][2] * m[2][3]);
    }
    out
}

fn first_ref_target(ent: &Entity, attr: &str) -> Option<i64> {
    ent.refs.iter().find(|r| &*r.attr_name == attr).map(|r| r.target)
}

fn axis2placement_matrix(step: i64, ctx: &SpatialCtx) -> Matrix {
    let placement = match ctx.entity(step) {
        Some(p) => p,
        None => return identity(),
    };
    let location = first_ref_target(placement, "Location")
        .and_then(|id| ctx.point_coords.get(&id).copied())
        .unwrap_or([0.0, 0.0, 0.0]);
    // A frame the schema leaves undefined has a zero rotation part.
    let (x, z) = ctx.placement_axes.get(&step).copied().unwrap_or(([0.0; 3], [0.0; 3]));
    let y = cross(z, x);
    let [tx, ty, tz] = location;

    [
        [x[0], y[0], z[0], tx],
        [x[1], y[1], z[1], ty],
        [x[2], y[2], z[2], tz],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

/// Points of the shape entities in the geometry-edge closure of `root`; a
/// feature element is a root with a placement of its own and is not entered.
/// `visited[i] == root` marks entity `i` as seen for this root.
fn collect_geometry_points(root: u32, ctx: &SpatialCtx, visited: &mut [u32]) -> Vec<[f64; 3]> {
    let mut stack = vec![root];
    visited[root as usize] = root;
    let mut points = Vec::new();
    while let Some(current) = stack.pop() {
        let ent = &ctx.entities[current as usize];
        if &*ent.keyword == "IFCCARTESIANPOINT" {
            if let Some(c) = ctx.point_coords.get(&ent.step_id) {
                points.push(*c);
            }
        }
        for edge in ctx.graph.children(current) {
            if edge.domain == Domain::Geometry
                && ctx.entities[edge.target as usize].is_geometric
                && visited[edge.target as usize] != root
            {
                visited[edge.target as usize] = root;
                stack.push(edge.target);
            }
        }
    }
    points
}

fn transform_point(m: &Matrix, p: [f64; 3]) -> [f64; 3] {
    let [x, y, z] = p;
    let tx = m[0][0] * x + m[0][1] * y + m[0][2] * z + m[0][3];
    let ty = m[1][0] * x + m[1][1] * y + m[1][2] * z + m[1][3];
    let tz = m[2][0] * x + m[2][1] * y + m[2][2] * z + m[2][3];
    let tw = m[3][0] * x + m[3][1] * y + m[3][2] * z + m[3][3];
    if tw.abs() > 1e-12 && (tw - 1.0).abs() > 1e-12 {
        [tx / tw, ty / tw, tz / tw]
    } else {
        [tx, ty, tz]
    }
}

fn compute_centroid(points: &[[f64; 3]]) -> Option<[f64; 3]> {
    if points.is_empty() {
        return None;
    }
    let n = points.len() as f64;
    let mut s = [0.0f64; 3];
    for p in points {
        s[0] += p[0];
        s[1] += p[1];
        s[2] += p[2];
    }
    Some([s[0] / n, s[1] / n, s[2] / n])
}

fn compute_aabb(points: &[[f64; 3]]) -> Option<[f64; 6]> {
    if points.is_empty() {
        return None;
    }
    let mut min = points[0];
    let mut max = points[0];
    for p in &points[1..] {
        for i in 0..3 {
            if p[i] < min[i] {
                min[i] = p[i];
            }
            if p[i] > max[i] {
                max[i] = p[i];
            }
        }
    }
    Some([min[0], min[1], min[2], max[0], max[1], max[2]])
}

fn quantize_matrix(m: Matrix) -> Vec<i64> {
    let mut q = Vec::with_capacity(16);
    for row in m.iter() {
        for &v in row.iter() {
            q.push((v * PLACEMENT_SCALE).round() as i64);
        }
    }
    q
}

fn matmul(a: &Matrix, b: &Matrix) -> Matrix {
    let mut out = [[0.0f64; 4]; 4];
    for i in 0..4 {
        for j in 0..4 {
            let mut s = 0.0;
            for k in 0..4 {
                s += a[i][k] * b[k][j];
            }
            out[i][j] = s;
        }
    }
    out
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edges::tests::{ent, graph, Flags};

    fn shape() -> Flags {
        Flags { geometric: true, ..Flags::default() }
    }

    fn product() -> Flags {
        Flags { product: true, ..Flags::default() }
    }

    fn features(entities: &[Entity], points: &[(i64, [f64; 3])]) -> HashMap<u32, SpatialFeature> {
        let index: HashMap<i64, u32> =
            entities.iter().enumerate().map(|(i, e)| (e.step_id, i as u32)).collect();
        let graph = graph(entities);
        let point_coords: HashMap<i64, [f64; 3]> = points.iter().copied().collect();
        let placement_axes: HashMap<i64, ([f64; 3], [f64; 3])> = entities
            .iter()
            .filter(|e| &*e.keyword == "IFCAXIS2PLACEMENT3D")
            .map(|e| (e.step_id, ([1.0, 0.0, 0.0], [0.0, 0.0, 1.0])))
            .collect();
        let placement_content = HashMap::new();
        let ctx = SpatialCtx {
            entities,
            index: &index,
            graph: &graph,
            point_coords: &point_coords,
            placement_axes: &placement_axes,
            placement_content: &placement_content,
        };
        build_spatial_features(&ctx).expect("placements resolve")
    }

    #[test]
    fn placement_chain_yields_world_space_geometry() {
        // Wall placed at (10,0,0) with one local geometry point (1,2,3).
        let entities = vec![
            ent(1, "IFCWALL", product(), &[("ObjectPlacement", 2), ("Representation", 5)]),
            ent(2, "IFCLOCALPLACEMENT", Flags::default(), &[("RelativePlacement", 3)]),
            ent(3, "IFCAXIS2PLACEMENT3D", shape(), &[("Location", 4)]),
            ent(4, "IFCCARTESIANPOINT", shape(), &[]),
            ent(5, "IFCCARTESIANPOINT", shape(), &[]),
        ];
        let feats = features(&entities, &[(4, [10.0, 0.0, 0.0]), (5, [1.0, 2.0, 3.0])]);
        let f = feats.get(&0).expect("wall feature");

        assert_eq!(f.centroid, Some([11.0, 2.0, 3.0]));
        assert_eq!(f.aabb, Some([11.0, 2.0, 3.0, 11.0, 2.0, 3.0]));
        let placement = f.placement.as_ref().expect("placement matrix");
        assert_eq!(placement[3], 10_000_000);
    }

    #[test]
    fn host_extent_excludes_the_points_of_its_feature_elements() {
        let entities = vec![
            ent(1, "IFCWALL", product(), &[("Representation", 4)]),
            ent(2, "IFCOPENINGELEMENT", product(), &[("Representation", 5)]),
            ent(
                3,
                "IFCRELVOIDSELEMENT",
                Flags { relationship: true, ..Flags::default() },
                &[("RelatingBuildingElement", 1), ("RelatedOpeningElement", 2)],
            ),
            ent(4, "IFCCARTESIANPOINT", shape(), &[]),
            ent(5, "IFCCARTESIANPOINT", shape(), &[]),
        ];
        let feats = features(&entities, &[(4, [1.0, 2.0, 3.0]), (5, [9.0, 9.0, 9.0])]);

        assert_eq!(feats[&0].centroid, Some([1.0, 2.0, 3.0]));
        assert_eq!(feats[&0].aabb, Some([1.0, 2.0, 3.0, 1.0, 2.0, 3.0]));
        assert_eq!(feats[&1].centroid, Some([9.0, 9.0, 9.0]));
    }

    #[test]
    fn feature_position_is_relative_to_the_host() {
        // Wall at `wall_x`, opening at `opening_x`, both in world coordinates.
        let position = |wall_x: f64, opening_x: f64| {
            let entities = vec![
                ent(1, "IFCWALL", product(), &[("ObjectPlacement", 4)]),
                ent(2, "IFCOPENINGELEMENT", product(), &[("ObjectPlacement", 7)]),
                ent(
                    3,
                    "IFCRELVOIDSELEMENT",
                    Flags { relationship: true, ..Flags::default() },
                    &[("RelatingBuildingElement", 1), ("RelatedOpeningElement", 2)],
                ),
                ent(4, "IFCLOCALPLACEMENT", Flags::default(), &[("RelativePlacement", 5)]),
                ent(5, "IFCAXIS2PLACEMENT3D", shape(), &[("Location", 6)]),
                ent(6, "IFCCARTESIANPOINT", shape(), &[]),
                ent(7, "IFCLOCALPLACEMENT", Flags::default(), &[("RelativePlacement", 8)]),
                ent(8, "IFCAXIS2PLACEMENT3D", shape(), &[("Location", 9)]),
                ent(9, "IFCCARTESIANPOINT", shape(), &[]),
            ];
            let feats = features(&entities, &[(6, [wall_x, 0.0, 0.0]), (9, [opening_x, 0.0, 0.0])]);
            assert!(feats[&1].feature_positions.is_empty());
            let [(feature, digest)] = feats[&0].feature_positions[..] else { panic!("one feature") };
            assert_eq!(feature, 1);
            digest
        };
        assert_eq!(position(0.0, 1.0), position(40.0, 41.0));
        assert_ne!(position(0.0, 1.0), position(0.0, 1.5));
    }
}
