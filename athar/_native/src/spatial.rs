//! Native port of `athar/bottom/spatial.py`: resolve `ObjectPlacement` chains
//! into quantized world matrices, and compute centroid + AABB from the
//! geometry-domain point cloud.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::canon::Entity;

const PLACEMENT_SCALE: f64 = 1_000_000.0;

pub struct SpatialFeature {
    pub placement: Option<Vec<i64>>,
    pub centroid: Option<[f64; 3]>,
    pub aabb: Option<[f64; 6]>,
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

pub struct SpatialCtx<'a> {
    pub by_id: &'a HashMap<i64, &'a Entity>,
    pub geometry_adj: &'a HashMap<i64, Vec<i64>>,
    pub point_coords: &'a HashMap<i64, [f64; 3]>,
    pub dir_ratios: &'a HashMap<i64, [f64; 3]>,
}

pub fn build_spatial_features(
    entities: &[Entity],
    ctx: &SpatialCtx,
) -> HashMap<i64, SpatialFeature> {
    let mut out = HashMap::new();
    let mut transform_cache: HashMap<i64, Matrix> = HashMap::new();

    for ent in entities {
        if !(ent.is_product || ent.is_spatial) {
            continue;
        }
        let matrix = placement_matrix_for_entity(ent, ctx, &mut transform_cache);
        let placement_q = matrix.map(quantize_matrix);

        let points = collect_geometry_points(ent.step_id, ctx);
        let world_points: Vec<[f64; 3]> = match &matrix {
            Some(m) => points.iter().map(|p| transform_point(m, *p)).collect(),
            None => points,
        };
        let mut centroid = compute_centroid(&world_points);
        let mut aabb = compute_aabb(&world_points);
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
            ent.step_id,
            SpatialFeature {
                placement: placement_q,
                centroid,
                aabb,
            },
        );
    }
    out
}

fn first_ref_target(ent: &Entity, attr: &str) -> Option<i64> {
    ent.refs.iter().find(|r| &*r.attr_name == attr).map(|r| r.target)
}

fn placement_matrix_for_entity(
    ent: &Entity,
    ctx: &SpatialCtx,
    cache: &mut HashMap<i64, Matrix>,
) -> Option<Matrix> {
    let placement_step = first_ref_target(ent, "ObjectPlacement")?;
    let mut seen = HashSet::new();
    resolve_local_placement(placement_step, ctx, cache, &mut seen)
}

fn resolve_local_placement(
    placement_step: i64,
    ctx: &SpatialCtx,
    cache: &mut HashMap<i64, Matrix>,
    seen: &mut HashSet<i64>,
) -> Option<Matrix> {
    if let Some(m) = cache.get(&placement_step) {
        return Some(*m);
    }
    if seen.contains(&placement_step) {
        return None;
    }
    seen.insert(placement_step);

    let placement = ctx.by_id.get(&placement_step)?;
    let rel_to = first_ref_target(placement, "PlacementRelTo");
    let relative = first_ref_target(placement, "RelativePlacement");

    let mut base = identity();
    if let Some(parent_step) = rel_to {
        if let Some(parent) = resolve_local_placement(parent_step, ctx, cache, seen) {
            base = parent;
        }
    }
    let local = match relative {
        Some(step) => axis2placement_matrix(step, ctx),
        None => identity(),
    };
    let matrix = matmul(&base, &local);
    cache.insert(placement_step, matrix);
    Some(matrix)
}

fn axis2placement_matrix(step: i64, ctx: &SpatialCtx) -> Matrix {
    let placement = match ctx.by_id.get(&step) {
        Some(p) => p,
        None => return identity(),
    };
    let location = first_ref_target(placement, "Location")
        .and_then(|id| ctx.point_coords.get(&id).copied())
        .unwrap_or([0.0, 0.0, 0.0]);
    let z_axis = first_ref_target(placement, "Axis")
        .and_then(|id| ctx.dir_ratios.get(&id).copied())
        .unwrap_or([0.0, 0.0, 1.0]);
    let x_axis = first_ref_target(placement, "RefDirection")
        .and_then(|id| ctx.dir_ratios.get(&id).copied())
        .unwrap_or([1.0, 0.0, 0.0]);

    let z = normalize(z_axis);
    let mut x = normalize(orthogonalize(x_axis, z));
    let y = normalize(cross(z, x));
    x = normalize(cross(y, z));
    let [tx, ty, tz] = location;

    [
        [x[0], y[0], z[0], tx],
        [x[1], y[1], z[1], ty],
        [x[2], y[2], z[2], tz],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

fn collect_geometry_points(step_id: i64, ctx: &SpatialCtx) -> Vec<[f64; 3]> {
    let mut queue = VecDeque::new();
    queue.push_back(step_id);
    let mut seen = HashSet::new();
    let mut points = Vec::new();
    while let Some(current) = queue.pop_front() {
        if !seen.insert(current) {
            continue;
        }
        if let Some(ent) = ctx.by_id.get(&current) {
            if ent.keyword == "IFCCARTESIANPOINT" {
                if let Some(c) = ctx.point_coords.get(&current) {
                    points.push(*c);
                }
            }
        }
        if let Some(neighbors) = ctx.geometry_adj.get(&current) {
            for &nxt in neighbors {
                if !seen.contains(&nxt) {
                    queue.push_back(nxt);
                }
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

fn normalize(v: [f64; 3]) -> [f64; 3] {
    let norm = (v[0] * v[0] + v[1] * v[1] + v[2] * v[2]).sqrt();
    if norm <= 1e-12 {
        [0.0, 0.0, 0.0]
    } else {
        [v[0] / norm, v[1] / norm, v[2] / norm]
    }
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn orthogonalize(v: [f64; 3], axis: [f64; 3]) -> [f64; 3] {
    let dot = v[0] * axis[0] + v[1] * axis[1] + v[2] * axis[2];
    [
        v[0] - dot * axis[0],
        v[1] - dot * axis[1],
        v[2] - dot * axis[2],
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canon::{Entity, RefOut};

    fn ent(step_id: i64, keyword: &str, is_product: bool, refs: &[(&str, i64)]) -> Entity {
        Entity {
            step_id,
            keyword: keyword.to_string(),
            canonical_class: keyword.to_string(),
            is_product,
            is_spatial: false,
            guid: None,
            name: None,
            geom_parts: Vec::new(),
            data_parts: Vec::new(),
            refs: refs
                .iter()
                .map(|(a, t)| RefOut { attr_name: std::rc::Rc::from(*a), target: *t })
                .collect(),
            data_facts: Vec::new(),
        }
    }

    #[test]
    fn placement_chain_yields_world_space_geometry() {
        // Wall placed at (10,0,0); its geometry point is local (1,2,3). The
        // centroid/AABB must be in world space (11,2,3) and the placement's
        // X translation must quantize to 10 * 1e6.
        let entities = vec![
            ent(1, "IFCWALL", true, &[("ObjectPlacement", 2)]),
            ent(2, "IFCLOCALPLACEMENT", false, &[("RelativePlacement", 3)]),
            ent(3, "IFCAXIS2PLACEMENT3D", false, &[("Location", 4)]),
            ent(4, "IFCCARTESIANPOINT", false, &[]),
            ent(5, "IFCCARTESIANPOINT", false, &[]),
        ];
        let by_id: HashMap<i64, &Entity> = entities.iter().map(|e| (e.step_id, e)).collect();
        let mut geometry_adj: HashMap<i64, Vec<i64>> = HashMap::new();
        geometry_adj.insert(1, vec![5]);
        let mut point_coords: HashMap<i64, [f64; 3]> = HashMap::new();
        point_coords.insert(4, [10.0, 0.0, 0.0]);
        point_coords.insert(5, [1.0, 2.0, 3.0]);
        let dir_ratios: HashMap<i64, [f64; 3]> = HashMap::new();

        let ctx = SpatialCtx {
            by_id: &by_id,
            geometry_adj: &geometry_adj,
            point_coords: &point_coords,
            dir_ratios: &dir_ratios,
        };
        let feats = build_spatial_features(&entities, &ctx);
        let f = feats.get(&1).expect("wall feature");

        assert_eq!(f.centroid, Some([11.0, 2.0, 3.0]));
        assert_eq!(f.aabb, Some([11.0, 2.0, 3.0, 11.0, 2.0, 3.0]));
        let placement = f.placement.as_ref().expect("placement matrix");
        assert_eq!(placement[3], 10_000_000); // row 0, col 3 = X translation * 1e6
    }
}
