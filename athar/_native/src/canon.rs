//! Per-instance canonicalization: STEP tokens + schema descriptors -> the
//! Merkle "parts" of each entity, its outgoing references, and the numeric
//! point/direction coordinates the spatial stage needs.
//!
//! Every attribute is hashed unless `UNHASHED_ATTRIBUTES` lists it. Shape-model
//! classes put their non-text attributes in the geometry parts and their text
//! attributes in the data parts; every other class has data parts only. Reals
//! are quantized (length/area/volume/angle/direction scales, round half to
//! even) so comparison is robust to float noise.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::rc::Rc;

use unicode_normalization::UnicodeNormalization;

use crate::descriptor::{AttrDesc, ClassDesc, Measure, SchemaDesc, Shape};
use crate::step::{Record, Token};

/// `RefOut::ord` of a reference whose position in its attribute is not content.
pub const UNORDERED: u32 = u32::MAX;

/// Attributes that never enter a hash: `(STEP keyword or "*", attribute, reason)`.
pub const UNHASHED_ATTRIBUTES: &[(&str, &str, &str)] = &[
    ("*", "GlobalId", "identity, compared by the matcher"),
    ("*", "OwnerHistory", "provenance: who wrote the entity and when"),
    (
        "IFCGEOMETRICREPRESENTATIONCONTEXT",
        "Precision",
        "exporter tolerance setting, not model content",
    ),
    (
        "IFCGEOMETRICREPRESENTATIONSUBCONTEXT",
        "Precision",
        "exporter tolerance setting, not model content",
    ),
];

fn is_unhashed(keyword: &str, attr: &str) -> bool {
    UNHASHED_ATTRIBUTES
        .iter()
        .any(|(kw, name, _)| *name == attr && (*kw == "*" || *kw == keyword))
}

pub struct Entity {
    pub step_id: i64,
    /// STEP uppercase keyword.
    pub keyword: Rc<str>,
    pub canonical_class: Rc<str>,
    pub is_product: bool,
    pub is_spatial: bool,
    pub is_project: bool,
    pub is_relationship: bool,
    pub is_geometric: bool,
    pub guid: Option<String>,
    pub name: Option<String>,
    pub geom_parts: Vec<String>,
    pub data_parts: Vec<String>,
    pub refs: Vec<RefOut>,
    /// (attr_name, human-readable value) of the attributes hashed into the
    /// data parts, for the report's `data_delta`.
    pub data_facts: Vec<(Rc<str>, String)>,
    /// The same for the scalar attributes hashed into the geometry parts.
    pub geom_facts: Vec<(Rc<str>, String)>,
}

impl Entity {
    /// Signature roots: the entities that get a signature vector.
    pub fn is_root(&self) -> bool {
        self.is_product || self.is_spatial || self.is_project
    }
}

pub struct RefOut {
    pub attr_name: Rc<str>,
    pub target: i64,
    /// Ordinal among the attribute's references when the innermost aggregate
    /// holding the reference is a LIST or ARRAY, else `UNORDERED`.
    pub ord: u32,
    /// The innermost aggregate holding the reference is a SET.
    pub in_set: bool,
    /// The target's content is expanded into this entity's parts.
    pub inlined: bool,
}

pub struct ParsedNative {
    pub entities: Vec<Entity>,
    /// STEP id -> index into `entities`.
    pub index: HashMap<i64, u32>,
    /// STEP ids of records whose keyword has no schema descriptor.
    pub skipped_ids: HashSet<i64>,
    /// IfcCartesianPoint id -> (x, y, z), unit-scaled and quantized.
    pub point_coords: HashMap<i64, [f64; 3]>,
    /// IfcAxis2Placement2D/3D id -> its unit X and Z axes, for every such
    /// placement whose frame is defined (see `placement_axes`).
    pub placement_axes: HashMap<i64, ([f64; 3], [f64; 3])>,
    pub unknown_keywords: Vec<String>,
}

fn nfc_strip(s: &str) -> String {
    s.trim().nfc().collect()
}

/// A single reference or a flat aggregate of references: fully described by
/// the entity's outgoing edges, so it needs no part.
fn is_plain_refs(token: &Token) -> bool {
    match token {
        Token::Ref(_) => true,
        Token::List(items) => {
            !items.is_empty() && items.iter().all(|it| matches!(it, Token::Ref(_)))
        }
        _ => false,
    }
}

pub fn canonicalize(
    records: &[Record],
    schema: &SchemaDesc,
    unit_factors: &HashMap<String, f64>,
    record_unit_factors: &HashMap<i64, HashMap<String, f64>>,
) -> ParsedNative {
    let tri_pointlists = collect_triangulated_pointlists(records, schema, unit_factors);
    let dir_ratios: HashMap<i64, [f64; 3]> = records
        .iter()
        .filter(|rec| rec.keyword == "IFCDIRECTION")
        .filter_map(|rec| unit_direction(rec).map(|(ratios, _)| (rec.step_id, ratios)))
        .collect();

    let mut entities: Vec<Entity> = Vec::with_capacity(records.len());
    let mut index: HashMap<i64, u32> = HashMap::with_capacity(records.len());
    let mut skipped_ids: HashSet<i64> = HashSet::new();
    let mut point_coords: HashMap<i64, [f64; 3]> = HashMap::new();
    let mut placement_axes_by_id: HashMap<i64, ([f64; 3], [f64; 3])> = HashMap::new();
    let mut unknown: HashSet<String> = HashSet::new();
    let mut attr_intern: HashMap<&str, Rc<str>> = HashMap::new();
    let mut class_intern: HashMap<&str, (Rc<str>, Rc<str>)> = HashMap::new();

    for rec in records {
        let (keyword_key, desc) = match schema.get_key_value(&rec.keyword) {
            Some((k, d)) => (k.as_str(), d),
            None => {
                unknown.insert(rec.keyword.clone());
                skipped_ids.insert(rec.step_id);
                continue;
            }
        };

        if rec.keyword == "IFCCARTESIANPOINT" {
            if let Some(c) = coords_from_first_list(rec, &Measure::LENGTH, unit_factors) {
                point_coords.insert(rec.step_id, c);
            }
        }
        // A direction has no magnitude: its ratios are hashed at unit length.
        let unit_ratios: Option<Token> = if rec.keyword == "IFCDIRECTION" {
            unit_direction(rec)
                .map(|(ratios, dim)| Token::List(ratios[..dim].iter().map(|v| Token::Real(*v)).collect()))
        } else {
            None
        };
        let axes = placement_axes(rec, desc, &dir_ratios);
        if let Some(axes) = axes {
            placement_axes_by_id.insert(rec.step_id, axes);
        }
        let frame = canonical_frame(rec, desc, &dir_ratios, axes);

        let mut geom_parts: Vec<String> = Vec::new();
        let mut data_parts: Vec<String> = Vec::new();
        let mut data_facts: Vec<(Rc<str>, String)> = Vec::new();
        let mut geom_facts: Vec<(Rc<str>, String)> = Vec::new();
        let mut refs: Vec<RefOut> = Vec::new();

        let tri_canon = if rec.keyword == "IFCTRIANGULATEDFACESET" {
            canonical_triangle_parts(rec, desc, &tri_pointlists)
        } else {
            None
        };
        if let Some(parts) = tri_canon.as_ref() {
            geom_parts.extend(parts.iter().cloned());
        }
        if let Some(part) = frame.as_ref() {
            geom_parts.push(part.clone());
        }

        let overlaid: HashMap<String, f64>;
        let attr_unit_factors = match record_unit_factors.get(&rec.step_id) {
            Some(overrides) => {
                overlaid = unit_factors
                    .iter()
                    .chain(overrides)
                    .map(|(unit, factor)| (unit.clone(), *factor))
                    .collect();
                &overlaid
            }
            None => unit_factors,
        };
        let attr_units = Units { factors: attr_unit_factors, measures: &schema.measures };

        // Attributes are zipped positionally with the schema descriptor.
        let n = rec.attrs.len().min(desc.attrs.len());
        for i in 0..n {
            let attr = &desc.attrs[i];
            let token = match (&unit_ratios, i) {
                (Some(ratios), 0) => ratios,
                _ => attr_token(&rec.attrs[i], attr),
            };
            let attr_rc = attr_intern
                .entry(attr.name.as_str())
                .or_insert_with(|| Rc::from(attr.name.as_str()))
                .clone();
            let inlined = (tri_canon.is_some()
                && matches!(attr.name.as_str(), "Coordinates" | "CoordIndex" | "PnIndex"))
                || (frame.is_some() && FRAME_ATTRIBUTES.contains(&attr.name.as_str()));
            let mut ord = 0u32;
            collect_refs(token, &attr.shape, None, &mut ord, inlined, &attr_rc, &mut refs);
            if inlined || is_unhashed(&rec.keyword, &attr.name) {
                continue;
            }
            let unset = matches!(token, Token::Null)
                || (attr.derived && matches!(token, Token::Derived));
            if unset || is_plain_refs(token) {
                continue;
            }
            let mut part = String::new();
            part.push_str("attr=");
            part.push_str(&attr.name);
            part.push(':');
            encode_inner_into(&mut part, token, &attr.shape, attr.direction, &attr_units);
            let geom = desc.geometric && !attr.text;
            if !geom {
                data_facts.push((attr_rc, fact_value(token, &attr.shape, attr.direction, &attr_units)));
            } else if !matches!(attr.shape, Shape::Agg { .. }) {
                geom_facts.push((attr_rc, fact_value(token, &attr.shape, attr.direction, &attr_units)));
            }
            if geom {
                geom_parts.push(part);
            } else {
                data_parts.push(part);
            }
        }

        geom_parts.sort();
        data_parts.sort();

        let guid = desc
            .guid
            .and_then(|idx| rec.attrs.get(idx))
            .and_then(|t| match t {
                Token::Str(s) => Some(s.clone()),
                _ => None,
            });
        let name = desc
            .name
            .and_then(|idx| rec.attrs.get(idx))
            .and_then(|t| match t {
                Token::Str(s) => {
                    let n = nfc_strip(s);
                    if n.is_empty() {
                        None
                    } else {
                        Some(n)
                    }
                }
                _ => None,
            });

        let (keyword, canonical_class) = class_intern
            .entry(keyword_key)
            .or_insert_with(|| (Rc::from(keyword_key), Rc::from(desc.class.as_str())))
            .clone();
        index.insert(rec.step_id, entities.len() as u32);
        entities.push(Entity {
            step_id: rec.step_id,
            keyword,
            canonical_class,
            is_product: desc.product,
            is_spatial: desc.spatial,
            is_project: desc.project,
            is_relationship: desc.relationship,
            is_geometric: desc.geometric,
            guid,
            name,
            geom_parts,
            data_parts,
            refs,
            data_facts,
            geom_facts,
        });
    }

    ParsedNative {
        entities,
        index,
        skipped_ids,
        point_coords,
        placement_axes: placement_axes_by_id,
        unknown_keywords: unknown.into_iter().collect(),
    }
}

/// Ratios of an `IfcDirection` at unit length, padded to three components,
/// with the dimension count. `None` for a zero or malformed direction.
fn unit_direction(rec: &Record) -> Option<([f64; 3], usize)> {
    let Some(Token::List(items)) = rec.attrs.first() else { return None };
    if items.is_empty() || items.len() > 3 {
        return None;
    }
    let mut ratios = [0.0f64; 3];
    for (i, item) in items.iter().enumerate() {
        ratios[i] = match item {
            Token::Real(r) => *r,
            Token::Int(n) => *n as f64,
            _ => return None,
        };
    }
    unit(ratios).map(|u| (u, items.len()))
}

/// `v` at unit length; `None` for a zero or non-finite vector. Dividing by the
/// largest component first keeps tiny ratios from underflowing when squared.
fn unit(v: [f64; 3]) -> Option<[f64; 3]> {
    if v.iter().any(|c| !c.is_finite()) {
        return None;
    }
    let largest = v.iter().fold(0.0f64, |m, c| m.max(c.abs()));
    if largest == 0.0 {
        return None;
    }
    let w = [v[0] / largest, v[1] / largest, v[2] / largest];
    let norm = dot(w, w).sqrt();
    Some([w[0] / norm, w[1] / norm, w[2] / norm])
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

/// The unit vector `v` minus its component along the unit vector `axis`, at
/// unit length; `None` when `v` is parallel to `axis` within `1e-12`.
fn reject(v: [f64; 3], axis: [f64; 3]) -> Option<[f64; 3]> {
    let d = dot(v, axis);
    let r = [v[0] - d * axis[0], v[1] - d * axis[1], v[2] - d * axis[2]];
    if dot(r, r).sqrt() <= 1e-12 {
        return None;
    }
    unit(r)
}

/// EXPRESS `IfcBuildAxes`: the unit X and Z axes of an `IfcAxis2Placement3D`
/// from its optional `RefDirection` and `Axis`.
fn build_axes(axis: Option<[f64; 3]>, ref_direction: Option<[f64; 3]>) -> Option<([f64; 3], [f64; 3])> {
    let z = axis.unwrap_or([0.0, 0.0, 1.0]);
    first_proj_axis(z, ref_direction).map(|x| (x, z))
}

/// EXPRESS `IfcFirstProjAxis`: the X axis for a given unit Z axis.
fn first_proj_axis(z: [f64; 3], arg: Option<[f64; 3]>) -> Option<[f64; 3]> {
    let v = match arg {
        Some(v) => v,
        None if z[0].abs() > 1.0 - 1e-9 => [0.0, 1.0, 0.0],
        None => [1.0, 0.0, 0.0],
    };
    reject(v, z)
}

/// Attributes replaced by the frame part of `canonical_frame`.
const FRAME_ATTRIBUTES: &[&str] =
    &["Axis", "RefDirection", "Axis1", "Axis2", "Axis3", "Scale", "Scale2", "Scale3"];

fn attribute<'a>(rec: &'a Record, desc: &ClassDesc, name: &str) -> Option<&'a Token> {
    desc.attrs.iter().position(|a| a.name == name).and_then(|i| rec.attrs.get(i))
}

/// The direction an axis attribute names. Outer `None`: the attribute is set
/// but not to a usable direction (not a reference, a dangling one, a zero or
/// malformed `IfcDirection`). Inner `None`: the attribute is omitted.
fn axis_attribute(
    rec: &Record,
    desc: &ClassDesc,
    directions: &HashMap<i64, [f64; 3]>,
    name: &str,
) -> Option<Option<[f64; 3]>> {
    match attribute(rec, desc, name) {
        None | Some(Token::Null) | Some(Token::Derived) => Some(None),
        Some(Token::Ref(id)) => directions.get(id).map(|d| Some(*d)),
        Some(_) => None,
    }
}

/// Unit X and Z axes of an `IfcAxis2Placement2D` or `IfcAxis2Placement3D`.
/// `None` for other classes and for a frame the schema rules leave undefined:
/// an axis attribute that names no usable direction, or parallel axes. The
/// frame part of the hash and the placement matrices both come from here.
fn placement_axes(
    rec: &Record,
    desc: &ClassDesc,
    directions: &HashMap<i64, [f64; 3]>,
) -> Option<([f64; 3], [f64; 3])> {
    let axis = |name: &str| axis_attribute(rec, desc, directions, name);
    match rec.keyword.as_str() {
        "IFCAXIS2PLACEMENT2D" => {
            let z = [0.0, 0.0, 1.0];
            Some((reject(axis("RefDirection")?.unwrap_or([1.0, 0.0, 0.0]), z)?, z))
        }
        "IFCAXIS2PLACEMENT3D" => build_axes(axis("Axis")?, axis("RefDirection")?),
        _ => None,
    }
}

/// The coordinate frame of a placement or transformation operator, resolved
/// by the schema's derivation rules (`IfcBuildAxes`, `IfcBuild2Axes`,
/// `IfcBaseAxis`, the `Scl` attributes), so an omitted axis and its default
/// written out are one frame. `None` for other classes and for frames the
/// rules leave undefined (a referenced axis that is not a direction, parallel
/// axes); those keep their attribute-wise encoding. `placement` is the
/// `placement_axes` result for `rec`.
fn canonical_frame(
    rec: &Record,
    desc: &ClassDesc,
    directions: &HashMap<i64, [f64; 3]>,
    placement: Option<([f64; 3], [f64; 3])>,
) -> Option<String> {
    let token = |name: &str| attribute(rec, desc, name);
    let axis = |name: &str| axis_attribute(rec, desc, directions, name);
    let scale = |name: &str, default: f64| -> Option<f64> {
        match token(name) {
            None | Some(Token::Null) | Some(Token::Derived) => Some(default),
            Some(Token::Real(r)) => Some(*r),
            Some(Token::Int(n)) => Some(*n as f64),
            Some(_) => None,
        }
    };
    let (axes, scales): (Vec<[f64; 3]>, Vec<f64>) = match rec.keyword.as_str() {
        "IFCAXIS1PLACEMENT" => (vec![axis("Axis")?.unwrap_or([0.0, 0.0, 1.0])], vec![]),
        "IFCAXIS2PLACEMENT2D" => (vec![placement?.0], vec![]),
        "IFCAXIS2PLACEMENT3D" => {
            let (x, z) = placement?;
            (vec![x, z], vec![])
        }
        "IFCCARTESIANTRANSFORMATIONOPERATOR2D" | "IFCCARTESIANTRANSFORMATIONOPERATOR2DNONUNIFORM" => {
            let orthogonal = |d: [f64; 3]| [-d[1], d[0], 0.0];
            let (u1, u2) = match (axis("Axis1")?, axis("Axis2")?) {
                (Some(a1), a2) => {
                    let u2 = orthogonal(a1);
                    match a2 {
                        Some(a2) if dot(a2, u2) < 0.0 => (a1, [-u2[0], -u2[1], 0.0]),
                        _ => (a1, u2),
                    }
                }
                (None, Some(a2)) => {
                    let u1 = orthogonal(a2);
                    ([-u1[0], -u1[1], 0.0], a2)
                }
                (None, None) => ([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]),
            };
            let s1 = scale("Scale", 1.0)?;
            let mut scales = vec![s1];
            if rec.keyword.ends_with("NONUNIFORM") {
                scales.push(scale("Scale2", s1)?);
            }
            (vec![u1, u2], scales)
        }
        "IFCCARTESIANTRANSFORMATIONOPERATOR3D" | "IFCCARTESIANTRANSFORMATIONOPERATOR3DNONUNIFORM" => {
            let z = axis("Axis3")?.unwrap_or([0.0, 0.0, 1.0]);
            let x = first_proj_axis(z, axis("Axis1")?)?;
            let y = reject(reject(axis("Axis2")?.unwrap_or([0.0, 1.0, 0.0]), z)?, x)?;
            let s1 = scale("Scale", 1.0)?;
            let mut scales = vec![s1];
            if rec.keyword.ends_with("NONUNIFORM") {
                scales.push(scale("Scale2", s1)?);
                scales.push(scale("Scale3", s1)?);
            }
            (vec![x, y, z], scales)
        }
        _ => return None,
    };
    let none = HashMap::new();
    let mut part = String::from("frame=");
    for a in &axes {
        part.push('[');
        for (k, v) in a.iter().enumerate() {
            if k > 0 {
                part.push(',');
            }
            let _ = write!(part, "{}", quantize_real(*v, Measure::Direction, false, &none));
        }
        part.push(']');
    }
    for s in &scales {
        let _ = write!(part, "*{}", quantize_real(*s, Measure::Default, false, &none));
    }
    Some(part)
}

/// Quantized vertex tables for the point lists referenced by triangulated
/// face sets. Only those lists are materialized; point lists consumed by
/// index-order-sensitive entities (e.g. IfcIndexedPolyCurve) are untouched.
fn collect_triangulated_pointlists(
    records: &[Record],
    schema: &SchemaDesc,
    unit_factors: &HashMap<String, f64>,
) -> HashMap<i64, Vec<[i64; 3]>> {
    let mut wanted: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for rec in records {
        if rec.keyword != "IFCTRIANGULATEDFACESET" {
            continue;
        }
        let Some(desc) = schema.get(&rec.keyword) else { continue };
        let Some(idx) = desc.attrs.iter().position(|a| a.name == "Coordinates") else { continue };
        if let Some(Token::Ref(target)) = rec.attrs.get(idx) {
            wanted.insert(*target);
        }
    }
    if wanted.is_empty() {
        return HashMap::new();
    }

    let mut out: HashMap<i64, Vec<[i64; 3]>> = HashMap::with_capacity(wanted.len());
    for rec in records {
        if rec.keyword != "IFCCARTESIANPOINTLIST3D" || !wanted.contains(&rec.step_id) {
            continue;
        }
        let Some(Token::List(rows)) = rec.attrs.first() else { continue };
        let mut points: Vec<[i64; 3]> = Vec::with_capacity(rows.len());
        let mut ok = true;
        'rows: for row in rows {
            let Token::List(coords) = row else {
                ok = false;
                break;
            };
            if coords.len() < 3 {
                ok = false;
                break;
            }
            let mut point = [0i64; 3];
            for (k, coord) in coords.iter().take(3).enumerate() {
                let value = match coord {
                    Token::Real(r) => *r,
                    Token::Int(n) => *n as f64,
                    _ => {
                        ok = false;
                        break 'rows;
                    }
                };
                point[k] = quantize_real(value, Measure::LENGTH, false, unit_factors);
            }
            points.push(point);
        }
        if ok {
            out.insert(rec.step_id, points);
        }
    }
    out
}

/// Order-canonical hash input for an IfcTriangulatedFaceSet: each triangle is
/// expanded to its three quantized vertex triples (PnIndex resolved away),
/// cyclically rotated to its lexicographically-smallest form so `(a,b,c)` and
/// `(b,c,a)` collide while a reversed winding `(a,c,b)` stays distinct. One
/// part per triangle; the caller's part sort makes the triangle list a set.
/// Returns None (caller keeps the raw encoding) when Normals are present or
/// any index/reference is irregular.
fn canonical_triangle_parts(
    rec: &Record,
    desc: &ClassDesc,
    pointlists: &HashMap<i64, Vec<[i64; 3]>>,
) -> Option<Vec<String>> {
    let attr_token = |name: &str| -> Option<&Token> {
        desc.attrs
            .iter()
            .position(|a| a.name == name)
            .and_then(|i| rec.attrs.get(i))
    };

    if let Some(normals) = attr_token("Normals") {
        if !matches!(normals, Token::Null) {
            return None;
        }
    }
    let Some(Token::Ref(coords_ref)) = attr_token("Coordinates") else {
        return None;
    };
    let points = pointlists.get(coords_ref)?;

    let pn: Option<Vec<usize>> = match attr_token("PnIndex") {
        None | Some(Token::Null) => None,
        Some(Token::List(items)) => {
            let mut mapped = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    Token::Int(n) if *n >= 1 => mapped.push(*n as usize),
                    _ => return None,
                }
            }
            Some(mapped)
        }
        Some(_) => return None,
    };
    let resolve = |index: i64| -> Option<[i64; 3]> {
        if index < 1 {
            return None;
        }
        let mut position = index as usize;
        if let Some(pn) = pn.as_ref() {
            position = *pn.get(position - 1)?;
        }
        points.get(position - 1).copied()
    };

    let Some(Token::List(triangles)) = attr_token("CoordIndex") else {
        return None;
    };
    let mut parts: Vec<String> = Vec::with_capacity(triangles.len());
    for triangle in triangles {
        let Token::List(indices) = triangle else {
            return None;
        };
        if indices.len() != 3 {
            return None;
        }
        let mut verts = [[0i64; 3]; 3];
        for (k, idx_token) in indices.iter().enumerate() {
            let Token::Int(raw) = idx_token else {
                return None;
            };
            verts[k] = resolve(*raw)?;
        }
        let rotations = [
            [verts[0], verts[1], verts[2]],
            [verts[1], verts[2], verts[0]],
            [verts[2], verts[0], verts[1]],
        ];
        let canonical = rotations.iter().min().unwrap();
        let mut part = String::with_capacity(64);
        part.push_str("tri:");
        for vertex in canonical {
            part.push('[');
            for (k, coord) in vertex.iter().enumerate() {
                if k > 0 {
                    part.push(',');
                }
                part.push('r');
                let _ = write!(part, "{coord}");
            }
            part.push(']');
        }
        parts.push(part);
    }
    Some(parts)
}

/// Read spatial coordinates in SI units without rounding before frame composition.
fn coords_from_first_list(
    rec: &Record,
    measure: &Measure,
    unit_factors: &HashMap<String, f64>,
) -> Option<[f64; 3]> {
    let items = match rec.attrs.first() {
        Some(Token::List(items)) if !items.is_empty() => items,
        _ => return None,
    };
    let mut out = [0.0f64; 3];
    for (i, it) in items.iter().take(3).enumerate() {
        let v = match it {
            Token::Real(r) => *r,
            Token::Int(n) => *n as f64,
            Token::Typed(kw, inner) if inner.len() == 1 => match &inner[0] {
                Token::Real(r) => *r,
                Token::Int(n) => *n as f64,
                _ => return None,
            },
            _ => return None,
        };
        let factor = measure.unit_type().and_then(|u| unit_factors.get(u)).copied().unwrap_or(1.0);
        out[i] = v * factor;
    }
    Some(out)
}

/// `sorted` is the kind of the innermost aggregate around `token`: `None`
/// outside any aggregate, `Some(true)` in a SET, `Some(false)` in a LIST or
/// ARRAY.
fn collect_refs(
    token: &Token,
    shape: &Shape,
    sorted: Option<bool>,
    next_ord: &mut u32,
    inlined: bool,
    attr_name: &Rc<str>,
    refs: &mut Vec<RefOut>,
) {
    match token {
        Token::Ref(target) => {
            let ord = if sorted == Some(false) {
                let ord = *next_ord;
                *next_ord += 1;
                ord
            } else {
                UNORDERED
            };
            refs.push(RefOut {
                attr_name: Rc::clone(attr_name),
                target: *target,
                ord,
                in_set: sorted == Some(true),
                inlined,
            });
        }
        Token::List(items) => {
            let (elem, elem_sorted) = match shape {
                Shape::Agg { sorted, elem } => (elem.as_ref(), Some(*sorted)),
                other => (other, sorted),
            };
            for it in items {
                collect_refs(it, elem, elem_sorted, next_ord, inlined, attr_name, refs);
            }
        }
        Token::Typed(_, inner) => {
            for it in inner {
                collect_refs(it, shape, sorted, next_ord, inlined, attr_name, refs);
            }
        }
        _ => {}
    }
}

fn quantize_real(
    value: f64,
    measure: Measure,
    attr_direction: bool,
    unit_factors: &HashMap<String, f64>,
) -> i64 {
    if !value.is_finite() {
        return 0;
    }
    let effective = if measure == Measure::Default && attr_direction {
        Measure::Direction
    } else {
        measure
    };
    let unit_scale = effective
        .unit_type()
        .and_then(|u| unit_factors.get(u).copied())
        .filter(|f| f.is_finite())
        .unwrap_or(1.0);
    let scaled = value * unit_scale * effective.scale();
    round_half_even(scaled)
}

/// Round half to even.
fn round_half_even(x: f64) -> i64 {
    let floor = x.floor();
    let diff = x - floor;
    let rounded = if diff > 0.5 {
        floor + 1.0
    } else if diff < 0.5 {
        floor
    } else {
        // exactly .5 -> round to even
        if (floor as i64) % 2 == 0 {
            floor
        } else {
            floor + 1.0
        }
    };
    rounded as i64
}

// ----- canonical-string encoding ------------------------------------------

/// The unit context of one record: unit factors by unit type, and the measure
/// of each measure type keyword.
pub struct Units<'a> {
    pub factors: &'a HashMap<String, f64>,
    pub measures: &'a HashMap<String, Measure>,
}

impl Units<'_> {
    /// The measure a typed value's keyword names; an untyped measure at a
    /// direction attribute otherwise.
    fn measure(&self, keyword: &str, attr_dir: bool) -> Measure {
        match self.measures.get(keyword) {
            Some(measure) => *measure,
            None if attr_dir => Measure::Direction,
            None => Measure::Default,
        }
    }
}

// The encoders append into a caller-provided buffer, so a whole attribute
// value, nested aggregates and typed wrappers included, is one allocation.

/// Exporters write `$` where the entity redeclares an attribute as DERIVE.
fn attr_token<'a>(token: &'a Token, attr: &AttrDesc) -> &'a Token {
    const DERIVED: &Token = &Token::Derived;
    if attr.derived && matches!(token, Token::Null) {
        DERIVED
    } else {
        token
    }
}

fn encode_inner_into(
    out: &mut String,
    token: &Token,
    shape: &Shape,
    attr_dir: bool,
    units: &Units,
) {
    match shape {
        Shape::Select => encode_select_into(out, token, attr_dir, units),
        Shape::Agg { sorted, elem } => match token {
            Token::List(items) => {
                out.push('[');
                if *sorted {
                    let mut encs: Vec<String> = items
                        .iter()
                        .map(|it| {
                            let mut s = String::new();
                            encode_inner_into(&mut s, it, elem, attr_dir, units);
                            s
                        })
                        .collect();
                    encs.sort();
                    for (i, e) in encs.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push_str(e);
                    }
                } else {
                    for (i, it) in items.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        encode_inner_into(out, it, elem, attr_dir, units);
                    }
                }
                out.push(']');
            }
            Token::Null => out.push('$'),
            other => encode_inner_into(out, other, elem, attr_dir, units),
        },
        Shape::Leaf { m } => encode_scalar_into(out, token, *m, attr_dir, units),
    }
}

fn encode_scalar_into(
    out: &mut String,
    token: &Token,
    measure: Measure,
    attr_dir: bool,
    units: &Units,
) {
    match token {
        Token::Ref(_) => out.push('R'),
        Token::Null => out.push('$'),
        Token::Derived => out.push('*'),
        Token::Int(i) => {
            out.push('i');
            let _ = write!(out, "{i}");
        }
        Token::Real(r) => {
            out.push('r');
            let _ = write!(out, "{}", quantize_real(*r, measure, attr_dir, units.factors));
        }
        Token::Str(s) => encode_string_into(out, s),
        Token::Enum(e) => encode_enum_into(out, e),
        Token::Typed(kw, inner) => encode_typed_into(out, kw, inner, attr_dir, units),
        Token::List(items) => {
            out.push('[');
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                encode_scalar_into(out, it, measure, attr_dir, units);
            }
            out.push(']');
        }
    }
}

fn encode_string_into(out: &mut String, s: &str) {
    let n = nfc_strip(s);
    if n.eq_ignore_ascii_case(".TRUE.") || n.eq_ignore_ascii_case(".T.") {
        out.push_str("b1");
    } else if n.eq_ignore_ascii_case(".FALSE.") || n.eq_ignore_ascii_case(".F.") {
        out.push_str("b0");
    } else {
        out.push_str("s'");
        out.push_str(&n);
    }
}

fn encode_enum_into(out: &mut String, e: &str) {
    if e.eq_ignore_ascii_case("T") || e.eq_ignore_ascii_case("TRUE") {
        out.push_str("b1");
    } else if e.eq_ignore_ascii_case("F") || e.eq_ignore_ascii_case("FALSE") {
        out.push_str("b0");
    } else if e.eq_ignore_ascii_case("U") || e.eq_ignore_ascii_case("UNKNOWN") {
        out.push('$');
    } else {
        out.push('e');
        out.push_str(e);
    }
}

fn encode_typed_into(
    out: &mut String,
    keyword: &str,
    inner: &[Token],
    attr_dir: bool,
    units: &Units,
) {
    let measure = units.measure(keyword, attr_dir);
    out.push_str("T(");
    out.push_str(keyword);
    out.push(':');
    if inner.len() == 1 {
        encode_scalar_into(out, &inner[0], measure, attr_dir, units);
    } else {
        out.push('[');
        for (i, it) in inner.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            encode_scalar_into(out, it, measure, attr_dir, units);
        }
        out.push(']');
    }
    out.push(')');
}

fn encode_select_into(
    out: &mut String,
    token: &Token,
    attr_dir: bool,
    units: &Units,
) {
    match token {
        Token::Ref(_) => out.push_str("sel(R)"),
        Token::Typed(kw, inner) => {
            out.push_str("sel(");
            encode_typed_into(out, kw, inner, attr_dir, units);
            out.push(')');
        }
        other => {
            out.push_str("sel(");
            encode_scalar_into(out, other, Measure::Default, attr_dir, units);
            out.push(')');
        }
    }
}

#[cfg(test)]
fn encode_string(s: &str) -> String {
    let mut out = String::new();
    encode_string_into(&mut out, s);
    out
}

// ----- human-readable rendering (report data facts) -----------------------

const FACT_VALUE_LIMIT: usize = 96;

/// A fact value as reported: verbatim up to `FACT_VALUE_LIMIT` bytes, its
/// length and a digest beyond.
pub fn bounded_fact(value: &str) -> String {
    if value.len() <= FACT_VALUE_LIMIT {
        value.to_string()
    } else {
        format!("{} bytes, sha256:{}", value.len(), &crate::sha256_hex(value.as_bytes())[..12])
    }
}

/// A single string, bare or under a type tag other than `IFCBINARY`.
fn is_text(token: &Token) -> bool {
    match token {
        Token::Str(_) => true,
        Token::Typed(kw, inner) if inner.len() == 1 && kw != "IFCBINARY" => is_text(&inner[0]),
        _ => false,
    }
}

/// An attribute value for the report-facing `data_facts` channel (readable,
/// never hashed). A text longer than `FACT_VALUE_LIMIT` bytes keeps the
/// leading characters that fit, followed by its byte length and a digest of
/// the whole text.
fn fact_value(token: &Token, shape: &Shape, attr_dir: bool, units: &Units) -> String {
    let value = human_value(token, shape, attr_dir, units);
    if value.len() <= FACT_VALUE_LIMIT || !is_text(token) {
        return bounded_fact(&value);
    }
    let mut cut = FACT_VALUE_LIMIT;
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{}… ({} bytes, sha256:{})",
        &value[..cut],
        value.len(),
        &crate::sha256_hex(value.as_bytes())[..12]
    )
}

fn human_value(
    token: &Token,
    shape: &Shape,
    attr_dir: bool,
    units: &Units,
) -> String {
    match shape {
        Shape::Select => human_select(token, attr_dir, units),
        Shape::Agg { elem, .. } => match token {
            Token::List(items) => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|it| human_value(it, elem, attr_dir, units))
                    .collect();
                format!("[{}]", parts.join(", "))
            }
            other => human_value(other, elem, attr_dir, units),
        },
        Shape::Leaf { m } => human_scalar(token, *m, attr_dir, units),
    }
}

/// A real as people read it: the hashed quantity as a decimal number in the
/// SI unit its measure is normalized to, followed by that unit. Two reals
/// print alike exactly when they hash alike.
fn human_real(value: f64, measure: Measure, attr_dir: bool, units: &Units) -> String {
    let effective = if measure == Measure::Default && attr_dir { Measure::Direction } else { measure };
    let quantized = quantize_real(value, measure, attr_dir, units.factors);
    let scale = effective.scale() as u64;
    let digits = scale.ilog10() as usize;
    let magnitude = quantized.unsigned_abs();
    let mut out = String::new();
    if quantized < 0 {
        out.push('-');
    }
    out.push_str(&(magnitude / scale).to_string());
    let fraction = format!("{:0digits$}", magnitude % scale);
    let fraction = fraction.trim_end_matches('0');
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(fraction);
    }
    let unit = match effective.unit_type() {
        Some("LENGTHUNIT") => " m",
        Some("AREAUNIT") => " m2",
        Some("VOLUMEUNIT") => " m3",
        Some("PLANEANGLEUNIT") => " rad",
        Some("MASSUNIT") => " kg",
        Some("TIMEUNIT") => " s",
        _ => "",
    };
    out.push_str(unit);
    out
}

fn human_scalar(
    token: &Token,
    measure: Measure,
    attr_dir: bool,
    units: &Units,
) -> String {
    match token {
        Token::Ref(_) => "<ref>".to_string(),
        Token::Null => "<null>".to_string(),
        Token::Derived => "<derived>".to_string(),
        Token::Int(i) => i.to_string(),
        Token::Real(r) => human_real(*r, measure, attr_dir, units),
        Token::Str(s) => nfc_strip(s),
        Token::Enum(e) => match e.to_uppercase().as_str() {
            "T" | "TRUE" => "true".to_string(),
            "F" | "FALSE" => "false".to_string(),
            other => other.to_string(),
        },
        Token::Typed(kw, inner) if inner.len() == 1 => {
            let m = units.measure(kw, attr_dir);
            format!("{}: {}", kw, human_scalar(&inner[0], m, attr_dir, units))
        }
        Token::Typed(kw, _) => kw.clone(),
        Token::List(items) => {
            let parts: Vec<String> = items
                .iter()
                .map(|it| human_scalar(it, measure, attr_dir, units))
                .collect();
            format!("[{}]", parts.join(", "))
        }
    }
}

fn human_select(token: &Token, attr_dir: bool, units: &Units) -> String {
    match token {
        Token::Typed(kw, inner) if inner.len() == 1 => {
            let m = units.measure(kw, false);
            format!("{}: {}", kw, human_scalar(&inner[0], m, attr_dir, units))
        }
        other => human_scalar(other, Measure::Default, attr_dir, units),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn measures() -> HashMap<String, Measure> {
        [("IFCLENGTHMEASURE", "LENGTHUNIT"), ("IFCTHERMALTRANSMITTANCEMEASURE", "THERMALTRANSMITTANCEUNIT")]
            .into_iter()
            .map(|(keyword, unit_type)| (keyword.to_string(), Measure::Unit(unit_type)))
            .collect()
    }

    #[test]
    fn string_scalars_preserve_numeric_literals() {
        // Numeric string literals stay strings (never coerced to int/bool).
        assert_eq!(encode_string("0"), "s'0");
        assert_eq!(encode_string("1"), "s'1");
        assert_eq!(encode_string("My Wall"), "s'My Wall");
    }

    #[test]
    fn only_explicit_step_logicals_become_booleans() {
        assert_eq!(encode_string(".TRUE."), "b1");
        assert_eq!(encode_string(".T."), "b1");
        assert_eq!(encode_string(".FALSE."), "b0");
        assert_eq!(encode_string(".F."), "b0");
        // Plain words are not logicals.
        assert_eq!(encode_string("TRUE"), "s'TRUE");
        assert_eq!(encode_string("FALSE"), "s'FALSE");
    }

    #[test]
    fn unset_equals_derived_only_at_derive_redeclared_attributes() {
        let factors = units(&[]);
        let measures = measures();
        let none = Units { factors: &factors, measures: &measures };
        let encode = |t: &Token, derived: bool| {
            let attr = AttrDesc {
                name: "Precision".to_string(),
                direction: false,
                derived,
                text: false,
                shape: Shape::Leaf { m: Measure::Default },
            };
            let mut s = String::new();
            encode_inner_into(&mut s, attr_token(t, &attr), &attr.shape, false, &none);
            s
        };
        assert_eq!(encode(&Token::Null, true), encode(&Token::Derived, true));
        assert_ne!(encode(&Token::Null, false), encode(&Token::Derived, false));
    }

    #[test]
    fn fact_values_are_bounded() {
        let factors = units(&[]);
        let measures = measures();
        let none = Units { factors: &factors, measures: &measures };
        let shape = Shape::Agg { sorted: false, elem: Box::new(Shape::Leaf { m: Measure::Default }) };
        let short = fact_value(&Token::List(vec![Token::Int(1), Token::Int(2)]), &shape, false, &none);
        assert_eq!(short, "[1, 2]");
        let long = |last: i64| {
            let mut items = vec![Token::Int(7); 200];
            items.push(Token::Int(last));
            fact_value(&Token::List(items), &shape, false, &none)
        };
        assert!(long(1).len() <= FACT_VALUE_LIMIT);
        assert!(long(1).starts_with("603 bytes, sha256:"));
        assert_ne!(long(1), long(2));
    }

    #[test]
    fn long_text_facts_keep_a_readable_start() {
        let factors = units(&[]);
        let measures = measures();
        let none = Units { factors: &factors, measures: &measures };
        let leaf = Shape::Leaf { m: Measure::Default };
        let text = |tail: &str| format!("{}{tail}", "é".repeat(60));
        let fact = |tail: &str| fact_value(&Token::Str(text(tail)), &leaf, false, &none);
        assert_eq!(fact("x"), format!("{}… (121 bytes, sha256:{})", "é".repeat(48), &crate::sha256_hex(text("x").as_bytes())[..12]));
        assert_ne!(fact("x"), fact("y"));
        let odd = fact_value(&Token::Str(format!("a{}", "é".repeat(60))), &leaf, false, &none);
        assert!(odd.starts_with(&format!("a{}… (121 bytes", "é".repeat(47))));

        let tagged = Token::Typed("IFCTEXT".to_string(), vec![Token::Str("t".repeat(200))]);
        assert!(fact_value(&tagged, &Shape::Select, false, &none).starts_with("IFCTEXT: tttt"));
        let list = Shape::Agg { sorted: false, elem: Box::new(Shape::Leaf { m: Measure::Default }) };
        let strings = Token::List(vec![Token::Str("t".repeat(200))]);
        assert!(fact_value(&strings, &list, false, &none).starts_with("202 bytes, sha256:"));
    }

    #[test]
    fn reals_read_as_si_numbers_with_their_unit() {
        let factors = units(&[("LENGTHUNIT", 0.001), ("AREAUNIT", 0.000001)]);
        let measures = measures();
        let mm = Units { factors: &factors, measures: &measures };
        assert_eq!(human_real(3000.0, Measure::LENGTH, false, &mm), "3 m");
        assert_eq!(human_real(-12.5, Measure::LENGTH, false, &mm), "-0.0125 m");
        assert_eq!(human_real(2_500_000.0, Measure::Unit("AREAUNIT"), false, &mm), "2.5 m2");
        assert_eq!(human_real(0.25, Measure::Default, false, &mm), "0.25");
        assert_eq!(human_real(-0.5, Measure::Default, true, &mm), "-0.5");
        assert_eq!(human_real(0.0, Measure::Unit("PLANEANGLEUNIT"), false, &mm), "0 rad");
        assert_eq!(
            human_real(3000.0000001, Measure::LENGTH, false, &mm),
            human_real(3000.0, Measure::LENGTH, false, &mm)
        );
    }

    #[test]
    fn typed_measures_scale_by_the_factor_of_their_unit_type() {
        let factors = units(&[("THERMALTRANSMITTANCEUNIT", 5.678)]);
        let measures = measures();
        let u = Units { factors: &factors, measures: &measures };
        let value = Token::Typed("IFCTHERMALTRANSMITTANCEMEASURE".to_string(), vec![Token::Real(1.0)]);
        let mut encoded = String::new();
        encode_select_into(&mut encoded, &value, false, &u);
        assert_eq!(encoded, "sel(T(IFCTHERMALTRANSMITTANCEMEASURE:r5678000))");
        assert_eq!(fact_value(&value, &Shape::Select, false, &u), "IFCTHERMALTRANSMITTANCEMEASURE: 5.678");
    }

    #[test]
    fn length_quantization_is_unit_normalized() {
        // 3000 mm (LENGTHUNIT=0.001) and 3 m (LENGTHUNIT=1.0) must agree.
        let mm = units(&[("LENGTHUNIT", 0.001)]);
        let m = units(&[("LENGTHUNIT", 1.0)]);
        assert_eq!(quantize_real(3000.0, Measure::LENGTH, false, &mm), 3_000_000);
        assert_eq!(quantize_real(3.0, Measure::LENGTH, false, &m), 3_000_000);
    }

    #[test]
    fn direction_uses_its_own_scale() {
        // A default-measure real on a direction-named attribute uses 1e5.
        let factors = units(&[]);
        let measures = measures();
        let none = Units { factors: &factors, measures: &measures };
        assert_eq!(quantize_real(1.0, Measure::Default, true, none.factors), 100_000);
        assert_eq!(quantize_real(1.0, Measure::Default, false, none.factors), 1_000_000);
    }

    #[test]
    fn any_nonzero_direction_has_a_unit_vector() {
        assert_eq!(unit([0.0, 0.0, 1e-13]), Some([0.0, 0.0, 1.0]));
        assert_eq!(unit([0.0, 0.0, -1e-300]), Some([0.0, 0.0, -1.0]));
        assert_eq!(unit([0.0, 0.0, 0.0]), None);
        assert_eq!(unit([f64::NAN, 0.0, 1.0]), None);
    }

    #[test]
    fn a_2d_placement_frame_lies_in_its_plane() {
        let desc = ClassDesc {
            class: "IfcAxis2Placement2D".to_string(),
            product: false,
            spatial: false,
            project: false,
            relationship: false,
            geometric: true,
            guid: None,
            name: None,
            attrs: ["Location", "RefDirection"]
                .iter()
                .map(|name| AttrDesc {
                    name: name.to_string(),
                    direction: false,
                    derived: false,
                    text: false,
                    shape: Shape::Select,
                })
                .collect(),
        };
        let rec = Record {
            step_id: 1,
            keyword: "IFCAXIS2PLACEMENT2D".to_string(),
            attrs: vec![Token::Ref(2), Token::Ref(3)],
        };
        let axes = |ratios: [f64; 3]| placement_axes(&rec, &desc, &HashMap::from([(3, unit(ratios).unwrap())]));
        assert_eq!(axes([1.0, 0.0, 1.0]), Some(([1.0, 0.0, 0.0], [0.0, 0.0, 1.0])));
        assert_eq!(axes([0.0, 0.0, 2.0]), None);
    }

    #[test]
    fn round_half_even_sends_ties_to_the_even_neighbor() {
        assert_eq!(round_half_even(0.5), 0);
        assert_eq!(round_half_even(1.5), 2);
        assert_eq!(round_half_even(2.5), 2);
        assert_eq!(round_half_even(-0.5), 0);
        assert_eq!(round_half_even(2.4), 2);
        assert_eq!(round_half_even(2.6), 3);
    }
}
