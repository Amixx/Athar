//! Per-instance canonicalization: STEP tokens + schema descriptors -> the
//! domain-filtered Merkle "parts", collected refs, and the numeric point/
//! direction coordinates the spatial stage needs.
//!
//! Produces a self-consistent, deterministic, discriminating canonical string
//! per attribute value. Reals are quantized (length/area/volume/angle/direction
//! scales, banker's rounding via `round`) so geometry comparison is robust to
//! float noise.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::rc::Rc;

use unicode_normalization::UnicodeNormalization;

use crate::descriptor::{Measure, SchemaDesc, Shape};
use crate::step::{Record, Token};

/// A canonicalized entity, holding only what the downstream native stages need.
pub struct Entity {
    pub step_id: i64,
    pub keyword: String, // STEP uppercase keyword; serves as entity_type / target_type
    pub canonical_class: String,
    pub is_product: bool,
    pub is_spatial: bool,
    pub guid: Option<String>,
    pub name: Option<String>,
    pub geom_parts: Vec<String>,
    pub data_parts: Vec<String>,
    pub refs: Vec<RefOut>,
    /// (attr_name, human-readable value) for data-domain scalar attributes —
    /// the report-facing `data_facts` side channel (see signatures.py). Keyed
    /// without STEP ids so reports can say which value changed.
    pub data_facts: Vec<(String, String)>,
}

pub struct RefOut {
    /// Interned attribute name. There are only a few hundred distinct attribute
    /// names in a schema but tens of millions of references, so this is a shared
    /// `Rc<str>` (a refcount bump per ref) rather than a fresh `String` each time.
    pub attr_name: Rc<str>,
    pub target: i64,
}

pub struct ParsedNative {
    pub entities: Vec<Entity>,
    pub id_to_keyword: HashMap<i64, String>,
    /// IfcCartesianPoint id -> (x, y, z) in unit-scaled space (Python parity:
    /// quantized length ints divided by 1e6).
    pub point_coords: HashMap<i64, [f64; 3]>,
    /// IfcDirection id -> direction ratios (same quantize/1e6 round-trip).
    pub dir_ratios: HashMap<i64, [f64; 3]>,
    /// Unhandled keywords (no schema descriptor) encountered, for diagnostics.
    pub unknown_keywords: Vec<String>,
}

const GEOM_HINTS: &[&str] = &[
    "Representation",
    "Representations",
    "RepresentationMaps",
    "Items",
    "Points",
    "Coordinates",
    "OuterCurve",
    "SweptArea",
    "BasisCurve",
    "Position",
    "MappedRepresentation",
    "MappingSource",
];
const PLACEMENT_ATTRS: &[&str] = &[
    "ObjectPlacement",
    "PlacementRelTo",
    "RelativePlacement",
    "Location",
    "Axis",
    "RefDirection",
];

fn is_geom_hint(name: &str) -> bool {
    GEOM_HINTS.contains(&name)
}
fn is_placement_attr(name: &str) -> bool {
    PLACEMENT_ATTRS.contains(&name)
}

fn matches_geometry(name: &str) -> bool {
    if is_placement_attr(name) {
        return false;
    }
    is_geom_hint(name)
        || name.contains("Coord")
        || name.contains("Direction")
        || name.contains("Point")
}

fn matches_data(name: &str) -> bool {
    if is_placement_attr(name) {
        return false;
    }
    if is_geom_hint(name) {
        return false;
    }
    name != "placement"
}

fn nfc_strip(s: &str) -> String {
    s.trim().nfc().collect()
}

pub fn canonicalize(
    records: &[Record],
    schema: &SchemaDesc,
    unit_factors: &HashMap<String, f64>,
) -> ParsedNative {
    let mut id_to_keyword: HashMap<i64, String> = HashMap::with_capacity(records.len());
    for rec in records {
        id_to_keyword.insert(rec.step_id, rec.keyword.clone());
    }

    let mut entities: Vec<Entity> = Vec::with_capacity(records.len());
    let mut point_coords: HashMap<i64, [f64; 3]> = HashMap::new();
    let mut dir_ratios: HashMap<i64, [f64; 3]> = HashMap::new();
    let mut unknown: HashMap<String, ()> = HashMap::new();
    // Intern attribute names once (keys borrow the schema, which outlives this
    // function): every reference then shares one `Rc<str>` instead of cloning
    // the name. Schemas have a few hundred names; files have tens of millions
    // of refs.
    let mut attr_intern: HashMap<&str, Rc<str>> = HashMap::new();

    for rec in records {
        let desc = match schema.get(&rec.keyword) {
            Some(d) => d,
            None => {
                unknown.entry(rec.keyword.clone()).or_insert(());
                continue;
            }
        };

        // Capture spatial coordinates straight from the tokens.
        if rec.keyword == "IFCCARTESIANPOINT" {
            if let Some(c) = coords_from_first_list(rec, &Measure::Length, unit_factors) {
                point_coords.insert(rec.step_id, c);
            }
        } else if rec.keyword == "IFCDIRECTION" {
            if let Some(c) = coords_from_first_list(rec, &Measure::Direction, unit_factors) {
                dir_ratios.insert(rec.step_id, c);
            }
        }

        let mut geom_parts: Vec<String> = Vec::new();
        let mut data_parts: Vec<String> = Vec::new();
        let mut data_facts: Vec<(String, String)> = Vec::new();
        let mut refs: Vec<RefOut> = Vec::new();

        // Attributes are zipped positionally with the schema descriptor.
        let n = rec.attrs.len().min(desc.attrs.len());
        for i in 0..n {
            let attr = &desc.attrs[i];
            let token = &rec.attrs[i];
            let attr_rc = attr_intern
                .entry(attr.name.as_str())
                .or_insert_with(|| Rc::from(attr.name.as_str()))
                .clone();
            if attr.name == "GlobalId" || attr.name == "OwnerHistory" {
                // Still collect refs (OwnerHistory etc.), but never hash these.
                collect_refs(token, &attr_rc, &mut refs);
                continue;
            }
            collect_refs(token, &attr_rc, &mut refs);

            let geom = matches_geometry(&attr.name);
            let data = matches_data(&attr.name);
            if !geom && !data {
                continue;
            }
            // A plain top-level entity ref contributes a Merkle edge, not a part
            // (mirrors `encode_attr` returning None). Skip before allocating.
            let ref_skip =
                matches!(token, Token::Ref(_)) && !matches!(attr.shape, Shape::Select);
            if !ref_skip {
                // Build the "attr=<name>:<enc>" part directly in one buffer,
                // encoding the value in place — no intermediate `enc` String, no
                // nested `format!`/`join` allocations.
                let mut part = String::new();
                part.push_str("attr=");
                part.push_str(&attr.name);
                part.push(':');
                encode_inner_into(&mut part, token, &attr.shape, attr.direction, unit_factors);
                if geom && data {
                    geom_parts.push(part.clone());
                    data_parts.push(part);
                } else if geom {
                    geom_parts.push(part);
                } else {
                    data_parts.push(part);
                }
                if data {
                    data_facts.push((
                        attr.name.clone(),
                        human_value(token, &attr.shape, attr.direction, unit_factors),
                    ));
                }
            }
        }

        // Python sorts attribute names before emitting parts; sort the encoded
        // parts so the per-entity payload order is deterministic regardless of
        // schema attribute order.
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

        entities.push(Entity {
            step_id: rec.step_id,
            keyword: rec.keyword.clone(),
            canonical_class: desc.class.clone(),
            is_product: desc.product,
            is_spatial: desc.spatial,
            guid,
            name,
            geom_parts,
            data_parts,
            refs,
            data_facts,
        });
    }

    ParsedNative {
        entities,
        id_to_keyword,
        point_coords,
        dir_ratios,
        unknown_keywords: unknown.into_keys().collect(),
    }
}

/// Read the first attribute as a list of reals, quantize each with `measure`,
/// and return the first three as `value/1e6` (matching `spatial._scalar_float`).
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
        out[i] = quantize_real(v, *measure, false, unit_factors) as f64 / 1_000_000.0;
    }
    Some(out)
}

fn collect_refs(token: &Token, attr_name: &Rc<str>, refs: &mut Vec<RefOut>) {
    match token {
        Token::Ref(target) => refs.push(RefOut {
            attr_name: Rc::clone(attr_name),
            target: *target,
        }),
        Token::List(items) => {
            for it in items {
                collect_refs(it, attr_name, refs);
            }
        }
        Token::Typed(_, inner) => {
            for it in inner {
                collect_refs(it, attr_name, refs);
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

/// Banker's rounding (round half to even), matching Python's `round()`.
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

// The encoders append into a caller-provided buffer (`*_into`) so a whole
// attribute value — including nested aggregates and typed wrappers — is built
// with a single allocation (the buffer) instead of a String per node plus
// per-aggregate `Vec<String>` + `join`. Output is byte-identical to the former
// returning versions. Thin `String`-returning wrappers are kept for unit tests.

fn encode_inner_into(
    out: &mut String,
    token: &Token,
    shape: &Shape,
    attr_dir: bool,
    units: &HashMap<String, f64>,
) {
    match shape {
        Shape::Select => encode_select_into(out, token, attr_dir, units),
        Shape::Agg { sorted, elem } => match token {
            Token::List(items) => {
                out.push('[');
                if *sorted {
                    // Sorted set/bag: still need the elements as separate strings
                    // to sort, but only this aggregate's direct children.
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
    units: &HashMap<String, f64>,
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
            let _ = write!(out, "{}", quantize_real(*r, measure, attr_dir, units));
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
    // ASCII-case-insensitive compare to the STEP logical literals — equivalent
    // to the old `to_uppercase()` for these ASCII patterns, but allocation-free.
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
    units: &HashMap<String, f64>,
) {
    let measure = Measure::from_keyword(keyword).unwrap_or(if attr_dir {
        Measure::Direction
    } else {
        Measure::Default
    });
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
    units: &HashMap<String, f64>,
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

/// Render a data attribute value for the report-facing `data_facts` channel.
/// Mirrors the intent of `signatures._human_value` (readable, not hashed); it
/// is a side channel, so exact formatting of rare numeric data is unimportant.
pub fn human_value(
    token: &Token,
    shape: &Shape,
    attr_dir: bool,
    units: &HashMap<String, f64>,
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

fn human_scalar(
    token: &Token,
    measure: Measure,
    attr_dir: bool,
    units: &HashMap<String, f64>,
) -> String {
    match token {
        Token::Ref(_) => "<ref>".to_string(),
        Token::Null => "<null>".to_string(),
        Token::Derived => "<derived>".to_string(),
        Token::Int(i) => i.to_string(),
        Token::Real(r) => quantize_real(*r, measure, attr_dir, units).to_string(),
        Token::Str(s) => nfc_strip(s),
        Token::Enum(e) => match e.to_uppercase().as_str() {
            "T" | "TRUE" => "true".to_string(),
            "F" | "FALSE" => "false".to_string(),
            other => other.to_string(),
        },
        Token::Typed(kw, inner) if inner.len() == 1 => {
            let m = Measure::from_keyword(kw).unwrap_or(if attr_dir {
                Measure::Direction
            } else {
                Measure::Default
            });
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

fn human_select(token: &Token, attr_dir: bool, units: &HashMap<String, f64>) -> String {
    match token {
        Token::Typed(kw, inner) if inner.len() == 1 => {
            let m = Measure::from_keyword(kw).unwrap_or(Measure::Default);
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
    fn length_quantization_is_unit_normalized() {
        // 3000 mm (LENGTHUNIT=0.001) and 3 m (LENGTHUNIT=1.0) must agree.
        let mm = units(&[("LENGTHUNIT", 0.001)]);
        let m = units(&[("LENGTHUNIT", 1.0)]);
        assert_eq!(quantize_real(3000.0, Measure::Length, false, &mm), 3_000_000);
        assert_eq!(quantize_real(3.0, Measure::Length, false, &m), 3_000_000);
    }

    #[test]
    fn direction_uses_its_own_scale() {
        // A default-measure real on a direction-named attribute uses 1e5.
        let none = units(&[]);
        assert_eq!(quantize_real(1.0, Measure::Default, true, &none), 100_000);
        assert_eq!(quantize_real(1.0, Measure::Default, false, &none), 1_000_000);
    }

    #[test]
    fn round_half_even_matches_python_round() {
        // Ties go to the even neighbor, like Python's round().
        assert_eq!(round_half_even(0.5), 0);
        assert_eq!(round_half_even(1.5), 2);
        assert_eq!(round_half_even(2.5), 2);
        assert_eq!(round_half_even(-0.5), 0);
        assert_eq!(round_half_even(2.4), 2);
        assert_eq!(round_half_even(2.6), 3);
    }
}
