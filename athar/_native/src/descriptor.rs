//! Per-class schema descriptors handed from Python (built once per schema from
//! ifcopenshell's EXPRESS schema) and the measure tables the canonicalizer
//! needs. Python owns schema knowledge (attribute names, types, class
//! membership) and serializes it to JSON; Rust applies it to the STEP records.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use serde::Deserialize;

/// Quantization family for a real-valued leaf: selects the unit lookup and the
/// fixed-point scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    /// A measure of the unit type (`IfcUnitEnum` or `IfcDerivedUnitEnum`
    /// value) whose file factor scales it to SI.
    Unit(&'static str),
    Direction,
    Default,
}

impl Measure {
    pub const LENGTH: Measure = Measure::Unit("LENGTHUNIT");

    pub fn unit_type(self) -> Option<&'static str> {
        match self {
            Measure::Unit(unit_type) => Some(unit_type),
            Measure::Direction | Measure::Default => None,
        }
    }

    pub fn scale(self) -> f64 {
        match self {
            Measure::Direction => 100_000.0,
            _ => 1_000_000.0,
        }
    }
}

/// Unit types live for the process: there are a few dozen per schema.
fn intern(unit_type: String) -> &'static str {
    static INTERNED: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());
    let mut interned = INTERNED.lock().expect("unit type interner poisoned");
    if let Some(known) = interned.get(unit_type.as_str()) {
        return known;
    }
    let leaked: &'static str = Box::leak(unit_type.into_boxed_str());
    interned.insert(leaked);
    leaked
}

impl<'de> Deserialize<'de> for Measure {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Option::<String>::deserialize(deserializer)?.map_or(Measure::Default, |u| Measure::Unit(intern(u))))
    }
}

/// The canonicalization shape of one attribute position.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "k")]
pub enum Shape {
    /// Scalar leaf; `m` gives the measure for bare reals at this position.
    #[serde(rename = "leaf")]
    Leaf { m: Measure },
    /// Aggregate; `sorted` is true for SET/BAG (order is not content), false
    /// for LIST/ARRAY (position is content).
    #[serde(rename = "agg")]
    Agg { sorted: bool, elem: Box<Shape> },
    /// Select — the value self-describes via its STEP type tag.
    #[serde(rename = "select")]
    Select,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AttrDesc {
    pub name: String,
    /// True when `attr_name.upper()` contains "DIRECTION" — used as the
    /// fallback measure for typed/default reals under this attribute.
    #[serde(default)]
    pub direction: bool,
    /// True where this entity redeclares the attribute as DERIVE.
    pub derived: bool,
    /// String-valued (label/text/identifier, or an aggregate of them): naming
    /// metadata, never shape content.
    pub text: bool,
    pub shape: Shape,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClassDesc {
    /// Canonical class name (`*StandardCase` / `*ElementedCase` collapsed).
    pub class: String,
    pub product: bool,
    pub spatial: bool,
    /// `IfcProject`: a signature root that is neither product nor spatial.
    pub project: bool,
    /// Subtype of `IfcRelationship`.
    pub relationship: bool,
    /// Shape-model class (representation, representation item, profile): its
    /// non-text attributes enter `vh_geometry`, its text attributes `vh_data`.
    pub geometric: bool,
    /// Attribute index of GlobalId / Name, if the class declares them.
    pub guid: Option<usize>,
    pub name: Option<usize>,
    pub attrs: Vec<AttrDesc>,
}

/// Whole-schema descriptors: classes keyed by the UPPERCASE STEP keyword, and
/// the measure of every measure type keyword with a unit type.
#[derive(Debug, Deserialize)]
pub struct SchemaDesc {
    pub classes: HashMap<String, ClassDesc>,
    pub measures: HashMap<String, Measure>,
}

impl SchemaDesc {
    pub fn get(&self, keyword: &str) -> Option<&ClassDesc> {
        self.classes.get(keyword)
    }

    pub fn get_key_value(&self, keyword: &str) -> Option<(&String, &ClassDesc)> {
        self.classes.get_key_value(keyword)
    }
}

pub fn parse_schema(json: &str) -> Result<SchemaDesc, String> {
    serde_json::from_str(json).map_err(|e| format!("bad schema descriptor json: {e}"))
}
