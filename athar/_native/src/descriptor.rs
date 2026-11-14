//! Per-class schema descriptors handed from Python (built once per schema from
//! ifcopenshell's EXPRESS schema) and the measure tables the canonicalizer
//! needs. Python owns schema knowledge (attribute names, types, class
//! membership) and serializes it to JSON; Rust applies it to the STEP records.

use std::collections::HashMap;

use serde::Deserialize;

/// Quantization family for a real-valued leaf: selects the unit lookup and the
/// fixed-point scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Measure {
    Length,
    Area,
    Volume,
    Angle,
    Direction,
    Default,
}

impl Measure {
    pub fn unit_type(self) -> Option<&'static str> {
        match self {
            Measure::Length => Some("LENGTHUNIT"),
            Measure::Area => Some("AREAUNIT"),
            Measure::Volume => Some("VOLUMEUNIT"),
            Measure::Angle => Some("PLANEANGLEUNIT"),
            Measure::Direction | Measure::Default => None,
        }
    }

    pub fn scale(self) -> f64 {
        match self {
            Measure::Direction => 100_000.0,
            _ => 1_000_000.0,
        }
    }

    /// Measure implied by a STEP typed keyword (e.g. `IFCLENGTHMEASURE`).
    /// `None` means the keyword is not a recognized measure.
    pub fn from_keyword(keyword: &str) -> Option<Measure> {
        match keyword {
            "IFCLENGTHMEASURE" | "IFCPOSITIVELENGTHMEASURE" | "IFCNONNEGATIVELENGTHMEASURE" => {
                Some(Measure::Length)
            }
            "IFCAREAMEASURE" => Some(Measure::Area),
            "IFCVOLUMEMEASURE" => Some(Measure::Volume),
            "IFCPLANEANGLEMEASURE" | "IFCPOSITIVEPLANEANGLEMEASURE" => Some(Measure::Angle),
            _ => None,
        }
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

/// Whole-schema descriptor map, keyed by the UPPERCASE STEP keyword.
pub type SchemaDesc = HashMap<String, ClassDesc>;

pub fn parse_schema(json: &str) -> Result<SchemaDesc, String> {
    serde_json::from_str(json).map_err(|e| format!("bad schema descriptor json: {e}"))
}
