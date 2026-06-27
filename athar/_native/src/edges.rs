//! Native port of `athar/bottom/edge_policy.py`: classify direct attribute
//! references and project `IfcRel*` relationships into semantic edges.
//!
//! Everything keys off UPPERCASE STEP keywords (relationship names, target-type
//! prefixes, spatial roots) since that is how the tokenizer reports types. The
//! edge labels are therefore uppercase too; this is self-consistent because
//! both sides of a diff run through the same Rust, and labels only need to be
//! deterministic for Merkle child sorting.

use std::collections::HashMap;

use crate::canon::Entity;

pub const INCLUDE: &str = "include";
pub const CONTEXT: &str = "context";

pub const GEOMETRY: &str = "geometry";
pub const DATA: &str = "data";
pub const SPATIAL: &str = "spatial";
pub const TOPOLOGY: &str = "topology";
pub const PLACEMENT: &str = "placement";

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Edge {
    pub source: i64,
    pub target: i64,
    pub classification: &'static str,
    pub domain: &'static str,
    pub label: String,
}

const PLACEMENT_ATTRS: &[&str] = &[
    "ObjectPlacement",
    "PlacementRelTo",
    "RelativePlacement",
    "Location",
    "Axis",
    "RefDirection",
];
const DATA_ATTR_HINTS: &[&str] = &["HasProperties", "Quantities", "HasQuantities", "HasPropertySets"];
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
const GEOM_TARGET_PREFIXES: &[&str] = &[
    "IFCSHAPE",
    "IFCGEOMETRIC",
    "IFCCARTESIANPOINT",
    "IFCPOLYLINE",
    "IFCCURVE",
    "IFCPROFILE",
    "IFCREPRESENTATION",
    "IFCDIRECTION",
    "IFCAXIS2PLACEMENT",
];
const SPATIAL_ROOTS: &[&str] = &[
    "IFCPROJECT",
    "IFCSITE",
    "IFCBUILDING",
    "IFCBUILDINGSTOREY",
    "IFCSPACE",
];

struct Rule {
    relationship: &'static str,
    source_attr: &'static str,
    target_attr: &'static str,
    classification: &'static str,
    domain: &'static str,
    bidirectional: bool,
    dynamic_domain: bool,
}

const POLICY: &[Rule] = &[
    Rule { relationship: "IFCRELDEFINESBYPROPERTIES", source_attr: "RelatedObjects", target_attr: "RelatingPropertyDefinition", classification: INCLUDE, domain: DATA, bidirectional: false, dynamic_domain: false },
    Rule { relationship: "IFCRELASSOCIATESMATERIAL", source_attr: "RelatedObjects", target_attr: "RelatingMaterial", classification: INCLUDE, domain: DATA, bidirectional: false, dynamic_domain: false },
    Rule { relationship: "IFCRELCONTAINEDINSPATIALSTRUCTURE", source_attr: "RelatedElements", target_attr: "RelatingStructure", classification: INCLUDE, domain: SPATIAL, bidirectional: false, dynamic_domain: false },
    Rule { relationship: "IFCRELAGGREGATES", source_attr: "RelatedObjects", target_attr: "RelatingObject", classification: INCLUDE, domain: GEOMETRY, bidirectional: false, dynamic_domain: true },
    Rule { relationship: "IFCRELVOIDSELEMENT", source_attr: "RelatingBuildingElement", target_attr: "RelatedOpeningElement", classification: INCLUDE, domain: GEOMETRY, bidirectional: false, dynamic_domain: false },
    Rule { relationship: "IFCRELFILLSELEMENT", source_attr: "RelatingOpeningElement", target_attr: "RelatedBuildingElement", classification: CONTEXT, domain: TOPOLOGY, bidirectional: true, dynamic_domain: false },
    Rule { relationship: "IFCRELCONNECTSPATHELEMENTS", source_attr: "RelatingElement", target_attr: "RelatedElement", classification: CONTEXT, domain: TOPOLOGY, bidirectional: true, dynamic_domain: false },
    Rule { relationship: "IFCRELCONNECTSELEMENTS", source_attr: "RelatingElement", target_attr: "RelatedElement", classification: CONTEXT, domain: TOPOLOGY, bidirectional: true, dynamic_domain: false },
    Rule { relationship: "IFCRELDEFINESBYTYPE", source_attr: "RelatedObjects", target_attr: "RelatingType", classification: CONTEXT, domain: TOPOLOGY, bidirectional: false, dynamic_domain: false },
    Rule { relationship: "IFCRELDEFINESBYTYPE", source_attr: "RelatedObjects", target_attr: "RelatingType", classification: INCLUDE, domain: DATA, bidirectional: false, dynamic_domain: false },
];

fn classify_direct(attr_name: &str, target_keyword: &str) -> Option<(&'static str, &'static str)> {
    if PLACEMENT_ATTRS.contains(&attr_name) {
        Some((INCLUDE, PLACEMENT))
    } else if DATA_ATTR_HINTS.contains(&attr_name) {
        Some((INCLUDE, DATA))
    } else if GEOM_HINTS.contains(&attr_name) {
        Some((INCLUDE, GEOMETRY))
    } else if GEOM_TARGET_PREFIXES
        .iter()
        .any(|p| target_keyword.starts_with(p))
    {
        Some((INCLUDE, GEOMETRY))
    } else {
        None // ignore
    }
}

pub fn build_edges(entities: &[Entity], id_to_keyword: &HashMap<i64, String>) -> Vec<Edge> {
    let mut edges: Vec<Edge> = Vec::new();

    // Direct attribute references (non-relationship entities).
    for ent in entities {
        if ent.keyword.starts_with("IFCREL") {
            continue;
        }
        for r in &ent.refs {
            // Drop references to nonexistent targets (mirrors ifcopenshell's
            // load-time repair of broken refs to an empty slot).
            let target_kw = match id_to_keyword.get(&r.target) {
                Some(kw) => kw.as_str(),
                None => continue,
            };
            if let Some((classification, domain)) = classify_direct(&r.attr_name, target_kw) {
                edges.push(Edge {
                    source: ent.step_id,
                    target: r.target,
                    classification,
                    domain,
                    label: format!("{}.{}", ent.keyword, r.attr_name),
                });
            }
        }
    }

    // Relationship projection.
    for ent in entities {
        if !ent.keyword.starts_with("IFCREL") {
            continue;
        }
        let mut by_attr: HashMap<&str, Vec<i64>> = HashMap::new();
        for r in &ent.refs {
            if !id_to_keyword.contains_key(&r.target) {
                continue; // drop dangling relationship members
            }
            by_attr.entry(r.attr_name.as_str()).or_default().push(r.target);
        }
        for rule in POLICY {
            if rule.relationship != ent.keyword {
                continue;
            }
            let sources = by_attr.get(rule.source_attr).cloned().unwrap_or_default();
            let targets = by_attr.get(rule.target_attr).cloned().unwrap_or_default();
            for &s in &sources {
                for &t in &targets {
                    let domain = if rule.dynamic_domain {
                        let tk = id_to_keyword.get(&t).map(String::as_str).unwrap_or("");
                        if SPATIAL_ROOTS.contains(&tk) {
                            SPATIAL
                        } else {
                            rule.domain
                        }
                    } else {
                        rule.domain
                    };
                    edges.push(Edge {
                        source: s,
                        target: t,
                        classification: rule.classification,
                        domain,
                        label: ent.keyword.clone(),
                    });
                    if rule.bidirectional {
                        edges.push(Edge {
                            source: t,
                            target: s,
                            classification: rule.classification,
                            domain,
                            label: ent.keyword.clone(),
                        });
                    }
                }
            }
        }
    }

    // Dedup by full key, then sort deterministically.
    edges.sort();
    edges.dedup();
    edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canon::{Entity, RefOut};
    use std::collections::HashSet;

    fn ent(step_id: i64, keyword: &str, refs: &[(&str, i64)]) -> Entity {
        Entity {
            step_id,
            keyword: keyword.to_string(),
            canonical_class: keyword.to_string(),
            is_product: false,
            is_spatial: false,
            guid: None,
            name: None,
            geom_parts: Vec::new(),
            data_parts: Vec::new(),
            refs: refs
                .iter()
                .map(|(a, t)| RefOut { attr_name: a.to_string(), target: *t })
                .collect(),
            data_facts: Vec::new(),
        }
    }

    fn keyword_map(entities: &[Entity]) -> HashMap<i64, String> {
        entities.iter().map(|e| (e.step_id, e.keyword.clone())).collect()
    }

    #[test]
    fn rel_defines_by_type_projects_context_and_data_edges() {
        // Type linkage is both neighborhood context (topology) and inherited
        // data: the include/data edge carries type-level psets into the
        // occurrence's vh_data.
        let entities = vec![
            ent(1, "IFCWALL", &[]),
            ent(2, "IFCWALLTYPE", &[]),
            ent(3, "IFCRELDEFINESBYTYPE", &[("RelatedObjects", 1), ("RelatingType", 2)]),
        ];
        let edges = build_edges(&entities, &keyword_map(&entities));
        let mut projected: Vec<(&str, &str)> = edges
            .iter()
            .filter(|e| e.source == 1 && e.target == 2)
            .map(|e| (e.classification, e.domain))
            .collect();
        projected.sort();
        assert_eq!(projected, vec![(CONTEXT, TOPOLOGY), (INCLUDE, DATA)]);
    }

    #[test]
    fn direct_ref_to_geometry_target_is_geometry_edge() {
        // An otherwise-unhinted attribute whose target is a geometry class
        // still classifies as include/geometry by target prefix.
        let entities = vec![ent(1, "IFCWALL", &[("Foo", 2)]), ent(2, "IFCCARTESIANPOINT", &[])];
        let edges = build_edges(&entities, &keyword_map(&entities));
        assert!(edges
            .iter()
            .any(|e| e.source == 1 && e.target == 2 && e.classification == INCLUDE && e.domain == GEOMETRY));
    }

    #[test]
    fn dangling_relationship_members_are_dropped() {
        // RelatingType points at a nonexistent entity -> no projected edge.
        let entities = vec![
            ent(1, "IFCWALL", &[]),
            ent(3, "IFCRELDEFINESBYTYPE", &[("RelatedObjects", 1), ("RelatingType", 999)]),
        ];
        let edges = build_edges(&entities, &keyword_map(&entities));
        assert!(edges.iter().all(|e| e.target != 999));
    }

    #[test]
    fn policy_table_covers_required_relationships() {
        let present: HashSet<&str> = POLICY.iter().map(|r| r.relationship).collect();
        for required in [
            "IFCRELDEFINESBYPROPERTIES",
            "IFCRELASSOCIATESMATERIAL",
            "IFCRELCONTAINEDINSPATIALSTRUCTURE",
            "IFCRELAGGREGATES",
            "IFCRELVOIDSELEMENT",
            "IFCRELFILLSELEMENT",
            "IFCRELCONNECTSPATHELEMENTS",
            "IFCRELCONNECTSELEMENTS",
            "IFCRELDEFINESBYTYPE",
        ] {
            assert!(present.contains(required), "policy missing {required}");
        }
    }

    #[test]
    fn policy_table_has_no_duplicate_rule_keys() {
        let mut keys: Vec<(&str, &str, &str, &str, &str, bool)> = POLICY
            .iter()
            .map(|r| {
                (
                    r.relationship,
                    r.source_attr,
                    r.target_attr,
                    r.classification,
                    r.domain,
                    r.bidirectional,
                )
            })
            .collect();
        let total = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), total, "duplicate rule keys in POLICY");
    }
}
