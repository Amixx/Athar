//! Semantic edges of the signature graph.
//!
//! Coverage is the default: every reference becomes an edge unless a table in
//! this file excludes it and says why. Include edges carry content into the
//! Merkle hashes of the entity that owns them; context and spatial edges only
//! feed the topology neighbourhood.
//!
//! Invariant: an include edge never points at a signature root (product,
//! spatial element, project), so one root's content never enters another
//! root's hash. The single exception is a feature element (opening,
//! projection) included into the geometry of its host.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use crate::canon::{Entity, RefOut, UNORDERED};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Domain {
    /// Shape content: enters `vh_geometry` of the owning root.
    Geometry,
    /// Everything else: enters `vh_data` of the owning root.
    Data,
    /// Object placement chain, resolved by the spatial stage.
    Placement,
    /// The target's content is expanded into the source's own parts.
    Inlined,
}

/// How often same-label edges of one source count.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Membership {
    /// Every edge counts: a single reference, a position of a list, a member
    /// of an order-neutral list.
    Each,
    /// Edges to one target count once: relationship links and reverse
    /// attachments.
    PerTarget,
    /// Edges to targets of equal content count once: the members of a SET.
    PerContent,
}

pub struct Include {
    pub source: u32,
    pub target: u32,
    pub domain: Domain,
    pub label: Rc<str>,
    /// Position among same-label siblings, or `UNORDERED`.
    pub ord: u32,
    pub membership: Membership,
    /// The same-label siblings form a closed loop whose start is not content.
    pub cyclic: bool,
}

pub struct Graph {
    /// Include edges grouped by source; `offsets[i]..offsets[i + 1]` are the
    /// edges leaving entity `i`.
    pub includes: Vec<Include>,
    pub offsets: Vec<u32>,
    /// Undirected context/topology neighbour pairs.
    pub context: HashSet<(u32, u32)>,
    /// Undirected containment/decomposition pairs.
    pub spatial: HashSet<(u32, u32)>,
    /// Relationship instances whose class has no `RELATIONSHIPS` entry.
    pub unknown_relationships: BTreeMap<String, i64>,
    /// Relationship instances whose class is `Policy::Ignored`.
    pub ignored_relationships: BTreeMap<String, i64>,
    /// Relationships with a projection none of whose links has both ends
    /// (empty or dangling role attributes): they project nothing.
    pub unprojected_relationships: HashSet<u32>,
    /// `(entity, index into relation_labels, target)`: what each signature
    /// root names through a relationship link that produces a context or
    /// spatial edge, where the target's role attribute holds one reference.
    /// The target is a root or carries a `GlobalId`. Sorted, without repeats.
    pub relation_targets: Vec<(u32, u32, u32)>,
    /// `(relationship class, role attribute holding the target)`.
    pub relation_labels: Vec<(Rc<str>, &'static str)>,
}

impl Graph {
    /// Indices into `includes` of the edges leaving `entity`.
    pub fn child_range(&self, entity: u32) -> std::ops::Range<usize> {
        self.offsets[entity as usize] as usize..self.offsets[entity as usize + 1] as usize
    }

    pub fn children(&self, entity: u32) -> &[Include] {
        &self.includes[self.child_range(entity)]
    }

    pub fn stats(&self) -> BTreeMap<String, i64> {
        let mut out: BTreeMap<String, i64> = BTreeMap::new();
        for edge in &self.includes {
            let key = match edge.domain {
                Domain::Geometry => "include:geometry",
                Domain::Data => "include:data",
                Domain::Placement => "include:placement",
                Domain::Inlined => "include:inlined",
            };
            *out.entry(key.to_string()).or_insert(0) += 1;
        }
        if !self.context.is_empty() {
            out.insert("context:topology".to_string(), self.context.len() as i64);
        }
        if !self.spatial.is_empty() {
            out.insert("context:spatial".to_string(), self.spatial.len() as i64);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Relationship projection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LinkKind {
    /// Include/data from source to target; context/topology when the target is
    /// a signature root.
    Data,
    /// Context/topology between source and target.
    Topology,
    /// Context/spatial between a contained element and its container.
    Containment,
    /// Part to whole: context/spatial when the whole is a spatial element or
    /// the project, context/topology when it is another root, include/data
    /// otherwise.
    Decomposition,
    /// Include/geometry from a host element to its feature element, and
    /// context/topology between the two.
    Feature,
}

impl LinkKind {
    pub fn name(self) -> &'static str {
        match self {
            LinkKind::Data => "data",
            LinkKind::Topology => "topology",
            LinkKind::Containment => "containment",
            LinkKind::Decomposition => "decomposition",
            LinkKind::Feature => "feature",
        }
    }
}

pub struct Link {
    pub source_attr: &'static str,
    pub target_attr: &'static str,
    pub kind: LinkKind,
}

pub enum Policy {
    Project(&'static [Link]),
    Ignored(&'static str),
}

const fn link(source_attr: &'static str, target_attr: &'static str, kind: LinkKind) -> Link {
    Link { source_attr, target_attr, kind }
}

use LinkKind::{Containment, Data, Decomposition, Feature, Topology};

const TO_GROUP: Policy = Policy::Project(&[link("RelatedObjects", "RelatingGroup", Data)]);
const TO_ACTOR: Policy = Policy::Project(&[link("RelatedObjects", "RelatingActor", Data)]);
const TO_CONTROL: Policy = Policy::Project(&[link("RelatedObjects", "RelatingControl", Data)]);
const SPACE_BOUNDARY: Policy =
    Policy::Project(&[link("RelatingSpace", "RelatedBuildingElement", Topology)]);
const STRUCTURAL_MEMBER: Policy = Policy::Project(&[link(
    "RelatingStructuralMember",
    "RelatedStructuralConnection",
    Topology,
)]);

/// One entry per concrete `IfcRelationship` subtype of IFC2X3 and IFC4, keyed
/// by STEP keyword. A relationship that sets attributes of its own besides its
/// role attributes is additionally attached, as a data child, to the entities
/// its links leave from, and to both ends of a context/topology link.
pub const RELATIONSHIPS: &[(&str, Policy)] = &[
    ("IFCRELAGGREGATES", Policy::Project(&[link("RelatedObjects", "RelatingObject", Decomposition)])),
    ("IFCRELNESTS", Policy::Project(&[link("RelatedObjects", "RelatingObject", Data)])),
    ("IFCRELCONTAINEDINSPATIALSTRUCTURE", Policy::Project(&[link("RelatedElements", "RelatingStructure", Containment)])),
    ("IFCRELREFERENCEDINSPATIALSTRUCTURE", Policy::Project(&[link("RelatedElements", "RelatingStructure", Topology)])),
    ("IFCRELDEFINESBYPROPERTIES", Policy::Project(&[link("RelatedObjects", "RelatingPropertyDefinition", Data)])),
    ("IFCRELOVERRIDESPROPERTIES", Policy::Project(&[
        link("RelatedObjects", "RelatingPropertyDefinition", Data),
        link("RelatedObjects", "OverridingProperties", Data),
    ])),
    ("IFCRELDEFINESBYTYPE", Policy::Project(&[
        link("RelatedObjects", "RelatingType", Topology),
        link("RelatedObjects", "RelatingType", Data),
    ])),
    ("IFCRELDEFINESBYOBJECT", Policy::Project(&[link("RelatedObjects", "RelatingObject", Data)])),
    ("IFCRELDEFINESBYTEMPLATE", Policy::Project(&[link("RelatedPropertySets", "RelatingTemplate", Data)])),
    ("IFCRELDECLARES", Policy::Ignored(
        "project-library membership; a declared definition is examined through the objects that use it, an unused one through the unreached-class accounting",
    )),
    ("IFCRELASSOCIATES", Policy::Ignored("association without an associated resource")),
    ("IFCRELASSOCIATESMATERIAL", Policy::Project(&[link("RelatedObjects", "RelatingMaterial", Data)])),
    ("IFCRELASSOCIATESCLASSIFICATION", Policy::Project(&[link("RelatedObjects", "RelatingClassification", Data)])),
    ("IFCRELASSOCIATESDOCUMENT", Policy::Project(&[link("RelatedObjects", "RelatingDocument", Data)])),
    ("IFCRELASSOCIATESLIBRARY", Policy::Project(&[link("RelatedObjects", "RelatingLibrary", Data)])),
    ("IFCRELASSOCIATESCONSTRAINT", Policy::Project(&[link("RelatedObjects", "RelatingConstraint", Data)])),
    ("IFCRELASSOCIATESAPPROVAL", Policy::Project(&[link("RelatedObjects", "RelatingApproval", Data)])),
    ("IFCRELASSOCIATESAPPLIEDVALUE", Policy::Project(&[link("RelatedObjects", "RelatingAppliedValue", Data)])),
    ("IFCRELASSOCIATESPROFILEPROPERTIES", Policy::Project(&[link("RelatedObjects", "RelatingProfileProperties", Data)])),
    ("IFCRELASSIGNSTOGROUP", TO_GROUP),
    ("IFCRELASSIGNSTOGROUPBYFACTOR", TO_GROUP),
    ("IFCRELASSIGNSTOACTOR", TO_ACTOR),
    ("IFCRELOCCUPIESSPACES", TO_ACTOR),
    ("IFCRELASSIGNSTOCONTROL", TO_CONTROL),
    ("IFCRELASSIGNSTOPROJECTORDER", TO_CONTROL),
    ("IFCRELASSIGNSTASKS", TO_CONTROL),
    ("IFCRELSCHEDULESCOSTITEMS", TO_CONTROL),
    ("IFCRELASSIGNSTOPROCESS", Policy::Project(&[link("RelatedObjects", "RelatingProcess", Data)])),
    ("IFCRELASSIGNSTOPRODUCT", Policy::Project(&[link("RelatedObjects", "RelatingProduct", Data)])),
    ("IFCRELASSIGNSTORESOURCE", Policy::Project(&[link("RelatedObjects", "RelatingResource", Data)])),
    ("IFCRELSERVICESBUILDINGS", Policy::Project(&[link("RelatedBuildings", "RelatingSystem", Data)])),
    ("IFCRELVOIDSELEMENT", Policy::Project(&[link("RelatingBuildingElement", "RelatedOpeningElement", Feature)])),
    ("IFCRELPROJECTSELEMENT", Policy::Project(&[link("RelatingElement", "RelatedFeatureElement", Feature)])),
    ("IFCRELFILLSELEMENT", Policy::Project(&[link("RelatingOpeningElement", "RelatedBuildingElement", Topology)])),
    ("IFCRELCONNECTSELEMENTS", Policy::Project(&[link("RelatingElement", "RelatedElement", Topology)])),
    ("IFCRELCONNECTSPATHELEMENTS", Policy::Project(&[link("RelatingElement", "RelatedElement", Topology)])),
    ("IFCRELCONNECTSWITHREALIZINGELEMENTS", Policy::Project(&[
        link("RelatingElement", "RelatedElement", Topology),
        link("RelatingElement", "RealizingElements", Topology),
        link("RelatedElement", "RealizingElements", Topology),
    ])),
    ("IFCRELCONNECTSPORTS", Policy::Project(&[
        link("RelatingPort", "RelatedPort", Topology),
        link("RelatingPort", "RealizingElement", Topology),
        link("RelatedPort", "RealizingElement", Topology),
    ])),
    ("IFCRELCONNECTSPORTTOELEMENT", Policy::Project(&[link("RelatingPort", "RelatedElement", Topology)])),
    ("IFCRELCONNECTSSTRUCTURALACTIVITY", Policy::Project(&[link("RelatingElement", "RelatedStructuralActivity", Topology)])),
    ("IFCRELCONNECTSSTRUCTURALELEMENT", Policy::Project(&[link("RelatingElement", "RelatedStructuralMember", Topology)])),
    ("IFCRELCONNECTSSTRUCTURALMEMBER", STRUCTURAL_MEMBER),
    ("IFCRELCONNECTSWITHECCENTRICITY", STRUCTURAL_MEMBER),
    ("IFCRELCOVERSBLDGELEMENTS", Policy::Project(&[link("RelatingBuildingElement", "RelatedCoverings", Topology)])),
    ("IFCRELCOVERSSPACES", Policy::Project(&[
        link("RelatingSpace", "RelatedCoverings", Topology),
        link("RelatedSpace", "RelatedCoverings", Topology),
    ])),
    ("IFCRELSPACEBOUNDARY", SPACE_BOUNDARY),
    ("IFCRELSPACEBOUNDARY1STLEVEL", SPACE_BOUNDARY),
    ("IFCRELSPACEBOUNDARY2NDLEVEL", SPACE_BOUNDARY),
    ("IFCRELINTERFERESELEMENTS", Policy::Project(&[link("RelatingElement", "RelatedElement", Topology)])),
    ("IFCRELFLOWCONTROLELEMENTS", Policy::Project(&[link("RelatingFlowElement", "RelatedControlElements", Topology)])),
    ("IFCRELSEQUENCE", Policy::Project(&[link("RelatingProcess", "RelatedProcess", Topology)])),
    ("IFCRELINTERACTIONREQUIREMENTS", Policy::Project(&[
        link("RelatingSpaceProgram", "RelatedSpaceProgram", Topology),
        link("RelatingSpaceProgram", "LocationOfInteraction", Topology),
        link("RelatedSpaceProgram", "LocationOfInteraction", Topology),
    ])),
];

// ---------------------------------------------------------------------------
// Direct references
// ---------------------------------------------------------------------------

/// References that produce no edge: `(STEP keyword or "*", attribute, reason)`.
pub const DENIED_REFERENCES: &[(&str, &str, &str)] = &[
    (
        "*",
        "OwnerHistory",
        "provenance; IfcOwnerHistory, IfcApplication, IfcPersonAndOrganization and what hangs off them are ignored when reached only this way",
    ),
    (
        "*",
        "ContextOfItems",
        "representation contexts are project content, hashed under IfcProject",
    ),
    (
        "IFCRELSPACEBOUNDARY2NDLEVEL",
        "CorrespondingBoundary",
        "mutual back-reference between the two sides of one boundary",
    ),
];

/// References that point from content at the entity it describes. The edge is
/// reversed: the referenced owner includes the referencing entity (data
/// domain) and the forward reference produces no edge.
/// `(STEP keyword, attribute)`.
pub const REVERSE_ATTACHMENTS: &[(&str, &str)] = &[
    ("IFCMAPCONVERSION", "SourceCRS"),
    ("IFCGEOMETRICREPRESENTATIONSUBCONTEXT", "ParentContext"),
    ("IFCMATERIALPROPERTIES", "Material"),
    ("IFCEXTENDEDMATERIALPROPERTIES", "Material"),
    ("IFCFUELPROPERTIES", "Material"),
    ("IFCGENERALMATERIALPROPERTIES", "Material"),
    ("IFCHYGROSCOPICMATERIALPROPERTIES", "Material"),
    ("IFCMECHANICALMATERIALPROPERTIES", "Material"),
    ("IFCMECHANICALCONCRETEMATERIALPROPERTIES", "Material"),
    ("IFCMECHANICALSTEELMATERIALPROPERTIES", "Material"),
    ("IFCOPTICALMATERIALPROPERTIES", "Material"),
    ("IFCPRODUCTSOFCOMBUSTIONPROPERTIES", "Material"),
    ("IFCTHERMALMATERIALPROPERTIES", "Material"),
    ("IFCWATERPROPERTIES", "Material"),
    ("IFCMATERIALDEFINITIONREPRESENTATION", "RepresentedMaterial"),
    ("IFCMATERIALCLASSIFICATIONRELATIONSHIP", "ClassifiedMaterial"),
    ("IFCMATERIALRELATIONSHIP", "RelatingMaterial"),
    ("IFCPROFILEPROPERTIES", "ProfileDefinition"),
    ("IFCGENERALPROFILEPROPERTIES", "ProfileDefinition"),
    ("IFCSTRUCTURALPROFILEPROPERTIES", "ProfileDefinition"),
    ("IFCSTRUCTURALSTEELPROFILEPROPERTIES", "ProfileDefinition"),
    ("IFCRIBPLATEPROFILEPROPERTIES", "ProfileDefinition"),
    ("IFCSHAPEASPECT", "PartOfProductDefinitionShape"),
    ("IFCSTYLEDITEM", "Item"),
    ("IFCPRESENTATIONLAYERASSIGNMENT", "AssignedItems"),
    ("IFCPRESENTATIONLAYERWITHSTYLE", "AssignedItems"),
    ("IFCINDEXEDCOLOURMAP", "MappedTo"),
    ("IFCINDEXEDTRIANGLETEXTUREMAP", "MappedTo"),
    ("IFCTEXTUREMAP", "MappedTo"),
    ("IFCEXTERNALREFERENCERELATIONSHIP", "RelatedResourceObjects"),
    ("IFCRESOURCEAPPROVALRELATIONSHIP", "RelatedResourceObjects"),
    ("IFCRESOURCECONSTRAINTRELATIONSHIP", "RelatedResourceObjects"),
    ("IFCAPPROVALRELATIONSHIP", "RelatingApproval"),
    ("IFCAPPROVALACTORRELATIONSHIP", "Approval"),
    ("IFCAPPROVALPROPERTYRELATIONSHIP", "ApprovedProperties"),
    ("IFCCURRENCYRELATIONSHIP", "RelatingMonetaryUnit"),
    ("IFCDOCUMENTINFORMATIONRELATIONSHIP", "RelatingDocument"),
    ("IFCORGANIZATIONRELATIONSHIP", "RelatingOrganization"),
    ("IFCPROPERTYDEPENDENCYRELATIONSHIP", "DependantProperty"),
    ("IFCPROPERTYCONSTRAINTRELATIONSHIP", "RelatedProperties"),
    ("IFCCONSTRAINTRELATIONSHIP", "RelatingConstraint"),
    ("IFCCONSTRAINTAGGREGATIONRELATIONSHIP", "RelatingConstraint"),
    ("IFCCONSTRAINTCLASSIFICATIONRELATIONSHIP", "ClassifiedConstraint"),
    ("IFCCLASSIFICATIONITEMRELATIONSHIP", "RelatingItem"),
    ("IFCAPPLIEDVALUERELATIONSHIP", "ComponentOfTotal"),
    ("IFCREFERENCESVALUEDOCUMENT", "ReferencingValues"),
    ("IFCTIMESERIESREFERENCERELATIONSHIP", "ReferencedTimeSeries"),
    ("IFCDRAUGHTINGCALLOUTRELATIONSHIP", "RelatingDraughtingCallout"),
    ("IFCDIMENSIONCALLOUTRELATIONSHIP", "RelatingDraughtingCallout"),
];

/// LIST attributes whose order is not content: `(STEP keyword, attribute, reason)`.
pub const UNORDERED_LISTS: &[(&str, &str, &str)] = &[
    ("IFCPOLYGONALFACESET", "Faces", "the faces of a tessellation form a set"),
    (
        "IFCPRODUCTDEFINITIONSHAPE",
        "Representations",
        "a representation is identified by its context and identifier, not its position",
    ),
    (
        "IFCPRODUCTREPRESENTATION",
        "Representations",
        "a representation is identified by its context and identifier, not its position",
    ),
    (
        "IFCMATERIALDEFINITIONREPRESENTATION",
        "Representations",
        "a representation is identified by its context and identifier, not its position",
    ),
];

/// LIST attributes holding a closed loop: hashed from a canonical rotation,
/// direction preserved. `(STEP keyword, attribute)`.
pub const CYCLIC_LISTS: &[(&str, &str)] = &[("IFCPOLYLOOP", "Polygon")];

fn membership_of(r: &RefOut) -> Membership {
    if r.in_set {
        Membership::PerContent
    } else {
        Membership::Each
    }
}

fn is_denied(keyword: &str, attr: &str) -> bool {
    DENIED_REFERENCES
        .iter()
        .any(|(kw, name, _)| *name == attr && (*kw == "*" || *kw == keyword))
}

struct Builder<'a> {
    entities: &'a [Entity],
    includes: Vec<Include>,
    context: HashSet<(u32, u32)>,
    spatial: HashSet<(u32, u32)>,
}

impl Builder<'_> {
    fn include(&mut self, edge: Include) {
        assert!(
            !self.entities[edge.target as usize].is_root(),
            "include edge from #{} into the signature root #{}",
            self.entities[edge.source as usize].step_id,
            self.entities[edge.target as usize].step_id
        );
        self.includes.push(edge);
    }

    fn pair(set: &mut HashSet<(u32, u32)>, a: u32, b: u32) {
        if a != b {
            set.insert((a.min(b), a.max(b)));
        }
    }

    /// Edge for one plain attribute reference, by the classes at both ends.
    fn direct(
        &mut self,
        source: u32,
        target: u32,
        label: &Rc<str>,
        ord: u32,
        membership: Membership,
        cyclic: bool,
    ) {
        let s = &self.entities[source as usize];
        let t = &self.entities[target as usize];
        if t.is_root() {
            if !s.is_relationship {
                Self::pair(&mut self.context, source, target);
            }
            return;
        }
        let placement = (&**label == "ObjectPlacement" && s.is_root())
            || (&*s.keyword == "IFCLOCALPLACEMENT"
                && matches!(&**label, "PlacementRelTo" | "RelativePlacement"));
        let domain = if placement {
            Domain::Placement
        } else if t.is_geometric && (s.is_geometric || s.is_root()) {
            Domain::Geometry
        } else {
            Domain::Data
        };
        self.include(Include { source, target, domain, label: Rc::clone(label), ord, membership, cyclic });
    }
}

pub fn build_edges(entities: &[Entity], index: &HashMap<i64, u32>) -> Graph {
    let policies: HashMap<&str, &Policy> = RELATIONSHIPS.iter().map(|(kw, p)| (*kw, p)).collect();
    let mut reverse: HashMap<&str, Vec<(&str, Rc<str>)>> = HashMap::new();
    for (kw, attr) in REVERSE_ATTACHMENTS {
        reverse.entry(kw).or_default().push((attr, Rc::from(format!("~{kw}.{attr}"))));
    }
    let mut rel_labels: HashMap<Rc<str>, (Rc<str>, Rc<str>)> = HashMap::new();

    let mut b = Builder {
        entities,
        includes: Vec::new(),
        context: HashSet::new(),
        spatial: HashSet::new(),
    };
    let mut unknown_relationships: BTreeMap<String, i64> = BTreeMap::new();
    let mut ignored_relationships: BTreeMap<String, i64> = BTreeMap::new();
    let mut unprojected_relationships: HashSet<u32> = HashSet::new();
    let mut relation_targets: Vec<(u32, u32, u32)> = Vec::new();
    let mut relation_labels: Vec<(Rc<str>, &'static str)> = Vec::new();
    let mut relation_label_index: HashMap<(Rc<str>, &'static str), u32> = HashMap::new();

    for (i, ent) in entities.iter().enumerate() {
        let source = i as u32;
        if ent.is_relationship {
            let links = match policies.get(&*ent.keyword) {
                Some(Policy::Project(links)) => *links,
                Some(Policy::Ignored(_)) => {
                    *ignored_relationships.entry(ent.canonical_class.to_string()).or_insert(0) += 1;
                    continue;
                }
                None => {
                    *unknown_relationships.entry(ent.canonical_class.to_string()).or_insert(0) += 1;
                    continue;
                }
            };
            let (label, content_label) = rel_labels
                .entry(Rc::clone(&ent.canonical_class))
                .or_insert_with(|| {
                    (
                        Rc::clone(&ent.canonical_class),
                        Rc::from(format!("{}~", ent.canonical_class)),
                    )
                })
                .clone();
            let is_role = |attr: &str| {
                links.iter().any(|l| l.source_attr == attr || l.target_attr == attr)
            };
            let members = |attr: &str| -> Vec<u32> {
                ent.refs
                    .iter()
                    .filter(|r| &*r.attr_name == attr)
                    .filter_map(|r| index.get(&r.target).copied())
                    .collect()
            };
            let has_content = !ent.data_parts.is_empty()
                || ent.refs.iter().any(|r| {
                    !is_role(&r.attr_name)
                        && !is_denied(&ent.keyword, &r.attr_name)
                        && index.contains_key(&r.target)
                });
            let single = |attr: &str| {
                ent.refs
                    .iter()
                    .filter(|r| &*r.attr_name == attr)
                    .all(|r| r.ord == UNORDERED && !r.in_set)
            };
            let mut name_target = |entity: u32, role: &'static str, target: u32| {
                let named = &entities[target as usize];
                if entity == target
                    || !entities[entity as usize].is_root()
                    || !(named.is_root() || named.guid.is_some())
                {
                    return;
                }
                let next = relation_labels.len() as u32;
                let label = *relation_label_index
                    .entry((Rc::clone(&ent.canonical_class), role))
                    .or_insert_with(|| {
                        relation_labels.push((Rc::clone(&ent.canonical_class), role));
                        next
                    });
                relation_targets.push((entity, label, target));
            };
            let mut carriers: Vec<u32> = Vec::new();
            let mut projected = false;
            for l in links {
                let sources = members(l.source_attr);
                let targets = members(l.target_attr);
                if sources.is_empty() || targets.is_empty() {
                    continue;
                }
                projected = true;
                let mut peers = false;
                for &s in &sources {
                    for &t in &targets {
                        let whole = &entities[t as usize];
                        if !matches!(l.kind, Data | Decomposition) || whole.is_root() {
                            if single(l.target_attr) {
                                name_target(s, l.target_attr, t);
                            }
                            if single(l.source_attr) {
                                name_target(t, l.source_attr, s);
                            }
                        }
                        let data = Include {
                            source: s,
                            target: t,
                            domain: Domain::Data,
                            label: Rc::clone(&label),
                            ord: UNORDERED,
                            membership: Membership::PerTarget,
                            cyclic: false,
                        };
                        match l.kind {
                            Data if !whole.is_root() => b.include(data),
                            Decomposition if !whole.is_root() => b.include(data),
                            Decomposition if whole.is_spatial || whole.is_project => {
                                Builder::pair(&mut b.spatial, s, t)
                            }
                            Containment => Builder::pair(&mut b.spatial, s, t),
                            Feature => {
                                peers = true;
                                Builder::pair(&mut b.context, s, t);
                                b.includes.push(Include { domain: Domain::Geometry, ..data })
                            }
                            Data | Decomposition | Topology => {
                                peers = true;
                                Builder::pair(&mut b.context, s, t)
                            }
                        }
                    }
                }
                carriers.extend(&sources);
                if peers {
                    carriers.extend(&targets);
                }
            }
            if !projected {
                unprojected_relationships.insert(source);
            }
            if has_content {
                carriers.sort_unstable();
                carriers.dedup();
                for carrier in carriers {
                    b.include(Include {
                        source: carrier,
                        target: source,
                        domain: Domain::Data,
                        label: Rc::clone(&content_label),
                        ord: UNORDERED,
                        membership: Membership::PerTarget,
                        cyclic: false,
                    });
                }
                for r in &ent.refs {
                    if is_role(&r.attr_name) || is_denied(&ent.keyword, &r.attr_name) {
                        continue;
                    }
                    if let Some(&target) = index.get(&r.target) {
                        b.direct(source, target, &r.attr_name, r.ord, membership_of(r), false);
                    }
                }
            }
            continue;
        }

        let reversed = reverse.get(&*ent.keyword);
        for r in &ent.refs {
            let Some(&target) = index.get(&r.target) else { continue };
            if r.inlined {
                b.include(Include {
                    source,
                    target,
                    domain: Domain::Inlined,
                    label: Rc::clone(&r.attr_name),
                    ord: UNORDERED,
                    membership: Membership::Each,
                    cyclic: false,
                });
                continue;
            }
            if is_denied(&ent.keyword, &r.attr_name) {
                continue;
            }
            if let Some((_, label)) =
                reversed.and_then(|attrs| attrs.iter().find(|(attr, _)| *attr == &*r.attr_name))
            {
                if !ent.is_root() {
                    b.include(Include {
                        source: target,
                        target: source,
                        domain: Domain::Data,
                        label: Rc::clone(label),
                        ord: UNORDERED,
                        membership: Membership::PerTarget,
                        cyclic: false,
                    });
                }
                continue;
            }
            let unordered = UNORDERED_LISTS
                .iter()
                .any(|(kw, attr, _)| *kw == &*ent.keyword && *attr == &*r.attr_name);
            let cyclic = CYCLIC_LISTS
                .iter()
                .any(|(kw, attr)| *kw == &*ent.keyword && *attr == &*r.attr_name);
            let ord = if unordered { UNORDERED } else { r.ord };
            b.direct(source, target, &r.attr_name, ord, membership_of(r), cyclic);
        }
    }

    let mut includes = b.includes;
    includes.sort_by_key(|e| e.source);
    let mut members: HashSet<(u32, u8, Rc<str>)> = HashSet::new();
    let mut group = u32::MAX;
    includes.retain(|e| {
        if e.source != group {
            group = e.source;
            members.clear();
        }
        e.membership != Membership::PerTarget
            || members.insert((e.target, e.domain as u8, Rc::clone(&e.label)))
    });
    let mut offsets: Vec<u32> = vec![0; entities.len() + 1];
    for edge in &includes {
        offsets[edge.source as usize + 1] += 1;
    }
    for i in 0..entities.len() {
        offsets[i + 1] += offsets[i];
    }

    relation_targets.sort_unstable();
    relation_targets.dedup();

    Graph {
        includes,
        offsets,
        context: b.context,
        spatial: b.spatial,
        unknown_relationships,
        ignored_relationships,
        unprojected_relationships,
        relation_targets,
        relation_labels,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[derive(Default)]
    pub(crate) struct Flags {
        pub product: bool,
        pub spatial: bool,
        pub project: bool,
        pub relationship: bool,
        pub geometric: bool,
    }

    pub(crate) fn ent(step_id: i64, keyword: &str, flags: Flags, refs: &[(&str, i64)]) -> Entity {
        Entity {
            step_id,
            keyword: Rc::from(keyword),
            canonical_class: Rc::from(keyword),
            is_product: flags.product,
            is_spatial: flags.spatial,
            is_project: flags.project,
            is_relationship: flags.relationship,
            is_geometric: flags.geometric,
            guid: None,
            name: None,
            geom_parts: Vec::new(),
            data_parts: Vec::new(),
            refs: refs
                .iter()
                .map(|(a, t)| RefOut {
                    attr_name: Rc::from(*a),
                    target: *t,
                    ord: UNORDERED,
                    in_set: false,
                    inlined: false,
                })
                .collect(),
            data_facts: Vec::new(),
            geom_facts: Vec::new(),
        }
    }

    fn product() -> Flags {
        Flags { product: true, ..Flags::default() }
    }
    fn rel() -> Flags {
        Flags { relationship: true, ..Flags::default() }
    }
    fn plain() -> Flags {
        Flags::default()
    }

    pub(crate) fn graph(entities: &[Entity]) -> Graph {
        let index: HashMap<i64, u32> =
            entities.iter().enumerate().map(|(i, e)| (e.step_id, i as u32)).collect();
        build_edges(entities, &index)
    }

    fn includes(g: &Graph) -> Vec<(u32, u32, Domain)> {
        g.includes.iter().map(|e| (e.source, e.target, e.domain)).collect()
    }

    #[test]
    fn type_relation_is_context_and_data() {
        let entities = vec![
            ent(1, "IFCWALL", product(), &[]),
            ent(2, "IFCWALLTYPE", plain(), &[]),
            ent(3, "IFCRELDEFINESBYTYPE", rel(), &[("RelatedObjects", 1), ("RelatingType", 2)]),
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(0, 1, Domain::Data)]);
        assert!(g.context.contains(&(0, 1)));
    }

    #[test]
    fn type_representation_map_is_a_data_edge() {
        let entities = vec![
            ent(1, "IFCWALLTYPE", plain(), &[("RepresentationMaps", 2)]),
            ent(2, "IFCREPRESENTATIONMAP", Flags { geometric: true, ..Flags::default() }, &[]),
        ];
        assert_eq!(includes(&graph(&entities)), vec![(0, 1, Domain::Data)]);
    }

    #[test]
    fn unclassified_reference_is_included_in_the_source_domain() {
        let entities = vec![
            ent(1, "IFCMATERIALLAYER", plain(), &[("Material", 2)]),
            ent(2, "IFCMATERIAL", plain(), &[]),
            ent(3, "IFCEXTRUDEDAREASOLID", Flags { geometric: true, ..Flags::default() }, &[("SweptArea", 4)]),
            ent(4, "IFCRECTANGLEPROFILEDEF", Flags { geometric: true, ..Flags::default() }, &[]),
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(0, 1, Domain::Data), (2, 3, Domain::Geometry)]);
    }

    #[test]
    fn denied_and_dangling_references_produce_no_edge() {
        let entities = vec![
            ent(1, "IFCPROPERTYSET", plain(), &[("OwnerHistory", 2), ("HasProperties", 999)]),
            ent(2, "IFCOWNERHISTORY", plain(), &[]),
        ];
        assert!(graph(&entities).includes.is_empty());
    }

    #[test]
    fn reverse_attachment_hangs_content_under_its_owner() {
        let entities = vec![
            ent(1, "IFCGEOMETRICREPRESENTATIONCONTEXT", plain(), &[]),
            ent(2, "IFCMAPCONVERSION", plain(), &[("SourceCRS", 1), ("TargetCRS", 3)]),
            ent(3, "IFCPROJECTEDCRS", plain(), &[]),
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(0, 1, Domain::Data), (1, 2, Domain::Data)]);
    }

    #[test]
    fn group_assignment_points_from_member_to_group() {
        let entities = vec![
            ent(1, "IFCWALL", product(), &[]),
            ent(2, "IFCGROUP", plain(), &[]),
            ent(3, "IFCRELASSIGNSTOGROUP", rel(), &[("RelatedObjects", 1), ("RelatingGroup", 2)]),
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(0, 1, Domain::Data)]);
        assert!(g.context.is_empty());
    }

    #[test]
    fn relationship_with_own_content_is_attached_to_both_peers() {
        let mut boundary = ent(
            3,
            "IFCRELSPACEBOUNDARY",
            rel(),
            &[("RelatingSpace", 1), ("RelatedBuildingElement", 2)],
        );
        boundary.data_parts.push("attr=InternalOrExternalBoundary:eINTERNAL".to_string());
        let entities = vec![
            ent(1, "IFCSPACE", Flags { product: true, spatial: true, ..Flags::default() }, &[]),
            ent(2, "IFCWALL", product(), &[]),
            boundary,
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(0, 2, Domain::Data), (1, 2, Domain::Data)]);
        assert!(g.context.contains(&(0, 1)));
    }

    #[test]
    fn target_attached_or_linked_twice_is_included_once() {
        let entities = vec![
            ent(1, "IFCPRESENTATIONLAYERASSIGNMENT", plain(), &[("AssignedItems", 2), ("AssignedItems", 2)]),
            ent(2, "IFCSHAPEREPRESENTATION", Flags { geometric: true, ..Flags::default() }, &[]),
            ent(3, "IFCWALL", product(), &[]),
            ent(4, "IFCPROPERTYSET", plain(), &[]),
            ent(5, "IFCRELDEFINESBYPROPERTIES", rel(), &[("RelatedObjects", 3), ("RelatingPropertyDefinition", 4)]),
            ent(6, "IFCRELDEFINESBYPROPERTIES", rel(), &[("RelatedObjects", 3), ("RelatingPropertyDefinition", 4)]),
        ];
        let g = graph(&entities);
        assert_eq!(includes(&g), vec![(1, 0, Domain::Data), (2, 3, Domain::Data)]);
    }

    #[test]
    fn set_members_and_list_positions_keep_one_edge_per_reference() {
        let mut set = ent(1, "IFCPROPERTYSET", plain(), &[("HasProperties", 3), ("HasProperties", 3)]);
        for r in &mut set.refs {
            r.in_set = true;
        }
        let entities = vec![
            set,
            ent(2, "IFCPOLYGONALFACESET", Flags { geometric: true, ..Flags::default() }, &[("Faces", 4), ("Faces", 4)]),
            ent(3, "IFCPROPERTYSINGLEVALUE", plain(), &[]),
            ent(4, "IFCINDEXEDPOLYGONALFACE", Flags { geometric: true, ..Flags::default() }, &[]),
        ];
        let g = graph(&entities);
        let memberships: Vec<Membership> = g.includes.iter().map(|e| e.membership).collect();
        assert_eq!(
            memberships,
            vec![Membership::PerContent, Membership::PerContent, Membership::Each, Membership::Each]
        );
    }

    #[test]
    fn unknown_relationship_class_is_counted() {
        let entities = vec![ent(1, "IFCRELINVENTED", rel(), &[("RelatedObjects", 2)]), ent(2, "IFCWALL", product(), &[])];
        let g = graph(&entities);
        assert!(g.includes.is_empty());
        assert_eq!(g.unknown_relationships.get("IFCRELINVENTED"), Some(&1));
    }

    #[test]
    fn relationship_without_both_ends_of_any_link_projects_nothing() {
        let entities = vec![
            ent(1, "IFCWALL", product(), &[]),
            ent(2, "IFCMATERIAL", plain(), &[]),
            ent(3, "IFCRELASSOCIATESMATERIAL", rel(), &[("RelatedObjects", 1), ("RelatingMaterial", 2)]),
            ent(4, "IFCRELASSOCIATESMATERIAL", rel(), &[("RelatedObjects", 999), ("RelatingMaterial", 2)]),
            ent(5, "IFCRELASSOCIATESMATERIAL", rel(), &[("RelatedObjects", 1)]),
        ];
        let g = graph(&entities);
        assert_eq!(g.unprojected_relationships, HashSet::from([3, 4]));
    }

    #[test]
    fn no_include_edge_points_at_a_signature_root() {
        // Every role attribute of every rule, and one plain reference, all
        // pointing at products.
        for (keyword, policy) in RELATIONSHIPS {
            let Policy::Project(links) = policy else { continue };
            let mut refs: Vec<(&str, i64)> = Vec::new();
            for l in *links {
                refs.push((l.source_attr, 1));
                refs.push((l.target_attr, 2));
            }
            let mut relationship = ent(3, keyword, rel(), &refs);
            relationship.data_parts.push("attr=Name:s'x".to_string());
            let entities = vec![
                ent(1, "IFCWALL", product(), &[("Anything", 2)]),
                ent(2, "IFCSLAB", product(), &[]),
                relationship,
            ];
            let g = graph(&entities);
            let feature = links.iter().any(|l| l.kind == Feature);
            for edge in &g.includes {
                let into_root = entities[edge.target as usize].is_root();
                assert!(
                    !into_root || (feature && edge.domain == Domain::Geometry),
                    "{keyword}: include edge into a root"
                );
            }
        }
    }

    #[test]
    fn relationship_table_has_one_entry_per_class() {
        let mut keys: Vec<&str> = RELATIONSHIPS.iter().map(|(kw, _)| *kw).collect();
        let total = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), total);
    }
}
