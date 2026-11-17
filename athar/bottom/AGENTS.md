# Signature Pipeline Agent Notes

`athar/bottom/` builds a `SignatureBundle` for one IFC file. It is
self-contained and should not depend on matcher, delta, Git, or presentation
code.

## Pipeline (native-only)

The bottom layer is implemented in Rust (`athar/_native`, exposed as
`athar_native`). It is required — there is no Python fallback. The Python
surface is thin glue:

- `_native.py` — discovers the compiled module (`native()`).
- `parser.py` — file-independent schema/unit glue only: schema-support guard,
  measure-type helpers, `IfcUnitAssignment` factor extraction through `ifcopenshell.util.unit.get_unit_scale` (IfcOpenShell 0.9.0.post1+). `unit_factor` also converts a property's own unit. SI, conversion-based chains and derived units have multiplicative factors; context-dependent units, chains or derived units containing them, and nonzero-offset conversions are omitted from both factor mappings. The first unit assignment supplies the unit types. Factors are to SI with mass in kilograms (IfcOpenShell scales mass relative to the gram, so `unit_factor` divides by 1000 per gram exponent). Every measure type whose unit type (`ifcopenshell.util.unit.get_measure_unit_type`) is an `IfcUnitEnum` or `IfcDerivedUnitEnum` value other than `USERDEFINED` scales natively by the file factor of that unit type (`Measure::Unit` in `descriptor.rs`); an absent factor leaves the number unscaled in `canon.rs`, without an SI property label. An integer of such a measure (`IfcIntegerCountRateMeasure`, or an integer literal where a real measure is expected) hashes and prints as the scaled quantity, exactly as the equal real; integers without a unit type (`IfcInteger`, `IfcCountMeasure`) keep their integer form. A property value, quantity or `IfcMeasureWithUnit.ValueComponent` that names its own unit hashes in that unit (a `record_unit_factors` override, factor 1 for a unit without a multiplicative factor), as property reporting converts it. Overrides are per attribute (step id -> attribute -> unit type -> factor), so an `IfcPropertyTableValue`'s `DefiningValues` take `DefiningUnit` and its `DefinedValues` take `DefinedUnit`, even for one measure type in two units. An `IfcPropertyEnumeration` names its unit for its values, and an `IfcPropertyEnumeratedValue` takes the unit of the enumeration its `EnumerationReference` names. `open_ifc` is
  the one place `ifcopenshell` opens a file. ifcopenshell crashes the
  interpreter on a file it cannot open, so `require_readable_file` raises an
  `OSError` naming the path unless it is a regular file the process can read.
  `open_ifc` calls it, and the Git cache calls it before hashing a file.
- `native_schema.py` — builds the per-class descriptor map and the unit type
  of every measure type (JSON, cached per schema) that Rust applies to the
  STEP records.
- `signatures.py` — opens the file once via `ifcopenshell` for the schema name
  and unit factors, releases it, runs the native pipeline, and wraps the result
  in `SignatureVector` / `SignatureBundle`.
  `IfcMapConversion` lengths are in the target `IfcProjectedCRS.MapUnit`, so
  `record_unit_factors` overrides the length factor of its `Eastings`,
  `Northings` and `OrthogonalHeight`;
  without a `MapUnit` the project length unit applies.

The Rust side (`athar/_native/src/`): `step.rs` tokenize → `canon.rs`
canonicalize (NFC, round-half-even quantization to `i128`, `data_facts`) → `edges.rs`
edge classification → Merkle (geometry/data), coverage accounting and fact
collection → WL topology → `spatial.rs` placements. `make
native-test` runs the crate's unit tests; CI runs it before the default suite.

`step.rs` reads every byte from the `DATA` statement (found after the header's
`ENDSEC;`, outside strings and comments) to its `ENDSEC;`. Anything that is
not a record or trivia, a duplicate `#id`, and complex (multiple-inheritance)
instances are errors carrying the byte offset and the record id. Keywords and
enumeration values are case-insensitive and normalized to upper case. String
bytes decode as UTF-8 where valid and as Latin-1 per byte otherwise, then STEP
control directives are decoded, so raw and escaped spellings hash alike
(canon-v10). ifcopenshell drops raw non-ASCII string bytes, so values read
through it (property extraction) can differ from the hashed ones.

`athar_native.parse_step_file(path)` is the tokenizer inspection entry point, outside the signature pipeline. It returns `(step_id, class, attributes)` records with tagged values: `integer`, `real`, `string`, `enum`, `reference`, `null`, `derived`, `list`, and `typed` (type name plus arguments). Lists and typed arguments retain nesting; `$` and `*` remain distinct. `tests/test_ifcopenshell_crosscheck.py` compares these records with IfcOpenShell, with exact expected differences for malformed corpus records. `athar_native.reference_graph(path, schema_json)` returns the canonicalized references `(source, attribute, target)` and the edges built from them (include edges `(source, target, label, domain)`, context and spatial pairs); the crosscheck compares every entity's references, inverse counts and reverse-attached children with IfcOpenShell (`traverse` at depth 1, `get_inverse`), and each reference's edge with the denied, reverse-attachment and relationship policy tables.

Signature roots are products, spatial elements (IFC4 `IfcSpatialElement`,
IFC2X3 `IfcSpatialStructureElement`) and `IfcProject` (descriptor flags
`product`, `spatial`, `project`). `schema_dict.py` collapses every
`*StandardCase` / `*ElementedCase` class to its supertype, derived from the
schema, so the matcher class of a case subtype is its supertype's.

## Coverage Is The Default

Every attribute is hashed and every reference becomes an edge unless a table
says otherwise and gives the reason. The tables live in `canon.rs` and
`edges.rs` and are exported for tests through `athar_native.relationship_policy()`
and `athar_native.reference_policy()`:

- `UNHASHED_ATTRIBUTES`: `GlobalId`, `OwnerHistory`, and the representation
  context `Precision`.
- `DENIED_REFERENCES`: `OwnerHistory` (provenance), `ContextOfItems`
  (contexts are project content), and the mutual `CorrespondingBoundary`
  back-reference of second-level space boundaries.
- `REVERSE_ATTACHMENTS`: content that points at the entity it describes
  (`IfcMapConversion.SourceCRS`, `IfcGeometricRepresentationSubContext.ParentContext`,
  material and profile properties, `IfcMaterialDefinitionRepresentation`,
  `IfcStyledItem.Item`, `IfcPresentationLayerAssignment.AssignedItems`,
  `IfcShapeAspect`, indexed colour/texture maps, and the resource-level
  relationships). The owner includes the pointing entity as a data child and
  the forward reference produces no edge.
- `RELATIONSHIPS`: one entry per concrete `IfcRelationship` subtype of both
  schemas, either a projection or `Ignored` with a reason.
  `tests/test_coverage_policy.py` enumerates both schemas against it.

## Canonical Frames And Directions

- `IfcDirection` ratios are hashed at unit length: a direction has no
  magnitude. Any finite, nonzero ratios normalize, however small. `IfcVector`
  keeps its `Magnitude` attribute.
- `IfcAxis1Placement`, `IfcAxis2Placement2D`, `IfcAxis2Placement3D` and the
  `IfcCartesianTransformationOperator*` classes hash one `frame=` part: the
  axes the schema derives (`IfcBuildAxes`, `IfcBuild2Axes`, `IfcBaseAxis`) and,
  for operators, the derived scales, quantized after resolution. An omitted
  axis or scale equals its default written out, and a `RefDirection` off the
  placement plane (of a 2D placement too) equals its projection. The axis
  references produce no edge. A frame the rules leave undefined (axes parallel
  within `1e-12` after normalization, an axis reference that is not a
  direction) keeps the attribute-wise encoding. `IfcFirstProjAxis` takes
  `(0,1,0)` as the default X for a Z axis within `1e-9` of either X direction;
  the schema leaves -X undefined.
- Spatial coordinates are converted to metres without quantization before placement-chain composition. World and local matrices are quantized at `PLACEMENT_SCALE = 1e6` (one micrometre for translations, `1e-6` for rotation entries). IfcOpenShell's `get_local_placement` returns file units; crosschecks scale its translation to metres and allow one quantum plus `1e-9` relative. IfcOpenShell's helper retains a non-perpendicular RefDirection; the engine uses the IFC-derived projection. IfcOpenShell's geometry kernel (`create_shape` on a placement with `convert-back-units`, then scaled the same way) projects like the engine; its divergences on degenerate frames (OCC's default X for an `Axis` without `RefDirection`, a kept Z axis for an undefined frame) are listed in `tests/test_ifcopenshell_crosscheck.py`.
- `spatial.rs` builds placement matrices from the same axes (`placement_axes`
  in `canon.rs`). A relative placement whose frame is undefined, including an
  axis attribute set to a zero, malformed or missing direction, has a zero
  rotation part: an unusable axis never reads as the default one.

## Edge Model

Include edges carry content into Merkle hashes; context and spatial edges only
feed the topology neighbourhood.

- A direct reference to a signature root is a context/topology edge. No
  include edge points at a root, so one root's content never enters another
  root's hash. The one exception is a feature element (`IfcRelVoidsElement`,
  `IfcRelProjectsElement`), included into the geometry of its host; host and
  feature are also context/topology neighbours.
- Where a feature sits is part of its host's geometry. `vh_geometry` of a
  host covers, per feature, the feature's shape digest together with the
  feature's placement relative to the host (the host's world frame inverted,
  times the feature's world frame, quantized like a placement). Moving an
  opening within its wall is a geometry change of the wall; moving both
  together is none. When either chain is unresolved the two unresolved
  digests stand in for the relative frame, so any change to either chain is
  a geometry change of the host. The position of a feature nested in a
  feature reaches the feature it sits in, not the outer host.
- A root's `ObjectPlacement`, and a local placement's `PlacementRelTo` /
  `RelativePlacement`, are placement edges resolved by `spatial.rs`; placement
  never enters `vh_geometry` or `vh_data`. A chain through a placement class
  other than `IfcLocalPlacement` (`IfcGridPlacement`) yields no matrix:
  `placement_unresolved` is a digest of that placement's content, the
  placements of the `IfcGrid`s owning the axes it reaches, and the local chain
  below it. Grid geometry is not evaluated. Local placements on a
  `PlacementRelTo` cycle are unresolved too, each standing on a digest of the
  relative placements met from it around the cycle, so the result does not
  depend on record order. An `ObjectPlacement` or
  `PlacementRelTo` that names a signature root fails the build with an error
  naming both records.
- Beside the world matrix a root carries `local_placement`, its frame
  relative to the nearest placement above its own that is the
  `ObjectPlacement` of a root, quantized like the world matrix, and
  `placement_parent`, that root. Local placements in between that place no
  root are multiplied into the frame. Where several roots share one placement
  record the parent is chosen by content (with a GlobalId, spatial element
  first, smallest GlobalId). Neither enters the matcher's vector.
- Every attribute of a root belongs to one domain by its schema type:
  `ObjectPlacement` is placement, a shape-model reference is geometry,
  everything else is data.
- A reference to a shape-model entity (descriptor flag `geometric`: subtypes
  of `IfcProductRepresentation`, `IfcRepresentation`, `IfcRepresentationMap`,
  `IfcRepresentationItem`, `IfcProfileDef`) from a shape-model entity or a
  root is an include/geometry edge. Every other reference is include/data.
- An occurrence includes its type object over a data edge, and a type object
  reaches its `RepresentationMaps` over data edges. A type's maps therefore
  enter `vh_data` of everything that includes the type (occurrences, ports
  nested under it) with their shape content, listed as a `(shape)` fact, and
  enter `vh_geometry` only of an occurrence that maps them through an
  `IfcMappedItem`. Editing a mapped map reports both `geometry` and `data` on the occurrence, with the map's `(shape)` fact in `data_delta`; editing an unmapped map reports `data` only.
- `vh_data` and `vh_geometry` are pure content hashes, independent of instance sharing. A shape entity reached over a data edge contributes its full content to `vh_data`, even when the geometry reaches the same instance. Editing a swept profile shared with a material profile therefore reports `geometry` and `data`; an untouched equal copy in the material profile leaves `data` unchanged. Merging or splitting equal copies without changing their content changes neither hash.
- Shape-model entities hash their non-text attributes into the geometry
  digest and their text attributes (`RepresentationIdentifier`,
  `RepresentationType`, `ProfileName`, `Name`) into the data digest.
- `vh_geometry` of a root covers its geometry-edge closure and not the root's
  class, so two roots of different classes with one shape have one geometry
  hash. `vh_data` covers
  its data parts, the full content (geometry and data digest) of everything
  reached over a data edge, and the data digests found along its geometry
  edges, so shape naming and presentation styles are a data change of the
  owning element and never a geometry change. A material profile's profile
  definition or a context's `WorldCoordinateSystem` therefore reach `vh_data`
  with their shape content.
- References inside a LIST or ARRAY carry their position in the edge label.
  Whether two positions hold one instance or two equal instances does not
  matter. `CYCLIC_LISTS` (`IfcPolyLoop.Polygon`) are hashed from the canonical
  rotation with the direction kept; `UNORDERED_LISTS`
  (`IfcPolygonalFaceSet.Faces`, the `Representations` of a product or material
  definition shape) ignore position and keep every member, so a repeated
  member is content.
- The members of a SET attribute are a set of contents: members whose label
  and digest are equal are one member where the owner's digest is computed
  (`entity_digest`), so naming an instance twice, or merging or duplicating
  value-identical instances inside a SET, changes nothing. `data_facts` list
  such members once.
- Merging equal instances is only invisible where nothing refers to one of
  them by identity. A reverse attachment (`IfcStyledItem.Item`, a
  presentation layer's `AssignedItems`) belongs to the instance it points at,
  so merging a styled item with an unstyled equal makes the unstyled owner
  styled, which is a data change.
- A relationship link or a reverse attachment reaching one target twice is
  one membership; two value-identical targets (two equal property sets on one
  element) stay two.
- A single reference or a flat aggregate of references has no part of its own;
  unset attributes have no part. Dangling references are dropped and counted
  in the parse diagnostics.
- Entities on an include cycle are hashed per strongly connected component:
  each member starts from a seed naming the domain and its class and all
  members are refined together for twice the component size, so a digest
  covers the whole component and depends neither on STEP ids nor on record
  order. `cycle_breaks` in the parse diagnostics counts cyclic components
  (geometry and data domains separately).

Relationship projection kinds:

| Kind | Edge |
|---|---|
| `data` | include/data from each source to the target; context/topology when the target is a root |
| `topology` | context/topology between source and target |
| `containment` | context/spatial between element and container |
| `decomposition` | context/spatial to a spatial or project whole, context/topology to another root, include/data otherwise |
| `feature` | include/geometry from host to feature element with its position relative to the host, context/topology between them |

`IfcRelDefinesByType` is both `topology` and `data`: type objects carry no
signature, so type content is attributed to each occurrence's `vh_data`.
Assignments (`IfcRelAssignsTo*`) point from member to group, never from group
to members, so a group edit reaches its members and a member edit stays local.

Every link that produces a context or spatial edge also yields relation
targets (`Graph.relation_targets`): a root at one end names the entity at the
other end when that end's role attribute holds a single reference, so the
element names its container, the part its whole, the occurrence its type, and
both ends of a 1:1 relationship (void, fill, space boundary, connection) name
each other. The many side (a storey's elements) names nothing; it sees the
change through `vh_topology`. Targets are roots or entities with a
`GlobalId`; those without a signature are listed in
`SignatureBundle.related_entities` with a content digest. Relation targets
enter no hash.

A relationship that sets attributes of its own besides its role attributes
(a space boundary's `InternalOrExternalBoundary`, a path connection's
connection types, a `ConnectionGeometry`, a `Name`) is attached as a data
child to the entities its links leave from and to both ends of a
context/topology link, so its content reaches their `vh_data`.

## Data Facts

`FactCollector` (`lib.rs`) lists, per root, a readable fact for everything
`vh_data` covers: the data attributes of the root and of every entity reached
over data edges, and the text attributes and data attachments found along
geometry edges (shape subtrees without data content are skipped). A shape
entity a data edge points at also lists its own geometry attributes
(scalars readable, aggregates and frames in canonical encoding, a digest when
long, a triangle count and digest) and one `(shape)` digest standing for the
geometry below it. Fact paths carry the positions of ordered lists passed over
data edges; the report layer turns differing facts into `data_delta`.

The root is labelled `Class[GlobalId=...]`. An owner (an entity with a
GlobalId reached below the root: a property or quantity set, a type object)
prefixes everything below it, and its key is its class and `Name`:
`IfcWallType[Name=WT]/IfcPropertySet[Name=Pset_WallCommon]/`. `GlobalId` enters
no hash, so it is not part of the key and a regenerated owner GlobalId leaves
every path as it was. When two owners of one root would get the same label at
the same place in the path (two sets of one name), both carry
`[Name=...,GlobalId=...]`, so their definitions stay apart; a regenerated
GlobalId on such an owner lists its facts as removed and added. An owner
without a `Name` is the bare class, or `Class[GlobalId=...]` on collision. A
fact therefore shows whether it comes from the occurrence or from its type. A real is
listed as the hashed quantity in SI units with its unit (`3.25 m`), not as
the quantized integer. Every
fact value is bounded: verbatim up to 96 bytes. Beyond that a single string
(bare or type-tagged, such as a `Description` or an `IfcText` property value)
keeps the leading characters that fit, followed by `… (N bytes,
sha256:<digest>)` over the whole text, so two long texts that differ late
still differ; every other value (an aggregate, canonical shape content) is
its byte length and a digest. A large aggregate reached as data (a colour
list, a table, a pixel texture) therefore costs a fixed size per root that
reaches it.

## Coverage Accounting

Per file the bundle records `Coverage`: relationship instances whose class has
no table entry, by class; for every class with instances that no root reaches
over include edges, the instance count and an order-independent digest of
their content (a relationship whose role attributes are empty or dangling
projects nothing and is counted here); and counts of what is ignored by
design (provenance reached only through `OwnerHistory`, ignored relationship
classes).

## Tessellated-Mesh Canonicalization

`IfcTriangulatedFaceSet` is hashed as order-canonical expanded triangles:
each triangle becomes its three quantized vertex triples (Coordinates and
PnIndex resolved away, no edge to the point list), cyclically rotated to the
lexicographically-smallest form and emitted one part per triangle so the
sorted parts make the triangle list a set. Vertex order and triangle order
are serialization noise (Revit permutes both between otherwise-identical
exports — 2026-07 round-trip corpus), but a reversed winding is a real
orientation change and must stay visible. Facesets carrying `Normals` fall
back to the raw order-sensitive encoding. Point lists consumed by
index-order-sensitive entities (e.g. `IfcIndexedPolyCurve`) are untouched.
Pinned by `tests/test_mesh_canonicalization.py`.

## WL Topology Contract

`vh_topology` answers "did the class-multiset of my direct relationship
neighborhood change": WL seeds are canonical-class-only and both gossip radii
are 1 (direct context neighbors, direct spatial parents/children). Neighbor
*content* (geometry/data) must never enter the seeds — a neighbor's content
change is already reported on that neighbor as intrinsic, and content-bearing
seeds fan a single change out storey-wide (2026-07 Revit round-trip corpus:
2 re-tessellated elements false-flagged 175 storey siblings per export pair).
Consequences to preserve: a pure content edit keeps the victim's own topology
aspect `unchanged`; add/delete flips only direct containers and directly
connected elements, not siblings.

## Known Limitation: Representation Equivalence

`vh_geometry` hashes the geometry-domain subgraph (representation items,
profiles, points), not a resolved solid. The same shape stored two ways — e.g.
an `IfcExtrudedAreaSolid` in one file and an `IfcTriangulatedFaceSet` /
`IfcFacetedBrep` of the identical solid in another — produces different
subgraphs and therefore a different geometry hash, so the engine reports it as
geometry-changed even though the realized BRep is the same.

This is intentional, not a bug: the engine does no deep B-rep comparison (a
core contract), so it cannot resolve representations to a canonical solid.
Identity still holds — a GUID-matched product survives the swap as `modified`
with the geometry aspect flipped, never as added+deleted. The gap is pinned by
`tests/test_geometry_representation_gap.py` (a strict `xfail`); closing it would
require canonical solid resolution through a geometry kernel.

## Canon Versions

`CANON_VERSION` (`constants.py`) names the canonical form; hashes are only
comparable within one version.

| Version | Canonical form |
|---|---|
| v2 | property/quantity subtrees in the data Merkle domain |
| v3 | type data included in occurrence `vh_data` |
| v4 | native-only bottom pipeline |
| v5 | WL topology seeds are class-only, gossip radius 1 |
| v6 | triangulated meshes hash as order-canonical expanded triangles; `IfcPolygonalFaceSet.Faces` in the geometry Merkle |
| v7 | STEP string control directives decoded before hashing |
| v8 | unset and derived values hash alike at DERIVE-redeclared attributes |
| v9 | scalar attributes of shape-model entities and axis placements reached from geometry in `vh_geometry` |
| v10 | string bytes decode as UTF-8 where valid, Latin-1 per byte otherwise; keywords and enumeration values normalized to upper case |
| v11 | coverage by default: schema-complete relationship table, direct references included unless denied, `IfcProject` as a signature root, ordered aggregates keep position, sharing invisible, SET members counted once per content; placements and operators as resolved frames, directions at unit length; type representation maps as data of the occurrence; include cycles hashed per component; grid placements and placement cycles as an unresolved placement digest; feature positions relative to the host in host geometry |
| v12 | signatures carry the local placement and the placement parent beside the world placement, and the relation targets of each root |
| v13 | `vh_geometry` of a root does not cover the root's class |
| v14 | hashes as in v13; `data_facts` paths label an owner below the root by class and `Name`; related entities say whether their GlobalId is shared; property values without a declared or convertible unit stay as written |
| v15 | spatial coordinates remain unquantized until frame composition; world/local matrices and relative feature-position hashes use the composed frame |
| v16 | `IfcMapConversion` lengths scale by the target CRS `MapUnit` when it is set |
| v17 | directions normalize at any nonzero magnitude; a 2D placement's `RefDirection` is projected into its plane; every measure with a unit type scales to SI by the file's factor (mass in kilograms), integers of such a measure included, and a property, enumeration, quantity or `IfcMeasureWithUnit` unit replaces the file's for that record (an enumerated value takes its enumeration's unit, a table's defining and defined values each their own); quantized reals are 128-bit; signatures carry no centroid or AABB |
