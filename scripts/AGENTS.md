# Script Agent Notes

Scripts are for inspection and repeatable investigation.

- Use `ifcopenshell` for IFC access. Do not parse STEP text directly.
- Keep reusable inspectors at `scripts/`.
- Put exploratory or investigative scripts in `scripts/explore/`.
- Do not add one-off throwaway scripts at repo root.
- Existing inspectors:
  - `inspect_ifc.py` prints IFC summary stats.
  - `inspect_ifc_identity.py` shows project name, `GlobalId`, and header
    timestamp.
  - `inspect_guid_overlap.py` shows entity GUID overlap between files.
  - `explore/corpus_survey.py` regenerates the corpus survey JSON behind
    `docs/corpus/`.
