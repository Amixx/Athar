# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## Installation

```bash
pip install -r requirements.txt
```

Requires Python 3.10+ and [ifcopenshell](https://ifcopenshell.org/).

## Usage

```bash
# Two-file diff (JSON output)
python -m athar old.ifc new.ifc

# Human-readable summary
python -m athar old.ifc new.ifc --summary

# Verbose summary (per-entity details)
python -m athar old.ifc new.ifc --summary --verbose

# Write JSON output to file
python -m athar old.ifc new.ifc -o diff.json

# Folder mode — auto-groups versions and diffs them (summary by default)
python -m athar some-folder/

# Export a Markdown report (great for sharing with architects/BIM managers)
python -m athar old.ifc new.ifc --report diff-report.md
python -m athar some-folder/ --report version-history.md

# Verbose report with per-entity details
python -m athar some-folder/ --report report.md --verbose
```

See a [sample report](docs/sample-report.md) for what the output looks like.

## Testing

```bash
python -m pytest tests/
```

## Documentation

See [docs/DETAILS.md](docs/DETAILS.md) for detailed documentation on comparison logic, folder mode, file metadata, helper scripts, and test data.

## License

MIT
