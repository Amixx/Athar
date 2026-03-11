# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## Installation

```bash
pip install -r requirements.txt
```

Requires Python 3.10+ and [ifcopenshell](https://ifcopenshell.org/).

## Usage

### Full Tool (Summary, Folder mode, Reports)

```bash
# Two-file diff summary
python -m athar_layers old.ifc new.ifc

# Verbose summary (per-entity details)
python -m athar_layers old.ifc new.ifc --summary --verbose

# Folder mode — auto-groups versions and diffs them
python -m athar_layers some-folder/

# Export a Markdown report
python -m athar_layers old.ifc new.ifc --report diff-report.md
python -m athar_layers some-folder/ --report version-history.md
```

### Core Engine (Raw JSON)

```bash
# Two-file diff (JSON output for computers)
python -m athar old.ifc new.ifc
```

See the [sample report](docs/SAMPLE_REPORT.md) for what the output looks like.

## Testing

```bash
python -m pytest tests/
```

## Documentation

See [docs/DETAILS.md](docs/DETAILS.md) for detailed documentation on comparison logic, folder mode, file metadata, helper scripts, and test data.

## License

MIT
