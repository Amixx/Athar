import pytest

import athar.diff_engine as diff_engine


def test_diff_files_rejects_cross_schema_pairs(monkeypatch):
    graphs = iter([
        {"metadata": {"schema": "IFC4"}, "entities": {}},
        {"metadata": {"schema": "IFC2X3"}, "entities": {}},
    ])
    monkeypatch.setattr(diff_engine, "parse_graph", lambda *_args, **_kwargs: next(graphs))

    with pytest.raises(ValueError, match="Schema mismatch: IFC4 vs IFC2X3"):
        diff_engine.diff_files("old.ifc", "new.ifc")


def test_stream_diff_files_rejects_cross_schema_pairs(monkeypatch):
    graphs = iter([
        {"metadata": {"schema": "IFC4"}, "entities": {}},
        {"metadata": {"schema": "IFC2X3"}, "entities": {}},
    ])
    monkeypatch.setattr(diff_engine, "parse_graph", lambda *_args, **_kwargs: next(graphs))

    with pytest.raises(ValueError, match="Schema mismatch: IFC4 vs IFC2X3"):
        list(diff_engine.stream_diff_files("old.ifc", "new.ifc"))
