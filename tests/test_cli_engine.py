import sys

import athar.__main__ as main_mod


def test_cli_graph_engine_calls_diff_files(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, profile):
        called["args"] = (old, new, profile)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(main_mod, "parse", lambda *_: (_ for _ in ()).throw(AssertionError("parse called")))
    monkeypatch.setattr(main_mod, "diff", lambda *_: (_ for _ in ()).throw(AssertionError("diff called")))
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--engine", "graph", "--profile", "raw_exact"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "\"ok\": true" in out
    assert called["args"] == ("old.ifc", "new.ifc", "raw_exact")


def test_cli_legacy_engine_calls_diff(monkeypatch, capsys):
    called = {}

    def fake_parse(path):
        return {"metadata": {"schema": "IFC4"}, "entities": {path: {}}}

    def fake_diff(old, new):
        called["args"] = (old, new)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", lambda *_: (_ for _ in ()).throw(AssertionError("diff_files called")))
    monkeypatch.setattr(main_mod, "parse", fake_parse)
    monkeypatch.setattr(main_mod, "diff", fake_diff)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--engine", "legacy"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "\"ok\": true" in out
    assert called["args"][0]["entities"]["old.ifc"] == {}
    assert called["args"][1]["entities"]["new.ifc"] == {}


def test_cli_graph_engine_streams_ndjson(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, profile, mode, chunk_size):
        called["stream"] = (old, new, profile, mode, chunk_size)
        return iter(["{\"record_type\":\"header\"}", "{\"record_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_: (_ for _ in ()).throw(AssertionError("diff_files called")))
    monkeypatch.setattr(main_mod, "parse", lambda *_: (_ for _ in ()).throw(AssertionError("parse called")))
    monkeypatch.setattr(main_mod, "diff", lambda *_: (_ for _ in ()).throw(AssertionError("diff called")))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--engine", "graph", "--stream", "ndjson"],
    )

    main_mod.main()
    out = capsys.readouterr().out
    assert "{\"record_type\":\"header\"}" in out
    assert "{\"record_type\":\"end\"}" in out
    assert called["stream"] == ("old.ifc", "new.ifc", "semantic_stable", "ndjson", 1000)


def test_cli_chunked_stream_passes_chunk_size(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, profile, mode, chunk_size):
        called["stream"] = (old, new, profile, mode, chunk_size)
        return iter(["{\"chunk_type\":\"header\"}", "{\"chunk_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_: (_ for _ in ()).throw(AssertionError("diff_files called")))
    monkeypatch.setattr(main_mod, "parse", lambda *_: (_ for _ in ()).throw(AssertionError("parse called")))
    monkeypatch.setattr(main_mod, "diff", lambda *_: (_ for _ in ()).throw(AssertionError("diff called")))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--engine", "graph", "--stream", "chunked_json", "--chunk-size", "7"],
    )

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["stream"] == ("old.ifc", "new.ifc", "semantic_stable", "chunked_json", 7)
