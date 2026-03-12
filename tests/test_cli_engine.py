import sys

import athar.__main__ as main_mod


def test_cli_graph_engine_calls_diff_files(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, profile, guid_policy):
        called["args"] = (old, new, profile, guid_policy)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--profile", "raw_exact"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "\"ok\": true" in out
    assert called["args"] == ("old.ifc", "new.ifc", "raw_exact", "fail_fast")


def test_cli_graph_engine_streams_ndjson(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, profile, guid_policy, mode, chunk_size):
        called["stream"] = (old, new, profile, guid_policy, mode, chunk_size)
        return iter(["{\"record_type\":\"header\"}", "{\"record_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_: (_ for _ in ()).throw(AssertionError("diff_files called")))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--stream", "ndjson"],
    )

    main_mod.main()
    out = capsys.readouterr().out
    assert "{\"record_type\":\"header\"}" in out
    assert "{\"record_type\":\"end\"}" in out
    assert called["stream"] == ("old.ifc", "new.ifc", "semantic_stable", "fail_fast", "ndjson", 1000)


def test_cli_chunked_stream_passes_chunk_size(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, profile, guid_policy, mode, chunk_size):
        called["stream"] = (old, new, profile, guid_policy, mode, chunk_size)
        return iter(["{\"chunk_type\":\"header\"}", "{\"chunk_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_: (_ for _ in ()).throw(AssertionError("diff_files called")))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--stream", "chunked_json", "--chunk-size", "7"],
    )

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["stream"] == ("old.ifc", "new.ifc", "semantic_stable", "fail_fast", "chunked_json", 7)


def test_cli_passes_guid_policy(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, profile, guid_policy):
        called["args"] = (old, new, profile, guid_policy)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--guid-policy", "disambiguate"],
    )

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["args"] == ("old.ifc", "new.ifc", "semantic_stable", "disambiguate")
