import sys
import json

import pytest

import athar.__main__ as main_mod


def test_cli_calls_engine_diff_files(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, **_kwargs):
        called["args"] = (old, new)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "\"ok\": true" in out
    assert called["args"] == ("old.ifc", "new.ifc")


def test_cli_passes_generated_at_timestamp(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, **kwargs):
        called["args"] = (old, new)
        called["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--generated-at", "2026-06-10T19:00:00Z"])

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["args"] == ("old.ifc", "new.ifc")
    assert called["kwargs"] == {"generated_at": "2026-06-10T19:00:00Z"}


def test_cli_streams_ndjson(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, mode, chunk_size, **_kwargs):
        called["stream"] = (old, new, mode, chunk_size)
        return iter(["{\"record_type\":\"header\"}", "{\"record_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--stream", "ndjson"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "{\"record_type\":\"header\"}" in out
    assert "{\"record_type\":\"end\"}" in out
    assert called["stream"] == ("old.ifc", "new.ifc", "ndjson", 1000)


def test_cli_stream_passes_generated_at_now(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, mode, chunk_size, **kwargs):
        called["stream"] = (old, new, mode, chunk_size)
        called["kwargs"] = kwargs
        return iter(["{\"record_type\":\"header\"}", "{\"record_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "generated_at_now_utc", lambda: "2026-06-10T19:00:00Z")
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--stream", "ndjson", "--generated-at", "now"])

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["stream"] == ("old.ifc", "new.ifc", "ndjson", 1000)
    assert called["kwargs"] == {"generated_at": "2026-06-10T19:00:00Z"}


def test_cli_stream_chunk_size(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, mode, chunk_size, **_kwargs):
        called["stream"] = (old, new, mode, chunk_size)
        return iter(["{\"chunk_type\":\"header\"}", "{\"chunk_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--stream", "chunked_json", "--chunk-size", "7"],
    )

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["stream"] == ("old.ifc", "new.ifc", "chunked_json", 7)


def test_cli_schema_mismatch_emits_structured_result_and_exit_3(monkeypatch, capsys):
    from athar.engine import SchemaMismatchError

    def fake_diff_files(old, new, **_kwargs):
        raise SchemaMismatchError("IFC2X3", "IFC4")

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc"])

    with pytest.raises(SystemExit) as excinfo:
        main_mod.main()
    assert excinfo.value.code == 3

    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["status"] == "schema_incompatible"
    assert result["schemas"] == {"old": "IFC2X3", "new": "IFC4"}
    assert "IFC2X3" in result["message"] and "IFC4" in result["message"]
    # Human-readable line still goes to stderr.
    assert "Schema mismatch" in captured.err


def test_cli_rejects_removed_legacy_flags(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--guid-policy", "disambiguate"])
    with pytest.raises(SystemExit):
        main_mod.main()


def test_cli_rejects_removed_matcher_radius_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--matcher-radius-m", "1.2"])
    with pytest.raises(SystemExit):
        main_mod.main()


def test_cli_rejects_removed_timings_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--timings"])
    with pytest.raises(SystemExit):
        main_mod.main()
