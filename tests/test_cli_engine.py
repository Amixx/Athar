import sys

import pytest

import athar.__main__ as main_mod


def test_cli_calls_engine_diff_files(monkeypatch, capsys):
    called = {}

    def fake_diff_files(old, new, matcher_policy, **_kwargs):
        called["args"] = (old, new, matcher_policy)
        return {"ok": True}

    monkeypatch.setattr(main_mod, "diff_files", fake_diff_files)
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "\"ok\": true" in out
    assert called["args"] == ("old.ifc", "new.ifc", {"spatial_radius_m": 0.5})


def test_cli_streams_ndjson(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, matcher_policy, mode, chunk_size, **_kwargs):
        called["stream"] = (old, new, matcher_policy, mode, chunk_size)
        return iter(["{\"record_type\":\"header\"}", "{\"record_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--stream", "ndjson"])

    main_mod.main()
    out = capsys.readouterr().out
    assert "{\"record_type\":\"header\"}" in out
    assert "{\"record_type\":\"end\"}" in out
    assert called["stream"] == ("old.ifc", "new.ifc", {"spatial_radius_m": 0.5}, "ndjson", 1000)


def test_cli_stream_chunk_size_and_radius(monkeypatch, capsys):
    called = {}

    def fake_stream_files(old, new, matcher_policy, mode, chunk_size, **_kwargs):
        called["stream"] = (old, new, matcher_policy, mode, chunk_size)
        return iter(["{\"chunk_type\":\"header\"}", "{\"chunk_type\":\"end\"}"])

    monkeypatch.setattr(main_mod, "stream_diff_files", fake_stream_files)
    monkeypatch.setattr(main_mod, "diff_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(
        sys,
        "argv",
        ["athar", "old.ifc", "new.ifc", "--stream", "chunked_json", "--chunk-size", "7", "--matcher-radius-m", "1.2"],
    )

    main_mod.main()
    _ = capsys.readouterr().out
    assert called["stream"] == ("old.ifc", "new.ifc", {"spatial_radius_m": 1.2}, "chunked_json", 7)


def test_cli_rejects_removed_legacy_flags(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--guid-policy", "disambiguate"])
    with pytest.raises(SystemExit):
        main_mod.main()


def test_cli_rejects_removed_timings_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["athar", "old.ifc", "new.ifc", "--timings"])
    with pytest.raises(SystemExit):
        main_mod.main()
