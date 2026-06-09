PYTHON ?= python
MATURIN ?= maturin
DATE := $(shell date +%F)
CASE ?= house_v1_v2_scrambled:tests/fixtures/house_v1.ifc:tests/fixtures/house_v2_scrambled.ifc
PERF_DIR ?= docs/perf
PERF_HEARTBEAT ?= 10
PERF_PROFILE ?= semantic_stable
HOLY_GRAIL_OLD ?= real-world-test/real-world-spanish-180mb.ifc
HOLY_GRAIL_NEW ?= real-world-test/uni-project-house-50mb.ifc
PERF_PROGRESS_FILE ?= /tmp/athar_benchmark_progress.json
PERF_LOG_FILE ?= /tmp/athar_benchmark.log
PERF_OUT_FILE ?= $(PERF_DIR)/baseline_rewrite_$(DATE).json

.PHONY: help dev-setup native-dev native-dev-release native-build native-release test-native test-perf test-small test-medium test-medium-bg test-medium-log test-large-acceptance determinism perf-native perf-native-parallel perf-native-check perf-native-stream perf-holy-grail-serial perf-holy-grail-parallel perf-rewrite-bg perf-rewrite-watch perf-compare profile-prepare make-basichouse-modified make-house-versions

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev-setup                  Install common dev/runtime deps into the active venv" \
	"  make native-dev                 Build/install the native extension into the active venv" \
	"  make native-dev-release         Build/install the native extension in release mode" \
	"  make native-build               Build native wheels in debug mode" \
	"  make native-release             Build native wheels in release mode" \
	"  make test-native                Run focused tests for native/fingerprint paths" \
	"  make test-perf                  Run focused perf-script regression tests" \
	"  make test-small                 Run fast synthetic/unit-oriented diff coverage" \
	"  make test-medium                Run medium regression on small real-world fixture pair" \
	"  make test-medium-bg             Run medium regression in background and write /tmp logs" \
	"  make test-medium-log            Show latest medium regression background log lines" \
	"  make test-large-acceptance      Run opt-in large IFC acceptance checks" \
	"  make determinism                Run determinism stress harness" \
	"  make make-basichouse-modified   Regenerate data/BasicHouse_modified.ifc from data/BasicHouse.ifc" \
	"  make make-house-versions        Regenerate house_v1/v2/v2_scrambled/v3 fixtures" \
	"  make perf-native                Benchmark diff_graphs with ATHAR_PARALLEL=0 (release native)" \
	"  make perf-native-parallel       Benchmark diff_graphs with ATHAR_PARALLEL=1 (release native)" \
	"  make perf-native-check          Run the full default diff_graphs perf matrix with parallel off/on (release native, timestamped reports)" \
	"  make perf-native-stream         Run diff+stream metrics on basichouse_v1_v2 with ATHAR_PARALLEL=0 (release native)" \
	"  make perf-holy-grail-serial     Run diff_graphs benchmark on holy-grail pair with ATHAR_PARALLEL=0" \
	"  make perf-holy-grail-parallel   Run diff_graphs benchmark on holy-grail pair with ATHAR_PARALLEL=1" \
	"  make perf-rewrite-bg            Start rewrite baseline benchmark in background (log + progress sidecar)" \
	"  make perf-rewrite-watch         Watch the configured perf progress sidecar" \
	"  make perf-compare               Compare OLD=... and NEW=... benchmark JSON reports" \
	"  make profile-prepare            Profile prepare_diff_context on house fixtures" \
	"" \
	"Variables:" \
	"  PERF_PROFILE=semantic_stable    Diff profile for perf targets (e.g. raw_exact)" \
	"  HOLY_GRAIL_OLD=...              Old IFC path for holy-grail perf targets" \
	"  HOLY_GRAIL_NEW=...              New IFC path for holy-grail perf targets" \
	"  PERF_PROGRESS_FILE=...          Sidecar progress JSON path for background rewrite baseline" \
	"  PERF_LOG_FILE=...               Log path for background rewrite baseline" \
	"  PERF_OUT_FILE=...               Output JSON artifact path for background rewrite baseline"

dev-setup:
	@missing="$$( $(PYTHON) -c "import importlib.util; packages=['pytest','xxhash','ifcopenshell']; print(' '.join(name for name in packages if importlib.util.find_spec(name) is None))" )"; \
	if [ -n "$$missing" ]; then $(PYTHON) -m pip install $$missing; else echo "Dependencies already available in active venv."; fi

native-dev:
	$(MATURIN) develop --manifest-path athar/_native/Cargo.toml

native-dev-release:
	$(MATURIN) develop --release --manifest-path athar/_native/Cargo.toml

native-build:
	$(MATURIN) build --manifest-path athar/_native/Cargo.toml

native-release:
	$(MATURIN) build --release --manifest-path athar/_native/Cargo.toml

test-native:
	$(PYTHON) -m pytest \
		tests/test_text_fingerprint.py \
		tests/test_similarity_seed.py \
		tests/test_wl_refinement.py \
		tests/test_diff_engine.py -q

test-perf:
	$(PYTHON) -m pytest \
		tests/test_benchmark_diff_engine_script.py \
		tests/test_package_boundaries.py \
		tests/test_cli_engine.py -q

test-small:
	$(PYTHON) -m pytest \
		tests/test_delta_report.py \
		tests/test_matcher_core.py \
		tests/test_matcher_graph.py \
		tests/test_diff_engine_markers.py -q

test-medium:
	$(PYTHON) -m pytest tests/test_engine_regression_house_fixtures.py -q

test-medium-bg:
	nohup $(PYTHON) -m pytest tests/test_engine_regression_house_fixtures.py -q > /tmp/athar_test_medium.log 2>&1 & \
	echo $$! > /tmp/athar_test_medium.pid; \
	echo "started test-medium in background"; \
	echo "pid: $$(cat /tmp/athar_test_medium.pid)"; \
	echo "log: /tmp/athar_test_medium.log"

test-medium-log:
	@tail -n 60 /tmp/athar_test_medium.log

test-large-acceptance:
	ATHAR_RUN_LARGE_ACCEPTANCE=1 $(PYTHON) -m pytest tests/test_acceptance_large_ifc.py -q

determinism:
	$(PYTHON) -m scripts.explore.stress_determinism \
		--rounds 10 \
		--progress-every 1 \
		--out $(PERF_DIR)/determinism_native_$(DATE).json

make-basichouse-modified:
	$(PYTHON) scripts/make_modified_ifc.py data/BasicHouse.ifc tests/fixtures/BasicHouse_modified.ifc

make-house-versions:
	$(PYTHON) scripts/generate_house_versions.py

perf-native: native-dev-release
	ATHAR_BENCHMARK_NAME=native_only_rust ATHAR_PARALLEL=0 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--case "$(CASE)" \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-native-parallel: native-dev-release
	ATHAR_BENCHMARK_NAME=native_parallel_rust ATHAR_PARALLEL=1 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--case "$(CASE)" \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-native-check: native-dev-release
	ATHAR_BENCHMARK_NAME=native_check_rust_serial ATHAR_PARALLEL=0 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)
	ATHAR_BENCHMARK_NAME=native_check_rust_parallel ATHAR_PARALLEL=1 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-native-stream: native-dev-release
	ATHAR_BENCHMARK_NAME=native_stream_rust ATHAR_PARALLEL=0 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--case "basichouse_v1_v2:tests/fixtures/BasicHouse.ifc:tests/fixtures/BasicHouse_modified.ifc" \
		--metric diff_graphs \
		--metric stream_ndjson \
		--metric stream_chunked_json \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-holy-grail-serial: native-dev-release
	ATHAR_BENCHMARK_NAME=holy_grail_serial ATHAR_PARALLEL=0 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--case "holy_grail:$(HOLY_GRAIL_OLD):$(HOLY_GRAIL_NEW)" \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-holy-grail-parallel: native-dev-release
	ATHAR_BENCHMARK_NAME=holy_grail_parallel ATHAR_PARALLEL=1 $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--case "holy_grail:$(HOLY_GRAIL_OLD):$(HOLY_GRAIL_NEW)" \
		--metric diff_graphs \
		--profile $(PERF_PROFILE) \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT)

perf-rewrite-bg:
	nohup $(PYTHON) -m scripts.explore.benchmark_diff_engine \
		--warmup 0 \
		--iterations 1 \
		--engine-timings \
		--heartbeat-s $(PERF_HEARTBEAT) \
		--progress-file $(PERF_PROGRESS_FILE) \
		--out $(PERF_OUT_FILE) \
		> $(PERF_LOG_FILE) 2>&1 & \
	echo "started benchmark_diff_engine in background"; \
	echo "log: $(PERF_LOG_FILE)"; \
	echo "progress: $(PERF_PROGRESS_FILE)"; \
	echo "out: $(PERF_OUT_FILE)"

perf-rewrite-watch:
	$(PYTHON) -m scripts.explore.watch_progress --file $(PERF_PROGRESS_FILE) --interval-s 2

perf-compare:
	@test -n "$(OLD)" || (echo "Set OLD=path/to/old.json"; exit 1)
	@test -n "$(NEW)" || (echo "Set NEW=path/to/new.json"; exit 1)
	@OLD="$(OLD)" NEW="$(NEW)" $(PYTHON) -c 'import json, os; from pathlib import Path; old = json.loads(Path(os.environ["OLD"]).read_text()); new = json.loads(Path(os.environ["NEW"]).read_text()); engine = lambda report, key: report["results"][0]["metrics"]["diff_graphs"]["engine_timings_ms"]["summary"][key]["mean"]; metric = lambda report, key: report["results"][0]["metrics"]["diff_graphs"]["summary"][key]["mean"]; keys = ["context.seed_text_fingerprints", "context.precompute_old_identity", "context.precompute_new_identity", "prepare_context", "total"]; [print(f"{key:35} {engine(old, key)/1000:8.1f}s -> {engine(new, key)/1000:8.1f}s   {(((engine(old, key) - engine(new, key)) / engine(old, key) * 100.0) if engine(old, key) else 0.0):6.1f}% faster") for key in keys]; print(f"\npeak_mem_bytes{'':24} {metric(old, \"peak_mem_bytes\")/1e9:8.2f}GB -> {metric(new, \"peak_mem_bytes\")/1e9:8.2f}GB")'

profile-prepare:
	$(PYTHON) -m scripts.explore.profile_prepare_context \
		--old tests/fixtures/house_v1.ifc \
		--new tests/fixtures/house_v2_scrambled.ifc \
		--warmup 0 \
		--iterations 1 \
		--heartbeat-s 15 \
		--out $(PERF_DIR)/prepare_context_$(DATE).json
