# Prefer the repo-local .venv; override with `make PYTHON=...` or an activated env.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)
BENCHMARK_DIR ?= .athar-benchmark
BENCHMARK_JSON ?= $(BENCHMARK_DIR)/competitor-benchmark.json
BENCHMARK_WORKDIR ?= $(BENCHMARK_DIR)/work
BENCHMARK_PAGE ?= $(BENCHMARK_DIR)/site/index.html
BENCHMARK_REPEATS ?= 3
BENCHMARK_TIMEOUT_S ?= 300

.PHONY: help dev-setup benchmark-setup benchmark-ui-setup benchmark-run benchmark-page benchmark-ui benchmark-ui-refresh test test-large-acceptance native-build native-clean viewer-build viewer-test

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev-setup              Install runtime/dev deps into the active venv" \
	"  make benchmark-setup        Install optional benchmark deps" \
	"  make benchmark-ui-setup     Install optional benchmark UI deps" \
	"  make benchmark-run          Generate local benchmark JSON artifact" \
	"  make benchmark-page         Generate static benchmark HTML artifact" \
	"  make benchmark-ui           Generate if missing, then serve dashboard locally" \
	"  make benchmark-ui-refresh   Regenerate benchmark JSON, then serve dashboard locally" \
	"  make test                   Run the default test suite (fast; small real IFC fixtures)" \
	"  make test-large-acceptance  Run opt-in large IFC acceptance checks" \
	"  make native-build           Build the optional Rust accelerator into the venv (requires cargo + maturin)" \
	"  make native-clean           Remove the Rust build artifacts (athar/_native/target)" \
	"  make viewer-build           Build the viewer SPA into viewer/dist (requires bun)" \
	"  make viewer-test            Viewer unit tests + headless e2e smoke (requires bun)"

dev-setup:
	@missing="$$( $(PYTHON) -c "import importlib.util; packages=['pytest','ifcopenshell']; print(' '.join(name for name in packages if importlib.util.find_spec(name) is None))" )"; \
	if [ -n "$$missing" ]; then $(PYTHON) -m pip install $$missing; else echo "Dependencies already available in active venv."; fi

benchmark-setup:
	$(PYTHON) -m pip --isolated install '.[benchmark]'

benchmark-ui-setup:
	$(PYTHON) -m pip --isolated install '.[benchmark-ui]'

benchmark-run:
	mkdir -p $(BENCHMARK_DIR)
	$(PYTHON) scripts/explore/benchmark_competitors.py \
		--out $(BENCHMARK_JSON) \
		--workdir $(BENCHMARK_WORKDIR) \
		--timeout-s $(BENCHMARK_TIMEOUT_S) \
		--repeats $(BENCHMARK_REPEATS)

benchmark-page:
	@if [ ! -f "$(BENCHMARK_JSON)" ]; then $(MAKE) benchmark-run; fi
	mkdir -p $(dir $(BENCHMARK_PAGE))
	$(PYTHON) -m athar_bench.ui --benchmark $(BENCHMARK_JSON) --static-out $(BENCHMARK_PAGE)

benchmark-ui:
	@if [ ! -f "$(BENCHMARK_JSON)" ]; then $(MAKE) benchmark-run; fi
	ATHAR_BENCHMARK_JSON=$(BENCHMARK_JSON) $(PYTHON) -m athar_bench.ui

benchmark-ui-refresh: benchmark-run benchmark-ui

test:
	$(PYTHON) -m pytest tests/ -q

test-large-acceptance:
	ATHAR_RUN_LARGE_ACCEPTANCE=1 $(PYTHON) -m pytest tests/test_acceptance_large_ifc.py -q --durations=5

# Optional native accelerator (Stage A: Merkle + WL gossip). Builds the Rust
# extension straight into the active venv via maturin; pure-Python users never
# need to run this — athar.bottom falls back automatically when it is absent.
native-build:
	@$(PYTHON) -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('maturin') else 1)" \
		|| $(PYTHON) -m pip install "maturin>=1.5,<2.0"
	$(PYTHON) -m maturin develop --release --manifest-path athar/_native/Cargo.toml --interpreter $(PYTHON)

native-clean:
	rm -rf athar/_native/target

viewer-build:
	cd viewer && bun install && bun run build

viewer-test:
	cd viewer && bun test tests && bunx playwright test
