# Prefer the repo-local .venv; override with `make PYTHON=...` or an activated env.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)

.PHONY: help dev-setup test test-large-acceptance viewer-build viewer-test

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev-setup              Install runtime/dev deps into the active venv" \
	"  make test                   Run the default test suite (fast; small real IFC fixtures)" \
	"  make test-large-acceptance  Run opt-in large IFC acceptance checks" \
	"  make viewer-build           Build the viewer SPA into viewer/dist (requires bun)" \
	"  make viewer-test            Viewer unit tests + headless e2e smoke (requires bun)"

dev-setup:
	@missing="$$( $(PYTHON) -c "import importlib.util; packages=['pytest','ifcopenshell']; print(' '.join(name for name in packages if importlib.util.find_spec(name) is None))" )"; \
	if [ -n "$$missing" ]; then $(PYTHON) -m pip install $$missing; else echo "Dependencies already available in active venv."; fi

test:
	$(PYTHON) -m pytest tests/ -q

test-large-acceptance:
	ATHAR_RUN_LARGE_ACCEPTANCE=1 $(PYTHON) -m pytest tests/test_acceptance_large_ifc.py -q --durations=5

viewer-build:
	cd viewer && bun install && bun run build

viewer-test:
	cd viewer && bun test tests && bunx playwright test
