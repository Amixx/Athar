# Prefer the repo-local .venv; override with `make PYTHON=...` or an activated env.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)

.PHONY: help dev-setup test test-large-acceptance

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev-setup              Install runtime/dev deps into the active venv" \
	"  make test                   Run the default test suite (fast; small real IFC fixtures)" \
	"  make test-large-acceptance  Run opt-in large IFC acceptance checks"

dev-setup:
	@missing="$$( $(PYTHON) -c "import importlib.util; packages=['pytest','ifcopenshell']; print(' '.join(name for name in packages if importlib.util.find_spec(name) is None))" )"; \
	if [ -n "$$missing" ]; then $(PYTHON) -m pip install $$missing; else echo "Dependencies already available in active venv."; fi

test:
	$(PYTHON) -m pytest tests/ -q

test-large-acceptance:
	ATHAR_RUN_LARGE_ACCEPTANCE=1 $(PYTHON) -m pytest tests/test_acceptance_large_ifc.py -q --durations=5
