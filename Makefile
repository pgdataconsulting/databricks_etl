# Developer tasks. Run `make help` for the list.
#
# The targets call tools from the active environment; to use a virtualenv
# without activating it:  make test PYTHON=.venv/bin/python

PYTHON ?= python
CONFIGS := $(sort $(wildcard configs/*.yaml) $(wildcard configs/medallion/*.yaml))
TF_STACKS := infra/terraform/stacks/platform infra/terraform/stacks/governance

.DEFAULT_GOAL := help
.PHONY: help install hooks lint format typecheck test coverage validate pipeline-docs \
        demo build check bundle-schema tf-check clean

help:  ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:  ## Install the package with development dependencies
	$(PYTHON) -m pip install -e ".[dev]"

hooks:  ## Install the pre-commit hooks
	$(PYTHON) -m pre_commit install

lint:  ## Lint and check formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:  ## Apply lint fixes and formatting
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

typecheck:  ## Type-check the package
	$(PYTHON) -m mypy

test:  ## Run the test suite (needs Java 17+)
	$(PYTHON) -m pytest

coverage:  ## Run the tests with a coverage report
	$(PYTHON) -m pytest --cov --cov-report=term-missing

validate:  ## Validate every pipeline configuration (no Spark needed)
	$(PYTHON) -m etl_framework validate $(CONFIGS)

pipeline-docs:  ## Regenerate docs/pipelines from the configurations
	@for config in $(CONFIGS); do \
		$(PYTHON) -m etl_framework docs $$config -o docs/pipelines/$$(basename $$config .yaml).md || exit 1; \
	done

demo:  ## Run the medallion example end to end on a local Spark session
	$(PYTHON) examples/quickstart.py

build:  ## Build and check the wheel and sdist
	rm -rf dist
	$(PYTHON) -m build
	$(PYTHON) -m twine check --strict dist/*

check: lint typecheck validate test  ## Everything CI checks on the Python side

bundle-schema:  ## Check the Asset Bundle against the Databricks CLI's schema
	databricks bundle schema > .bundle.schema.json
	$(PYTHON) -m check_jsonschema --schemafile .bundle.schema.json databricks.yml resources/*.yml; \
		status=$$?; rm -f .bundle.schema.json; exit $$status

tf-check:  ## Format-check, validate and test the Terraform stacks (no credentials needed)
	terraform fmt -check -recursive infra/terraform
	@for stack in $(TF_STACKS); do \
		terraform -chdir=$$stack init -backend=false -input=false > /dev/null && \
		terraform -chdir=$$stack validate && \
		terraform -chdir=$$stack test || exit 1; \
	done

clean:  ## Remove build, cache and local run artifacts
	rm -rf build dist *.egg-info artifacts htmlcov coverage.xml .coverage
	rm -rf .pytest_cache .ruff_cache .mypy_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -not -path "./.venv/*" -prune -exec rm -rf {} +
