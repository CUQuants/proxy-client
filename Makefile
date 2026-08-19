.DEFAULT_GOAL := help

VENV   := .venv
PYTHON := python3.11

.PHONY: help install test build clean

help:
	@echo "  make install   create $(VENV) and install with dev dependencies"
	@echo "  make test      run the test suite"
	@echo "  make build     build the sdist and wheel into dist/"
	@echo "  make clean     remove $(VENV), build artifacts, and caches"

$(VENV)/bin/python:
	$(PYTHON) -m venv $(VENV)

install: $(VENV)/bin/python
	@$(VENV)/bin/pip install -q -e ".[dev]"

test: install
	@$(VENV)/bin/python -m pytest -q

build: install
	@$(VENV)/bin/python -m build

clean:
	rm -rf $(VENV) dist build *.egg-info .pytest_cache
	find . -name __pycache__ -exec rm -rf {} +
