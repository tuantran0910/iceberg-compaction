.PHONY: install-maturin install-uv check develop build test

## Install maturin as a uv tool (run once)
install-maturin:
	uv tool install maturin

## Install uv (run once, if not already installed)
install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

## Check that uv and maturin are available
check:
	@uv --version
	@maturin --version
	@ruff check python/
	@ruff format --check python/

## Format Python code with ruff
format:
	ruff format python/
	ruff check --fix python/

## Install dev dependencies (without building the Rust extension)
sync:
	uv sync --no-install-project

## Build and install the extension in development mode
develop:
	maturin develop --uv

## Build a release wheel
build:
	maturin build --release

## Run tests
test:
	.venv/bin/pytest tests/ -v
