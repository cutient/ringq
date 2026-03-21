# Contributing

## Setup

```bash
git clone https://github.com/cutient/ringq.git
cd ringq
uv sync --extra dev
```

This installs all development dependencies and builds the Cython extensions in-place.

## Running tests

```bash
uv run pytest tests/ -v
```

## Rebuilding Cython extensions

After modifying `.pyx` or `.pxd` files, rebuild in-place:

```bash
uv run python setup.py build_ext --inplace
```

## Benchmarks

```bash
uv run python benchmarks/run.py
```

## Code style

- Follow existing code conventions in the project.
- Add type annotations to all public functions and methods.
- Keep docstrings on public API — one-liner is fine.

## Pull requests

1. Create a feature branch off `main`.
2. Write tests for new functionality — aim to cover both happy path and edge cases.
3. Make sure all tests pass (`uv run pytest tests/ -v`).
4. Keep commits focused — one logical change per commit.
5. Open a PR and fill in the template.
