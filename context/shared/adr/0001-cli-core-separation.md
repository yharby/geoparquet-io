# ADR-0001: CLI/Core Separation

## Status

Accepted

## Context

geoparquet-io provides a wide range of GeoParquet operations -- adding columns, partitioning, converting formats, sorting, validating, and more. Early in development, business logic was embedded directly in Click command functions, which made it difficult to test functionality without invoking the CLI and impossible to use the same logic from Python code.

As the project grew to include 50+ core modules and a comprehensive CLI surface, the need for a clean architectural boundary between CLI presentation and business logic became critical.

## Decision

CLI commands in `cli/main.py` are thin wrappers that handle argument parsing, user-facing output, and error presentation. All business logic lives in dedicated `core/` modules, where each module exposes a primary entry-point function (e.g., `add_bbox_table()`, `partition_by_quadkey()`, `hilbert_order_table()`).

The separation follows this pattern:

1. **CLI layer** (`cli/main.py`): Defines Click commands and options, calls core functions, handles `ClickException` for user-facing errors.
2. **Core layer** (`core/*.py`): Contains all data processing logic using DuckDB, PyArrow, and spatial operations. No dependency on Click. Functions follow naming conventions like `add_bbox_table()`, `partition_by_quadkey()`, `hilbert_order_table()`, etc.
3. **API layer** (`api/table.py`, `api/ops.py`): Provides Pythonic interfaces that wrap core functions for programmatic use.

## Consequences

### Positive
- Core logic is testable without the CLI framework -- unit tests call `*_table()` functions directly.
- The Python API (`api/table.py` and `api/ops.py`) can expose the same functionality without reimplementing logic.
- Core modules can be refactored or replaced independently of the CLI presentation.
- Multiple interfaces (CLI, API, future web) can share the same core logic.

### Negative
- Each new feature requires changes in at least two files (`core/*.py` and `cli/main.py`), plus the API layer.
- `cli/main.py` has grown large (~5400 lines) as a single file containing all command definitions, though each command is a thin wrapper.

### Neutral
- The `core/` directory contains 50+ modules, each focused on a specific operation. This keeps individual files manageable but increases the file count.
- Shared utilities live in `core/common.py` (~4000 lines), which acts as the internal standard library.

## Alternatives Considered

### Fat CLI commands
Putting all logic directly in Click command functions. Rejected because it makes unit testing require CLI invocation, prevents building a Python API, and creates tight coupling to the Click framework.

### Separate packages
Splitting core logic into a separate installable package. Rejected as premature -- the current module-level separation provides sufficient decoupling without the overhead of managing multiple packages.

## References

- `geoparquet_io/cli/main.py` -- CLI command definitions
- `geoparquet_io/core/` -- Business logic modules
- `geoparquet_io/api/table.py` -- Table class with method-based API
- `geoparquet_io/api/ops.py` -- Functional API
