# ADR-0004: Python API Mirrors CLI

## Status

Accepted

## Context

geoparquet-io started as a CLI tool, but users increasingly want to use its functionality from Python scripts, Jupyter notebooks, and data pipelines. Rather than forcing users to shell out to the CLI with `subprocess`, the project needs a first-class Python API that provides the same capabilities.

The question is how to structure this API: should it mirror the CLI surface, provide a completely different interface, or offer multiple paradigms?

## Decision

Every CLI command has a corresponding Python API exposed through two complementary interfaces:

1. **Method-based API** (`api/table.py`): A `Table` class that wraps a GeoParquet file and provides chainable methods. Created via `gpio.read("file.parquet")`, with methods like `.add_bbox()`, `.sort_hilbert()`, `.reproject()`.

2. **Functional API** (`api/ops.py`): Standalone functions like `ops.add_bbox(table)` that operate on PyArrow tables, providing a lower-level interface for pipeline composition.

Both APIs delegate to the same `core/` module functions (e.g., `add_bbox_table()`, `partition_by_quadkey()` from ADR-0001), ensuring behavioral parity with the CLI.

```python
import geoparquet_io as gpio
from geoparquet_io.api import ops
import pyarrow.parquet as pq

# Method-based (chainable, file-oriented)
table = gpio.read("input.parquet")
table.add_bbox().sort_hilbert().write("output.parquet")

# Functional (operates on PyArrow tables)
arrow_table = pq.read_table("input.parquet")
arrow_table = ops.add_bbox(arrow_table)
arrow_table = ops.sort_hilbert(arrow_table)
```

When a new CLI command is added, a corresponding method must be added to `Table` and a corresponding function to `ops.py`. This is enforced through code review and documented in the contributor checklist.

## Consequences

### Positive
- Users get full functionality from Python without subprocess calls.
- The `Table` class provides IDE autocomplete and method chaining for interactive use.
- The functional API in `ops.py` provides a table-centric interface for users composing PyArrow pipelines.
- Behavioral parity is guaranteed since both APIs and the CLI share core implementations.

### Negative
- Every new feature requires updating three locations: `core/*.py`, `cli/main.py`, and `api/` (both `table.py` and `ops.py`).
- The API surface must be maintained and documented alongside the CLI.
- Parameter naming must be kept consistent between CLI options and API arguments.

### Neutral
- The `Table` class in `api/table.py` currently has ~2300 lines, growing with each new operation. This is manageable since each method is self-contained.
- Some operations (like partitioning) produce directory output rather than a single file, which fits the functional API better than the method-based API.

## Alternatives Considered

### CLI-only (no Python API)
Requiring users to call the CLI via subprocess. Rejected because it provides poor developer experience -- no type hints, no autocomplete, string-based argument passing, and subprocess overhead.

### Auto-generated API from CLI
Automatically generating Python functions from Click command definitions. Rejected because the resulting API would expose Click-specific concepts (like parameter types) and would not support method chaining or a clean `Table` abstraction.

### Table class only (no functional API)
Providing only the method-based `Table` API. Rejected because some operations (batch processing, simple one-off transforms) are more naturally expressed as function calls than method chains.

## References

- `geoparquet_io/api/table.py` -- `Table` class with method-based API
- `geoparquet_io/api/ops.py` -- Functional API (operates on PyArrow tables)
- `geoparquet_io/api/__init__.py` -- Public API surface
- `geoparquet_io/api/check.py` -- Validation API
- `geoparquet_io/api/pipeline.py` -- Pipeline operations
- `geoparquet_io/api/stac.py` -- STAC metadata API
- ADR-0001 -- CLI/Core separation that enables this pattern
