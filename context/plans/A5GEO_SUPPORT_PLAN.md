# A5Geo Support Implementation Plan

## Overview
Add support for A5Geo discrete global grid system (DGGS) partitioning, following the same patterns as H3 support. A5 uses hierarchical pentagonal cells based on a dodecahedron to partition the Earth's surface into equal-area cells with minimal shape distortion.

## Research Summary
- **A5 Format**: 64-bit integer cell IDs at 31 resolution levels (smallest ~30mm²)
- **DuckDB Integration**: Community extension `a5` with functions like `a5_lonlat_to_cell()`
- **Key Advantage**: Equal-area cells with minimal distortion using dodecahedron geometry
- **Implementation**: Available via DuckDB community extensions

## Implementation Chunks

### Chunk 1: Core add_a5_column functionality
**Files to create:**
- `geoparquet_io/core/add_a5_column.py`

**Implementation details:**
- Follow `add_h3_column.py` pattern exactly
- Use `a5_lonlat_to_cell()` function from A5 DuckDB extension
- Store A5 cell IDs as UBIGINT (unsigned 64-bit integer)
- Support resolutions 0-30 (A5 has 31 levels: 0-30)
- Add resolution size helper mapping approximate cell areas
- Support both file-based and streaming modes
- Include `add_a5_table()` function for Python API
- Add custom metadata: `{"covering": {"a5": {"column": name, "resolution": res}}}`

**Tests to write first:**
- Valid resolution ranges (0-30)
- Invalid resolution (negative, >30)
- Column name customization
- Geometry column auto-detection
- Streaming input/output

### Chunk 2: Core partition_by_a5 functionality
**Files to create:**
- `geoparquet_io/core/partition_by_a5.py`

**Implementation details:**
- Follow `partition_by_h3.py` pattern exactly
- Auto-add A5 column if not present using `_ensure_a5_column()`
- Support preview mode for partition distribution
- Integrate with partition_common utilities
- Handle stdin streaming input
- Support Hive-style partitioning
- Cleanup temporary files properly

**Tests to write first:**
- Partition creation with auto-add column
- Partition with existing A5 column
- Preview mode
- Hive-style partitioning
- Keep/drop A5 column in output
- Force mode with analysis warnings

### Chunk 3: CLI commands
**Files to modify:**
- `geoparquet_io/cli/main.py`

**Implementation details:**
- Add `gpio add a5` command following H3 pattern
- Add `gpio partition a5` command following H3 pattern
- Use existing decorators from `cli/decorators.py`
- Default resolution: 15 (balanced for most use cases)
- DEFAULT_A5_COLUMN_NAME = "a5_cell"

**Tests to write first:**
- CLI argument parsing
- Help text generation
- Integration with core functions

### Chunk 4: Python API
**Files to modify:**
- `geoparquet_io/api/table.py`
- `geoparquet_io/api/ops.py`

**Implementation details:**
- Export `add_a5_table()` in table.py
- Export `add_a5_column()` in ops.py
- Follow existing API patterns

**Tests to write first:**
- API imports work correctly
- Table-based operations
- File-based operations

### Chunk 5: Constants and configuration
**Files to modify:**
- `geoparquet_io/core/constants.py`

**Implementation details:**
- Add `DEFAULT_A5_COLUMN_NAME = "a5_cell"`

### Chunk 6: Documentation
**Files to create/modify:**
- `docs/guide/a5.md` (new)
- `docs/api/python-api.md` (update)
- Update any index/navigation files

**Content requirements:**
- CLI examples with tabs
- Python API examples with tabs
- Resolution guide (similar to H3)
- Comparison with other DGGS systems
- Use cases and best practices

## Test Strategy
- Write tests FIRST for each chunk (TDD)
- Run fast tests during development: `uv run pytest -n auto -m "not slow and not network"`
- Aim for >80% coverage on new code
- Test both CLI and Python API paths
- Include edge cases (empty files, invalid resolutions, etc.)

## Quality Checks
- Complexity: Aim for grade A with `xenon --max-absolute=A`
- Linting: `uv run ruff check --fix .`
- Formatting: `uv run ruff format .`
- Pre-commit: All hooks must pass

## Commit Strategy
- One commit per chunk after tests pass
- Format: "Add [feature] for A5Geo support"
- No emoji, imperative mood

## Integration Points
- Reuse `partition_common.py` utilities
- Reuse `add_computed_column()` from `common.py`
- Follow existing streaming patterns
- Use existing decorators for CLI
