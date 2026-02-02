# S2 Spatial Index Support Plan

**Issue**: [#12 - Add S2 as an 'add' option](https://github.com/geoparquet/geoparquet-io/issues/12)
**Date**: 2025-02-02
**Status**: Planned

## Overview

Add Google S2 spatial indexing support to geoparquet-io, consistent with existing H3, KD-tree, and quadkey implementations. This leverages DuckDB's mature [geography extension](https://duckdb.org/community_extensions/extensions/geography) which provides S2 cell operations.

## S2 Background

S2 (Google's Spherical Geometry library) divides the Earth's surface into a hierarchy of cells:

- **Level 0**: 6 base cells (cube faces projected onto sphere)
- **Each level subdivides by 4**: Level N has 6 × 4^N cells
- **Level 30**: Maximum resolution (~0.7 cm² per cell)
- **Cell IDs**: 64-bit integers encoding both hierarchy and position

### S2 vs H3 Comparison

| Aspect | S2 | H3 |
|--------|----|----|
| Subdivision | 4× (quadtree) | 7× (hexagonal) |
| Base cells | 6 (cube faces) | 122 |
| Max level | 30 | 15 |
| Cell shape | Variable (mostly quads) | Hexagons + pentagons |
| Typical use | Coarse: 4-8, Fine: 12-18 | Coarse: 3-5, Fine: 9-13 |

## Implementation Plan

### Step 1: Add S2 Constants
**File**: `geoparquet_io/core/constants.py`

```python
# Default column name for S2 cell IDs
DEFAULT_S2_COLUMN_NAME = "s2_cell"

# Default S2 level (comparable to H3 resolution 9)
DEFAULT_S2_LEVEL = 13
```

**Commit**: "Add S2 constants for default column name and level"

---

### Step 2: Create S2 Column Addition Module
**File**: `geoparquet_io/core/add_s2_column.py`

Following the exact pattern from `add_h3_column.py`:

1. `add_s2_table()` - Table-centric Python API
2. `add_s2_column()` - File-based API with streaming support
3. `_add_s2_streaming()` - Handle stdin/stdout streaming
4. `_make_add_s2_query()` - Build SQL for S2 cell computation
5. `_get_level_size()` - Human-readable cell sizes per level

**Key SQL Expression** (using DuckDB geography extension):
```sql
-- Convert to S2 cell token (string format for portability)
s2_cell_to_token(
    s2_cellfromlonlat(
        ST_X(ST_Centroid(geometry)),
        ST_Y(ST_Centroid(geometry)),
        {level}
    )
)
```

**S2 Level Reference Table**:
| Level | Approx. Area | Use Case |
|-------|--------------|----------|
| 4 | ~324,000 km² | Continental |
| 8 | ~1,250 km² | Regional |
| 10 | ~78 km² | City-scale |
| 13 | ~1.2 km² | Neighborhood |
| 15 | ~0.08 km² | Block-level |
| 18 | ~4,500 m² | Building-level |
| 21 | ~280 m² | Parcel-level |

**Commit**: "Add S2 column computation with add_s2_table and add_s2_column"

---

### Step 3: Create S2 Partition Module
**File**: `geoparquet_io/core/partition_by_s2.py`

Following `partition_by_h3.py` pattern:

1. `_ensure_s2_column()` - Check/add S2 column if needed
2. `_run_preview()` - Preview partition distribution
3. `partition_by_s2()` - Main partitioning function with:
   - Auto-resolution support
   - Hive-style partitioning
   - Preview mode
   - Keep/remove S2 column options

**Commit**: "Add S2 partitioning with partition_by_s2"

---

### Step 4: Add S2 Auto-Resolution Calculation
**File**: `geoparquet_io/core/partition_auto_resolution.py`

Add `_calculate_s2_resolution()` function:

```python
def _calculate_s2_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 30,
    verbose: bool = False,
) -> int:
    """
    S2 has 6 base cells at level 0.
    Each level multiplies by 4 (quadtree subdivision).
    Formula: cells(level) = 6 × 4^level

    Solving for level: target = 6 × 4^level
    level = log(target / 6) / log(4)
    """
```

Update `calculate_auto_resolution()` to support `spatial_index_type="s2"`.

**Commit**: "Add S2 auto-resolution calculation"

---

### Step 5: Add CLI Commands
**File**: `geoparquet_io/cli/main.py`

#### 5a. Add S2 command under `add` group
```bash
gpio add s2 input.parquet output.parquet --level 13
gpio add s2 input.parquet output.parquet --level 13 --s2-name s2_index
```

**Options**:
- `--level` / `-l`: S2 level (0-30), default 13
- `--s2-name`: Column name (default: s2_cell)
- Standard output options (compression, row-group-size, etc.)

#### 5b. Add S2 command under `partition` group
```bash
gpio partition s2 input.parquet output/ --level 13
gpio partition s2 input.parquet output/ --auto
gpio partition s2 input.parquet output/ --auto --target-rows 50000
```

**Options**:
- `--level` / `-l`: S2 level (mutually exclusive with --auto)
- `--auto`: Auto-calculate optimal level
- `--target-rows`: Target rows per partition for auto mode
- `--keep-s2-column`: Keep S2 column in output
- `--hive`: Hive-style partitioning
- Standard partition options

**Commit**: "Add S2 CLI commands for add and partition"

---

### Step 6: Add Python API Methods
**File**: `geoparquet_io/api/table.py` and `geoparquet_io/api/ops.py`

Add to `GeoParquetTable` class:
```python
def add_s2(
    self,
    column_name: str = "s2_cell",
    level: int = 13,
) -> "GeoParquetTable":
    """Add S2 cell ID column based on geometry centroids."""

def partition_by_s2(
    self,
    output: str,
    level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    ...
) -> None:
    """Partition by S2 cells."""
```

**Commit**: "Add S2 methods to Python API"

---

### Step 7: Write Tests (TDD - tests first!)
**Files**:
- `tests/test_add_s2.py`
- `tests/test_partition_s2.py`

#### Test Categories:

**test_add_s2.py**:
1. `TestAddS2Table`:
   - `test_add_s2_basic` - Basic column addition
   - `test_add_s2_custom_column_name` - Custom column name
   - `test_add_s2_different_levels` - Various S2 levels
   - `test_add_s2_invalid_level_low` - Error on level < 0
   - `test_add_s2_invalid_level_high` - Error on level > 30
   - `test_add_s2_metadata_preserved` - GeoParquet metadata preserved

2. `TestAddS2File`:
   - `test_add_s2_file_basic` - File-to-file S2 addition
   - `test_add_s2_file_custom_name` - Custom column name

3. `TestAddS2Streaming`:
   - `test_stdin_to_file` - Streaming input
   - `test_file_to_stdout` - Streaming output

4. `TestAddS2CLI`:
   - `test_add_s2_cli_help` - Help text works
   - `test_add_s2_cli_basic` - Basic CLI invocation

**test_partition_s2.py**:
1. `TestPartitionByS2`:
   - `test_partition_basic` - Basic partitioning
   - `test_partition_custom_level` - Custom level
   - `test_partition_hive_style` - Hive-style output
   - `test_partition_keep_column` - Keep S2 column option
   - `test_partition_preview` - Preview mode

2. `TestS2AutoResolution`:
   - `test_auto_resolution_small_dataset` - Few rows
   - `test_auto_resolution_large_dataset` - Many rows
   - `test_auto_resolution_respects_max_partitions` - Max partitions limit

**Commit**: "Add comprehensive S2 tests"

---

### Step 8: Update Documentation
**Files**:
- `docs/guide/add.md` - Add S2 section
- `docs/guide/partition.md` - Add S2 section
- `docs/_includes/s2-levels.md` - S2 level reference table
- `docs/api/python-api.md` - Document add_s2 and partition_by_s2

#### Documentation for docs/guide/add.md:

```markdown
## S2 Spherical Cells

Add [S2](https://s2geometry.io/) spherical cell IDs based on geometry centroids:

=== "CLI"

    ```bash
    gpio add s2 input.parquet output.parquet --level 13

    # From HTTPS to S3
    gpio add s2 https://example.com/data.parquet s3://bucket/indexed.parquet --level 13
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_s2(level=13).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_s2(column_name='s2_index', level=18).write('output.parquet')
    ```

**Level guide:**

--8<-- "_includes/s2-levels.md"
```

**Commit**: "Update documentation for S2 support"

---

## Implementation Order (TDD)

For each step, follow TDD:

1. **Write tests first** that define expected behavior
2. **Run tests** - verify they fail
3. **Implement** minimal code to pass
4. **Run tests** - verify they pass
5. **Refactor** if needed
6. **Commit**

### Execution Sequence:

1. Step 1 (Constants) - No tests needed
2. Step 7a (test_add_s2.py tests) → Step 2 (Implementation)
3. Step 7b (test_partition_s2.py tests) → Step 3 + Step 4 (Implementation)
4. Step 7c (CLI tests) → Step 5 (Implementation)
5. Step 7d (API tests) → Step 6 (Implementation)
6. Step 8 (Documentation)
7. Final verification: `uv run pytest -n auto`

## Verification Checklist

- [ ] All tests pass: `uv run pytest -n auto -m "not slow and not network"`
- [ ] Linting passes: `uv run ruff check .`
- [ ] Formatting passes: `uv run ruff format --check .`
- [ ] Complexity is acceptable: `uv run xenon --max-absolute=A geoparquet_io/`
- [ ] Documentation builds (if applicable)

## Open Questions

1. **S2 cell format**: Should we store as token (string) or raw 64-bit integer?
   - **Recommendation**: Token (string) for portability, matching H3 approach

2. **Default level**: What should be the default S2 level?
   - **Recommendation**: Level 13 (~1.2 km² cells), comparable to H3 resolution 9

3. **Covering metadata**: Should we add GeoParquet covering metadata for S2?
   - **Recommendation**: Yes, following H3 pattern with `{"covering": {"s2": {"column": "s2_cell", "level": 13}}}`
