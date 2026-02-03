# Admin Prefix Feature Design

**Date:** 2025-02-03
**Issue:** #45 - Support multiple admin boundary joins with custom column prefixes

## Problem

Running `add admin-divisions` twice creates confusing duplicate columns. DuckDB auto-renames them (e.g., `admin:country_code_1`), making it unclear which dataset each column came from.

## Solution

Add a `--prefix` option that defaults to the dataset name, preventing conflicts automatically.

## Architecture Changes

### Core Changes to `AdminDataset` Base Class

1. **New Method: `get_default_prefix()`**
   - Returns the dataset's default prefix
   - Default implementation: extracts first word from `get_dataset_name()` and lowercases
   - Subclasses can override for custom behavior
   - Examples: "GAUL L2..." → "gaul", "Overture Admin..." → "overture"

2. **Modified Method: `get_output_column_name(level_name, prefix=None)`**
   - If `prefix` is `None`: calls `get_default_prefix()` and uses underscore format
   - If `prefix == "admin"`: uses colon format (`admin:{level_name}`)
   - Otherwise: uses underscore format with custom prefix (`{prefix}_{level_name}`)

### CLI Integration

**New Option in `cli/main.py`:**
```python
@click.option(
    "--prefix",
    type=str,
    default=None,
    help="Column name prefix. Defaults to dataset name (gaul, overture). "
         "Use 'admin' for admin:level format."
)
```

**Parameter Flow:**
CLI option → `add_admin_divisions_multi()` → `_build_admin_select_clause()` → `dataset.get_output_column_name()`

### Core Function Updates

**Modified in `core/add_admin_divisions_multi.py`:**
1. `_build_admin_select_clause()` - Add `prefix` parameter
2. `add_admin_divisions_multi()` - Add `prefix: str | None = None` parameter

## Behavior

| Scenario | Prefix Value | Output Columns |
|----------|-------------|----------------|
| **Default (GAUL)** | `None` → `"gaul"` | `gaul_country_code`, `gaul_continent`, `gaul_department` |
| **Default (Overture)** | `None` → `"overture"` | `overture_country_code`, `overture_subdivision_code` |
| **Admin format** | `--prefix admin` | `admin:country`, `admin:continent` |
| **Custom** | `--prefix mycustom` | `mycustom_country`, `mycustom_continent` |

## Example Workflow

```bash
# Add GAUL boundaries (default prefix: "gaul")
gpio add admin-divisions buildings.parquet step1.parquet \
  --dataset gaul \
  --levels continent country department
# Creates: gaul_country, gaul_continent, gaul_department

# Add Overture boundaries (default prefix: "overture")
gpio add admin-divisions step1.parquet step2.parquet \
  --dataset overture \
  --levels country region
# Creates: overture_country, overture_region
# NO CONFLICTS!

# Partition by combined levels
gpio partition step2.parquet output/ \
  --levels gaul_continent gaul_country overture_country
```

## Testing Strategy

### Core Functionality Tests
- Default prefix creates dataset-prefixed columns
- Custom prefix creates correctly named columns
- `--prefix admin` creates colon-format columns
- Sequential runs with different prefixes don't conflict
- Verify actual column names in output Parquet

### Dataset Method Tests
- `get_default_prefix()` returns correct value for each dataset
- `get_output_column_name()` with various prefix values
- Special "admin" prefix handling

### Integration Tests
- Multi-dataset workflow: GAUL → Overture → partition
- Verify partition command works with prefixed columns
- Dry-run mode shows correct column names

## Documentation Updates

1. **CLI Help Text** - Clear `--prefix` option description
2. **User Guide** - Multi-dataset workflow example
3. **API Documentation** - Updated function signatures
4. **Docstrings** - Updated examples

## Breaking Changes

**Backward Compatibility Impact:**
This changes the default behavior. Previously, no prefix meant Vecorel-style names. Now it means dataset-name-prefixed columns. This is acceptable since we're pre-1.0.

Users who want the old behavior can use `--prefix admin`.

## Implementation Checklist

- [ ] Add `get_default_prefix()` to `AdminDataset` base class
- [ ] Modify `get_output_column_name()` signature and implementation
- [ ] Update `_build_admin_select_clause()` to pass prefix
- [ ] Update `add_admin_divisions_multi()` signature
- [ ] Add `--prefix` CLI option
- [ ] Write tests for core functionality
- [ ] Write tests for dataset methods
- [ ] Write integration tests
- [ ] Update CLI help text
- [ ] Update user guide
- [ ] Update API documentation
- [ ] Update docstrings
