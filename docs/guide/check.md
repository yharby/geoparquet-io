# Checking Best Practices

The `check` commands validate GeoParquet files against [best practices](https://github.com/opengeospatial/geoparquet/pull/254/files).

## Run All Checks

=== "CLI"

    ```bash
    gpio check all myfile.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('myfile.parquet')
    result = table.check()

    if result.passed():
        print("All checks passed!")
    else:
        for failure in result.failures():
            print(f"Failed: {failure}")

    # Get full results as dictionary
    details = result.to_dict()
    ```

Runs all validation checks:

- Spatial ordering
- Compression settings
- Bbox structure and metadata
- Row group optimization

## Individual Checks

### Spatial Ordering

=== "CLI"

    ```bash
    gpio check spatial myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_spatial()
    print(f"Spatially ordered: {result.passed()}")
    ```

Checks if data is spatially ordered. Spatially ordered data improves:

- Query performance (10-100x faster for spatial queries)
- Compression ratios
- Cloud access patterns

**Method Selection:**

- **GeoParquet 2.0+ files** (with bbox column): Uses fast bbox-stats method by analyzing row group metadata (~10-100x faster)
- **GeoParquet 1.x files** (no bbox column): Falls back to sampling method which analyzes actual geometry data

!!! tip "For faster spatial order checks"
    Add a bbox column to your file with `gpio add bbox` to enable the fast bbox-stats method.

**How it works:**

- **Bbox-stats method**: Checks if consecutive row groups have overlapping bounding boxes. Non-overlapping row groups indicate good spatial ordering. Passes if < 30% of row group pairs overlap.
- **Sampling method**: Compares average distance between consecutive features vs random feature pairs. Lower ratio indicates better spatial clustering. Passes if ratio < 0.5.

### Compression

=== "CLI"

    ```bash
    gpio check compression myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_compression()
    print(f"Compression optimal: {result.passed()}")
    ```

Validates geometry column compression settings.

### Bbox Structure

=== "CLI"

    ```bash
    gpio check bbox myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_bbox()
    if not result.passed():
        # Add bbox if missing
        table = table.add_bbox().add_bbox_metadata()
    ```

Verifies:

- Bbox column structure
- GeoParquet metadata version
- Bbox covering metadata

### Row Groups

=== "CLI"

    ```bash
    gpio check row-group myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_row_groups()
    for rec in result.recommendations():
        print(rec)
    ```

Checks row group size optimization for cloud-native access.

### STAC Validation

=== "CLI"

    ```bash
    gpio check stac output.json
    ```

=== "Python"

    ```python
    from geoparquet_io import validate_stac

    result = validate_stac('output.json')
    if result.passed():
        print("Valid STAC!")
    ```

Validates STAC Item or Collection JSON:

- STAC spec compliance
- Required fields
- Asset href resolution (local files)
- Best practices

## Options

=== "CLI"

    ```bash
    # Verbose output with details
    gpio check all myfile.parquet --verbose

    # Custom sampling for spatial check
    gpio check spatial myfile.parquet --random-sample-size 200 --limit-rows 1000000
    ```

=== "Python"

    ```python
    # Custom sampling for spatial check
    result = table.check_spatial(sample_size=200, limit_rows=1000000)
    ```

## Checking Partitioned Data

When checking a directory containing partitioned data, you can control how many files are checked:

```bash
# By default, checks only the first file
gpio check all partitions/
# Output: Checking first file (of 4 total). Use --check-all or --check-sample N for more.

# Check all files in the partition
gpio check all partitions/ --check-all

# Check a sample of files (first N files)
gpio check all partitions/ --check-sample 3
```

!!! note "--fix not available for partitions"
    The `--fix` option only works with single files. To fix issues in partitioned data, first consolidate with `gpio extract`, apply fixes, then re-partition if needed.

## See Also

- [CLI Reference: check](../cli/check.md)
- [add command](add.md) - Add spatial indices
- [sort command](sort.md)
