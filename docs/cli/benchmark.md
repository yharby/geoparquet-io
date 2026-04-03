# benchmark Command

Benchmark GeoParquet conversion and operation performance.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `compare` | Compare converter performance on a single file |
| `suite` | Run comprehensive benchmark suite |
| `report` | View and compare benchmark results |

## gpio benchmark compare

Compare converter performance on a single file.

### Quick Reference

```bash
gpio benchmark compare INPUT_FILE [OPTIONS]
```

### Available Converters

| Converter | Description | Install |
|-----------|-------------|---------|
| `duckdb` | DuckDB spatial extension | Always available |
| `geopandas_fiona` | GeoPandas with Fiona engine | `geopandas`, `fiona` |
| `geopandas_pyogrio` | GeoPandas with PyOGRIO engine | `geopandas`, `pyogrio` |
| `gdal_ogr2ogr` | GDAL ogr2ogr CLI | System GDAL installation |

Install all optional converters:

```bash
uv pip install geoparquet-io[benchmark]
# or: pip install geoparquet-io[benchmark]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--iterations N` | 3 | Number of iterations per converter |
| `--converters LIST` | all available | Comma-separated list of converters to run |
| `--output-json PATH` | - | Save results to JSON file |
| `--keep-output DIR` | temp (cleaned up) | Directory to save converted files |
| `--warmup/--no-warmup` | enabled | Run warmup iteration before timing |
| `--format table\|json` | table | Output format |
| `--quiet` | - | Suppress progress output |

### Examples

```bash
# Basic comparison
gpio benchmark compare input.geojson

# Specific converters with more iterations
gpio benchmark compare input.shp --converters duckdb,geopandas_pyogrio --iterations 5

# Save results and keep output files
gpio benchmark compare input.gpkg --output-json results.json --keep-output ./output

# JSON output
gpio benchmark compare input.geojson --format json
```

### Output

#### Table Format (default)

```
======================================================================
BENCHMARK RESULTS
======================================================================

File: ARG.geojson
  Format: .geojson
  Features: 3,486,802
  Size: 1120.15 MB
  Geometry: LINESTRING

Converter                 Time (s)           Memory (MB)
-------------------------------------------------------------
DuckDB                    29.751 +/- 0.443   0.0 +/- 0.0
GeoPandas (PyOGRIO)       59.957 +/- 1.078   1196.7 +/- 0.0

Fastest: DuckDB (29.751s)
```

## gpio benchmark suite

Run comprehensive benchmark suite across multiple operations.

```bash
gpio benchmark suite [OPTIONS]
```

Runs a configurable suite of gpio operations (convert, add, sort, partition) on test files and generates detailed reports.

## gpio benchmark report

View and compare benchmark results from previous runs.

```bash
gpio benchmark report [OPTIONS] [RESULT_FILES]...
```

### Examples

```bash
# View single result file
gpio benchmark report results.json

# Compare multiple runs
gpio benchmark report results/*.json
```

## Interpreting Results

- **Time**: Mean elapsed seconds +/- standard deviation
- **Memory**: Peak memory usage in MB (Python tracemalloc for in-process, psutil for external)
- DuckDB shows 0 MB because it manages memory outside Python's allocator

## See Also

- [Convert Guide - Performance](../guide/convert.md#performance) - Summary benchmark results
