# inspect meta Command

Show comprehensive metadata from GeoParquet files.

For detailed usage and examples, see the [Viewing Metadata Guide](../guide/meta.md).

## Quick Reference

```bash
gpio inspect meta --help
```

## Usage

```bash
gpio inspect meta [OPTIONS] PARQUET_FILE
```

## Options

| Option | Description |
|--------|-------------|
| `--geo` | Show only GeoParquet 'geo' metadata |
| `--parquet` | Show only Parquet file metadata |
| `--parquet-geo` | Show only Parquet geospatial metadata |
| `--row-groups INTEGER` | Number of row groups to display (default: 1) |
| `--json` | Output as JSON for scripting |
| `-v, --verbose` | Print verbose output |
| `--help` | Show help message |

## Examples

```bash
# All metadata
gpio inspect meta data.parquet

# GeoParquet 'geo' key only
gpio inspect meta data.parquet --geo

# Parquet file metadata only
gpio inspect meta data.parquet --parquet

# Show 5 row groups
gpio inspect meta data.parquet --row-groups 5

# JSON output for scripting
gpio inspect meta data.parquet --geo --json

# Verbose output
gpio inspect meta data.parquet -v
```

## Output Description

### Default Output

Shows a combined view of all metadata types.

### `--geo` Output

Shows the GeoParquet metadata stored in the 'geo' key:

- Primary geometry column
- Geometry types (Point, Polygon, etc.)
- CRS information
- Bounding box
- Encoding format

### `--parquet` Output

Shows Parquet-level file metadata:

- Creator information
- Row count
- Row group count
- Schema with column types
- Compression codec

### `--parquet-geo` Output

Shows geospatial metadata from the Parquet footer (GeoParquet 1.1):

- GEOMETRY/GEOGRAPHY logical types
- Per-column bounding boxes
- Geospatial statistics
