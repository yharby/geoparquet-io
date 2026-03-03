# publish stac Command

Generate STAC Item or Collection from GeoParquet files.

For detailed usage and examples, see the [STAC Generation Guide](../guide/stac.md).

## Quick Reference

```bash
gpio publish stac --help
```

## Usage

```bash
gpio publish stac [OPTIONS] INPUT OUTPUT
```

## Options

| Option | Description | Required |
|--------|-------------|----------|
| `--bucket TEXT` | S3 bucket prefix for asset hrefs | Yes |
| `--public-url TEXT` | Public HTTPS URL for assets | No |
| `--collection-id TEXT` | Custom collection ID (partitioned datasets) | No |
| `--item-id TEXT` | Custom item ID (single files) | No |
| `--overwrite` | Overwrite existing STAC files | No |
| `-v, --verbose` | Print verbose output | No |
| `--help` | Show help message | No |

## Output Modes

### Single File

Generates a STAC Item JSON:

```bash
gpio publish stac input.parquet output.json --bucket s3://bucket/path/
```

### Partitioned Directory

Generates a STAC Collection + Items (Items co-located with data):

```bash
gpio publish stac partitions/ . --bucket s3://bucket/dataset/
```

## Examples

```bash
# Single file to STAC Item
gpio publish stac data.parquet output.json --bucket s3://my-bucket/roads/

# Partitioned dataset to STAC Collection
gpio publish stac partitions/ . --bucket s3://my-bucket/dataset/

# With public URL mapping
gpio publish stac data.parquet output.json \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/

# Custom collection ID
gpio publish stac partitions/ . \
  --bucket s3://bucket/dataset/ \
  --collection-id my-custom-collection

# Custom item ID
gpio publish stac data.parquet output.json \
  --bucket s3://bucket/data/ \
  --item-id my-custom-item

# Overwrite existing
gpio publish stac data.parquet output.json \
  --bucket s3://bucket/data/ \
  --overwrite
```

## PMTiles Support

Automatically detects and includes PMTiles overview files as assets when present alongside parquet files.
