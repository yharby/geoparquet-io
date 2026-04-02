# geoparquet-io

[![Tests](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/geoparquet/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/geoparquet/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

**📚 [Full Documentation](https://geoparquet.io/)** | **[Quick Start Tutorial](https://geoparquet.io/getting-started/quickstart/)**

## Features

- **Fast**: Built on PyArrow and DuckDB for high-performance operations
- **Pipeable**: Chain commands with Unix pipes using Arrow IPC streaming - no intermediate files
- **Comprehensive**: Sort, extract, partition, enhance, validate, and upload GeoParquet files
- **Cloud-Native**: Read from and write to S3, GCS, Azure, and HTTPS sources
- **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and admin divisions
- **Best Practices**: Automatic optimization following GeoParquet 1.1 and 2.0 specs
- **Parquet Geo Types support**: Read and write Parquet geometry and geography types.
- **Flexible**: CLI and Python API for any workflow
- **Tested**: Extensive test suite across Python 3.10-3.13 and all platforms

## Installation

```bash
pipx install --pip-args='--pre' geoparquet-io     # CLI tool
pip install --pre geoparquet-io                   # Python library
```

See the [Installation Guide](https://geoparquet.io/getting-started/installation/) for more options including uv tool, from source, and requirements.

## Quick Start

```bash
# Inspect file structure and metadata
gpio inspect myfile.parquet

# Check file quality and best practices
gpio check all myfile.parquet

# Add spatial indexing (bbox, h3, quadkey, s2, a5, kdtree)
gpio add bbox input.parquet output.parquet
gpio add h3 input.parquet output.parquet --resolution 8

# Enrich with administrative boundaries
gpio add admin-divisions input.parquet output.parquet --dataset gaul --levels continent,country

# Sort using spatial curves for better compression and query performance
gpio sort hilbert input.parquet output_sorted.parquet   # Hilbert curve
gpio sort quadkey input.parquet output_sorted.parquet   # Quadkey sorting

# Partition by admin boundaries (supports up to 3 hierarchical levels)
gpio partition admin buildings.parquet output_dir/ --dataset gaul --levels continent,country,department

# Remote-to-remote processing (S3, GCS, Azure, HTTPS)
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --aws-profile my-aws
gpio partition h3 gs://bucket/data.parquet gs://bucket/partitions/ --resolution 9
gpio sort hilbert https://example.com/data.parquet s3://bucket/sorted.parquet

# Chain commands with Unix pipes - no intermediate files needed
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | gpio add bbox - | gpio sort hilbert - output.parquet
```

For complete command documentation including all spatial indexing options, see the [CLI Reference](https://geoparquet.io/cli/add/) and [User Guide](https://geoparquet.io/guide/inspect/).

## Python API

Use gpio programmatically for the best performance:

```python
import geoparquet_io as gpio

# Read, transform, and write in a fluent chain
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')

# Convert from other formats (Shapefile, GeoJSON, GeoPackage, CSV)
gpio.convert('data.gpkg') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=5)

# Upload to cloud storage
gpio.read('data.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .upload('s3://bucket/filtered.parquet')
```

The Python API keeps data in memory as Arrow tables, providing significant performance improvements over file-based CLI operations. Benchmarks show 78% faster execution on a 75MB test file (400K rows) compared to the file-based CLI approach. See the [Python API documentation](https://geoparquet.io/api/python-api/) and [Performance Benchmarks](https://geoparquet.io/guide/benchmarks/) for details.

## Plugins

gpio supports plugins that add specialized format support. Plugins are installed alongside the main tool:

```bash
# Install gpio with PMTiles support
uv tool install geoparquet-io --with gpio-pmtiles
pipx install geoparquet-io --preinstall gpio-pmtiles

# Or add to existing installation
uv tool install --with gpio-pmtiles geoparquet-io
pipx inject geoparquet-io gpio-pmtiles
```

### Available Plugins

- **[gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)** - Convert between GeoParquet and PMTiles format for efficient web map tiles

## LLM Integration

gpio includes a skill file that teaches LLMs (ChatGPT, Claude, Gemini, etc.) how to work with spatial data using gpio.

```bash
# List available skills
gpio skills

# Print skill content (pipe to clipboard or paste into chat)
gpio skills --show

# Copy skill to current directory for customization
gpio skills --copy .
```

The skill teaches LLMs how to:
- Convert spatial data to optimized GeoParquet
- Validate files against best practices
- Recommend partitioning strategies based on data size
- Publish to cloud storage

### Claude Code

For [Claude Code](https://claude.ai/code) users, invoke the skill via `/geoparquet` or ask Claude to help with GeoParquet conversions.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](docs/contributing.md) for development setup, coding standards, and how to submit changes.

## Links

- **Documentation**: [https://geoparquet.io/](https://geoparquet.io/)
- **PyPI**: [https://pypi.org/project/geoparquet-io/](https://pypi.org/project/geoparquet-io/)
- **Issues**: [https://github.com/geoparquet/geoparquet-io/issues](https://github.com/geoparquet/geoparquet-io/issues)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
