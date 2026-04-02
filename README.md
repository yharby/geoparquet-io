# geoparquet-io

[![Tests](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/geoparquet/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/geoparquet/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

**[Documentation](https://geoparquet.io/)** · **[Quick Start](https://geoparquet.io/getting-started/quickstart/)** · **[Python API](https://geoparquet.io/api/python-api/)**

## Installation

```bash
uv tool install geoparquet-io    # CLI tool (recommended)
uv add geoparquet-io             # Python library
```

See [Installation Guide](https://geoparquet.io/getting-started/installation/) for pip, plugins, and other options.

## Quick Example

```bash
gpio inspect myfile.parquet                      # View metadata
gpio check all myfile.parquet                    # Validate best practices
gpio add bbox input.parquet output.parquet       # Add spatial index
gpio sort hilbert input.parquet sorted.parquet   # Optimize for queries
```

```python
import geoparquet_io as gpio

gpio.read('input.parquet').add_bbox().sort_hilbert().write('output.parquet')
```

See the [User Guide](https://geoparquet.io/guide/inspect/) for spatial indexing, partitioning, cloud storage, and more.

## Features

- **Fast** — PyArrow and DuckDB backend
- **Pipeable** — Unix pipes with Arrow IPC streaming
- **Cloud-native** — S3, GCS, Azure, HTTPS
- **Spatial indices** — bbox, H3, quadkey, S2, A5, KD-tree
- **GeoParquet 1.1 & 2.0** — Best practices enforcement
- **CLI + Python API** — Fluent Table interface

## Links

- [CLI Reference](https://geoparquet.io/cli/overview/)
- [Contributing](https://geoparquet.io/contributing/)
- [PyPI](https://pypi.org/project/geoparquet-io/)
- [Issues](https://github.com/geoparquet/geoparquet-io/issues)

## License

Apache 2.0 — See [LICENSE](LICENSE)
