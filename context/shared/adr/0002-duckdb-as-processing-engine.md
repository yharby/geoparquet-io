# ADR-0002: DuckDB as Processing Engine

## Status

Accepted

## Context

geoparquet-io needs to process GeoParquet files that can range from a few megabytes to many gigabytes. Operations include spatial queries, column transformations, partitioning, sorting, and format conversion. The processing engine must:

1. Handle files larger than available RAM.
2. Support spatial operations (geometry parsing, bounding box calculation, spatial indexing).
3. Provide a productive query interface for complex data transformations.
4. Work with the Parquet format natively.

## Decision

Use DuckDB as the primary data processing engine, with its spatial extension for geometry operations and httpfs extension for remote file access (S3, HTTPS).

All core modules obtain a DuckDB connection through the shared helper `get_duckdb_connection()` in `core/common.py`, which manages extension loading:

```python
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs

con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_path))
```

SQL is the primary interface for data transformation. Complex operations are expressed as SQL queries executed against DuckDB, which handles query planning, memory management, and parallel execution internally.

## Consequences

### Positive
- SQL provides a concise, readable interface for complex data transformations.
- DuckDB processes data out-of-core, handling files much larger than available memory.
- The spatial extension provides geometry operations (ST_AsWKB, ST_Envelope, ST_GeomFromWKB, etc.) without external dependencies.
- Native Parquet support means no intermediate format conversions for read/write.
- The httpfs extension enables direct read/write to S3 and HTTPS URLs.

### Negative
- DuckDB's SQL dialect has minor differences from standard SQL that contributors must learn.
- On Windows, DuckDB holds file locks that require explicit connection cleanup. Temp files must use UUIDs to avoid collisions.
- DuckDB is an in-process database, so crashes in DuckDB can crash the host process.
- Spatial extension functionality may lag behind dedicated spatial libraries like GEOS/Shapely.

### Neutral
- PyArrow is still used alongside DuckDB for metadata operations, schema inspection, and certain read/write patterns. The two libraries complement each other.
- DuckDB version upgrades occasionally introduce breaking changes in SQL syntax or extension behavior, requiring testing across the full CLI surface.

## Alternatives Considered

### GeoPandas
Using GeoPandas with Shapely for spatial operations. Rejected because GeoPandas loads entire datasets into memory, making it unsuitable for large files. Its row-by-row geometry processing is also significantly slower than DuckDB's vectorized SQL execution.

### Raw PyArrow
Using PyArrow directly for all data processing. Rejected because PyArrow lacks built-in spatial operations, requiring manual WKB parsing. Complex transformations expressed as PyArrow compute expressions are verbose and harder to read than equivalent SQL.

### PostGIS (external database)
Using PostgreSQL/PostGIS for spatial processing. Rejected because it requires an external database server, adding deployment complexity. DuckDB's in-process architecture is simpler for a CLI tool.

## References

- `geoparquet_io/core/common.py` -- `get_duckdb_connection()` and `needs_httpfs()` helpers
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial.html)
- [DuckDB HTTPFS Extension](https://duckdb.org/docs/extensions/httpfs.html)
