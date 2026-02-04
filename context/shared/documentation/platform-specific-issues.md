# Platform-Specific Issues

This document catalogs known platform-specific issues and their solutions. These are recurring problems that may surface again in similar contexts.

## Windows File Locking with PyArrow

### Problem

On Windows, when `pq.ParquetFile(path)` throws an exception (e.g., invalid Parquet file), the file handle may not be released immediately. Python's garbage collector doesn't clean up the `ParquetFile` object right away, which keeps the file locked.

This causes `PermissionError: [WinError 32]` when attempting to delete the file:

```
PermissionError: [WinError 32] The process cannot access the file because it
is being used by another process: 'C:\...\tmp123.parquet'
```

### Solution

Always use a `finally` block to explicitly delete the `ParquetFile` object:

```python
def read_parquet_metadata(parquet_file: str) -> dict:
    pf = None  # Initialize to None
    try:
        pf = pq.ParquetFile(parquet_file)
        return dict(pf.metadata.metadata) if pf.metadata.metadata else {}
    except Exception as e:
        raise SomeError(f"Cannot read: {parquet_file}") from e
    finally:
        # Explicitly delete to release file handle (Windows compatibility)
        del pf
```

### Files Affected

- `geoparquet_io/core/duckdb_metadata.py`:
  - `_pyarrow_get_kv_metadata()`
  - `_pyarrow_get_geo_metadata()`
  - `_pyarrow_get_schema_info()`

### References

- PR #233 (inspect metadata performance)
- Windows CI failures in `test_duckdb_metadata.py::TestGeoParquetErrorExceptions`

---

## GeoArrow-PyArrow CRS Deserialization

### Problem

When `geoarrow-pyarrow` is imported, it registers custom PyArrow extension types that change how geometry metadata is exposed. Instead of CRS being available in `field.metadata['ARROW:extension:metadata']`, the library "consumes" this metadata and exposes it through `field.type.crs`.

This causes CRS extraction to fail silently (returning `None`) if code only checks `field.metadata`.

### Solution

Check for CRS in both locations, with `field.type.crs` taking priority (since geoarrow-pyarrow consumes the metadata):

```python
def get_crs_from_field(field) -> dict | None:
    # Case 1: geoarrow-pyarrow is imported - CRS is in field.type.crs
    if hasattr(field.type, "crs") and field.type.crs is not None:
        crs_obj = field.type.crs
        if hasattr(crs_obj, "to_json_dict"):
            return crs_obj.to_json_dict()

    # Case 2: Standard Arrow - CRS is in extension metadata
    if hasattr(field.type, "extension_metadata") and field.type.extension_metadata:
        try:
            ext_meta = json.loads(field.type.extension_metadata)
            if "crs" in ext_meta:
                return ext_meta["crs"]
        except (json.JSONDecodeError, KeyError):
            pass

    return None
```

### Additional Consideration: SRID-format CRS

`geoarrow-pyarrow` cannot deserialize certain CRS formats like `srid:5070`. When this happens, it raises:

```
ValueError: Can't create geoarrow.types.Crs from 5070
```

**Solution**: Catch this specific error and fall back to DuckDB:

```python
except ValueError as e:
    if "Can't create geoarrow.types.Crs" in str(e):
        return None  # Fall back to DuckDB
    raise
```

### Files Affected

- `geoparquet_io/core/duckdb_metadata.py`:
  - `_get_pyarrow_logical_type()`
  - `_pyarrow_get_schema_info()`

### References

- PR #233 (inspect metadata performance)
- Failing CI tests for CRS extraction
- Issue with shift from PyArrow to DuckDB for metadata reading

---

## DuckDB Connection Cleanup on Windows

### Problem

On Windows, DuckDB connections must be explicitly closed before temporary files can be deleted. Unlike Unix where files can be deleted while open, Windows enforces strict file locking.

### Solution

Always use try/finally or context managers for DuckDB connections:

```python
con = duckdb.connect()
try:
    # ... use connection
finally:
    con.close()
```

Or use UUID in temporary filenames to avoid collisions:

```python
temp_file = f"/tmp/geoparquet_{uuid.uuid4().hex}.parquet"
```

### References

- Multiple tests throughout the codebase
- CLAUDE.md mentions this under "Debugging"
