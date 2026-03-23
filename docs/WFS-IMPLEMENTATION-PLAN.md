# WFS Extractor Implementation Plan

**Date:** 2026-03-23
**Status:** Proposed

---

## Context

Add WFS (Web Feature Service) extraction to geoparquet-io, enabling users to download geospatial data from WFS servers directly to optimized GeoParquet files. This follows the successful patterns established by ArcGIS and BigQuery extractors while keeping the initial implementation lean (YAGNI).

**Why:** WFS is a widely-used OGC standard for serving vector geospatial data over HTTP. Many government agencies, municipalities, and organizations publish data via WFS, making it a valuable addition alongside ArcGIS REST and BigQuery support.

**Target Use Case:** Users need to extract data from public WFS servers (e.g., USGS, state GIS portals, municipal data services) with automatic pagination, intelligent spatial filtering, and memory-efficient streaming to GeoParquet.

---

## Design Decisions (from Brainstorming)

These decisions keep the initial implementation focused and practical:

1. **WFS Version**: Support WFS 1.0.0/1.1.0 (widely compatible, simpler GetFeature API)
2. **Authentication**: Public WFS only for v1 (defer auth to separate issue)
3. **Bbox Filtering**: Reuse BigQuery's `--bbox-mode auto|server|local` pattern with row count thresholds
4. **WHERE Filtering**: Bbox only initially (defer CQL/FES translation to separate issue)
5. **Pagination**: Auto-detect capabilities, page automatically with progress tracking (like ArcGIS)
6. **Geometry Formats**: Auto-detect best format (prefer GeoJSON for speed, fall back to GML for compatibility)
7. **CRS Handling**: Auto-negotiate (try EPSG:4326 first, accept server default), optional `--output-crs` for explicit control
8. **Typename/Layer**: Single typename argument with namespace auto-resolution, list layers if typename omitted

---

## Implementation Overview

### Architecture Pattern (Proven from ArcGIS)

**Two-pass streaming approach:**
1. **Pass 1**: Stream WFS pages → temp Parquet file (memory-efficient, constant RAM regardless of dataset size)
2. **Pass 2**: Read temp Parquet → Arrow table → apply post-processing (Hilbert, bbox column)

This pattern handles datasets larger than available memory while producing complete Arrow tables.

### Key Files to Create/Modify

1. **`geoparquet_io/core/wfs.py`** (~1000-1200 lines) - Core WFS logic
2. **`geoparquet_io/cli/main.py`** - Add `@extract.command(name="wfs")` command
3. **`geoparquet_io/api/table.py`** - Add `Table.from_wfs()` method
4. **`geoparquet_io/api/ops.py`** - Add `ops.from_wfs()` function
5. **`tests/test_wfs.py`** (~800-1000 lines) - Comprehensive tests
6. **`docs/guide/extract.md`** - Update with WFS examples
7. **`docs/api/python-api.md`** - Document Python API
8. **`pyproject.toml`** - Add `owslib>=0.29.0` dependency

---

## Core Module Implementation (`geoparquet_io/core/wfs.py`)

### 1. Data Structures

```python
@dataclass
class WFSLayerInfo:
    """WFS layer/feature type metadata."""
    typename: str
    title: str | None
    crs_list: list[str]
    default_crs: str | None
    bbox: tuple[float, float, float, float] | None
    geometry_column: str  # Detected from DescribeFeatureType
```

### 2. HTTP Client (Reuse ArcGIS Pattern)

- Module-level `httpx.Client` with connection pooling
- Retry logic for transient errors (timeouts, network failures)
- Error handling for HTTP status codes (401, 403, 404, 5xx)

**Reuse from ArcGIS:**
- `_get_http_client()` - Connection pooling pattern
- `_make_request()` - Retry logic with exponential backoff

### 3. Capability Parsing (OWSLib Integration)

```python
def get_wfs_capabilities(service_url, version="1.1.0") -> WFSCapabilities
def get_layer_info(service_url, typename, version) -> WFSLayerInfo
def list_available_layers(service_url, version) -> None  # Prints to console
```

**OWSLib Usage:**
- `WebFeatureService(url, version)` for capabilities parsing
- `wfs.contents` for available layers
- `wfs.get_schema(typename)` for DescribeFeatureType (XSD → geometry column detection)

### 4. Bbox Filtering (Reuse BigQuery Pattern)

```python
def _determine_bbox_strategy(capabilities, layer_info, bbox_mode, bbox_threshold) -> bool
def _build_bbox_filter_wfs(bbox, use_server_side, geometry_column) -> tuple[str | None, str | None]
```

**Server-side:** WFS bbox parameter: `"xmin,ymin,xmax,ymax"`
**Local:** DuckDB ST_Intersects with WKT polygon

**Key difference from BigQuery:** WFS doesn't expose row counts easily, so auto mode defaults to server-side (conservative for remote services).

### 5. Pagination Logic

```python
def fetch_features_page(service_url, typename, version, offset, page_size, ...) -> dict | str
def fetch_all_features(..., max_workers: int = 1) -> Generator[dict | str, None, None]
```

**Pagination parameters:**
- WFS 1.1.0: `startIndex` + `maxFeatures`
- Progress tracking: "Fetching features 1-1000 of ??? ..." (try `resultType=hits` for count)

**Parallel fetching (reuse ArcGIS pattern):**
- `max_workers` parameter: 1 = sequential (default), 2-3 recommended for speed
- Uses `ThreadPoolExecutor` to submit N requests simultaneously
- Collects and sorts results by offset to maintain ordering
- Yields pages sequentially for streaming to Parquet
- Fail-fast on errors (propagate first failure)

**Reuse from ArcGIS:**
- Generator pattern yielding pages
- Progress reporting with `progress()` logging helper
- `ThreadPoolExecutor` parallel fetching pattern (lines 640-714 in arcgis.py)

### 6. Geometry Parsing (Multi-Format)

```python
def _detect_best_output_format(available_formats) -> str
def _geojson_to_arrow_table(features, geometry_column) -> pa.Table
def _gml_to_arrow_table(gml_text, typename, geometry_column) -> pa.Table
```

**Format preference order:**
1. GeoJSON (`application/json`, `json`, `geojson`) - fastest to parse
2. GML3 (`gml3`, `text/xml; subtype=gml/3.1.1`)
3. GML2 (`gml2`)
4. Fallback to first available

**Parsing strategy (DuckDB via GDAL):**
- Write temp GeoJSON/GML file
- Use `ST_Read(temp_file)` to parse
- Convert geometry: `ST_AsWKB(geometry)` → PyArrow binary column

**Reuse from ArcGIS:**
- Same DuckDB-based conversion pattern
- Same temp file cleanup in finally blocks

### 7. CRS Negotiation

```python
def _negotiate_crs(layer_info, output_crs=None) -> str
```

**Strategy:**
1. If `--output-crs` specified and supported → use it
2. Try EPSG:4326 variants (most universal)
3. Fall back to server default
4. Add warning if CRS unavailable

**CRS format variations to handle:**
- `EPSG:4326`
- `urn:ogc:def:crs:EPSG::4326`
- `http://www.opengis.net/def/crs/EPSG/0/4326`

### 8. Streaming to Parquet (Reuse ArcGIS Pattern)

```python
def _stream_features_to_parquet(service_url, typename, ..., output_path) -> int
```

**Pattern:**
- Initialize `ParquetWriter` with schema from first page
- Stream each page: parse → Arrow table → cast to fixed schema → write
- Track `total_rows` and `page_count`
- Clean up writer in finally block

**Reuse from ArcGIS:**
- Fixed schema pattern (prevents type mismatches between pages)
- Schema casting with `safe=True` and helpful error messages

### 9. High-Level Functions

```python
def wfs_to_table(service_url, typename, bbox=None, ...) -> pa.Table
def convert_wfs_to_geoparquet(service_url, typename, output_file, ...) -> None
```

**`wfs_to_table()` flow:**
1. Get capabilities and layer info
2. Determine bbox strategy
3. Negotiate CRS
4. Detect best output format
5. Stream to temp Parquet
6. Read temp Parquet as Arrow table
7. Apply local bbox filter if needed
8. Add CRS metadata to schema

**`convert_wfs_to_geoparquet()` flow:**
1. Call `wfs_to_table()`
2. Apply Hilbert ordering (unless `--skip-hilbert`)
3. Add bbox column (unless `--skip-bbox`)
4. Write to output with compression

---

## CLI Command (`geoparquet_io/cli/main.py`)

### Command Signature

```bash
gpio extract wfs <service_url> <typename> <output_file> [OPTIONS]
```

### Key Options

```python
@click.option("--version", default="1.1.0", type=click.Choice(["1.0.0", "1.1.0"]))
@click.option("--bbox", help="Bounding box: xmin,ymin,xmax,ymax in WGS84")
@click.option("--bbox-mode", type=click.Choice(["auto", "server", "local"]), default="auto")
@click.option("--bbox-threshold", type=int, default=10000)
@click.option("--limit", type=int, help="Max features to extract")
@click.option("--output-crs", help="Request specific CRS (e.g., EPSG:4326)")
@click.option("--batch-size", type=int, default=1000, help="Page size")
@click.option("--max-workers", type=int, default=1, help="Concurrent requests (1=sequential, 2-3 recommended)")
@click.option("--skip-hilbert", is_flag=True)
@click.option("--skip-bbox", is_flag=True)
@compression_options  # Standard decorator
@row_group_options    # Standard decorator
@verbose_option       # Standard decorator
```

### Special Behavior

**List layers mode:** If typename not provided, show available layers:

```bash
$ gpio extract wfs https://geo.example.com/wfs

Available layers (3):
  - cities (US Cities)
  - roads (Road Network)
  - parcels (Property Parcels)

Use: gpio extract wfs https://geo.example.com/wfs <typename> output.parquet
```

### Examples in Docstring

```python
"""
Extract WFS (Web Feature Service) to GeoParquet.

Examples:

    # Extract entire layer
    gpio extract wfs https://geo.example.com/wfs cities output.parquet

    # With bbox filter (server-side)
    gpio extract wfs https://geo.example.com/wfs roads output.parquet \
        --bbox -122.5,37.5,-122.0,38.0

    # Limit features and specify CRS
    gpio extract wfs https://geo.example.com/wfs parcels output.parquet \
        --limit 10000 --output-crs EPSG:3857

    # List available layers
    gpio extract wfs https://geo.example.com/wfs
"""
```

---

## Python API

### Functional API (`geoparquet_io/api/ops.py`)

```python
def from_wfs(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    max_workers: int = 1,
) -> pa.Table:
    """Fetch WFS layer as PyArrow Table."""
```

### Chainable API (`geoparquet_io/api/table.py`)

```python
@classmethod
def from_wfs(
    cls,
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
) -> Table:
    """Create Table from WFS layer."""
```

**Example:**

```python
from geoparquet_io.api import Table

Table.from_wfs('https://geo.example.com/wfs', 'cities') \
    .add_bbox() \
    .sort_hilbert() \
    .write('cities.parquet')
```

---

## Testing Strategy (`tests/test_wfs.py`)

### Test Structure (Following ArcGIS Pattern)

1. **Unit Tests (Fast, No Network)**
   - `TestBboxStrategy` - Bbox mode logic (auto/server/local)
   - `TestBboxFilters` - Filter construction (WFS param vs DuckDB SQL)
   - `TestFormatDetection` - Output format preference logic
   - `TestCRSNegotiation` - CRS selection logic
   - `TestNamespaceResolution` - Typename matching with namespaces

2. **Mock-Based Tests**
   - Mock `_make_request()` at lowest level
   - Use realistic XML/JSON responses
   - Test capability parsing, pagination, error handling

3. **Integration Tests** (Marked `@pytest.mark.network`)
   - Test against real public WFS (e.g., USGS Protected Areas)
   - Test pagination end-to-end
   - Test bbox filtering (server vs local modes)
   - Mark as `@pytest.mark.slow`

### Mock Response Fixtures

```python
MOCK_CAPABILITIES_XML = """<?xml version="1.0"?>..."""
MOCK_GEOJSON_RESPONSE = {"type": "FeatureCollection", "features": [...]}
MOCK_GML_RESPONSE = """<?xml version="1.0"?>..."""
```

### Edge Cases to Test

- Empty result sets
- Missing geometry columns
- Unsupported CRS requests
- Server pagination limits
- Type mismatches between pages
- GeoJSON vs GML parsing
- Namespace variations (`topp:states` vs `states`)

---

## Documentation Updates

### 1. `docs/guide/extract.md`

Add new section: **"Extracting from WFS Services"**

Include:
- Basic usage with CLI examples
- Bbox filtering examples
- CRS negotiation explanation
- Pagination behavior
- Common public WFS services (USGS, state portals)

### 2. `docs/api/python-api.md`

Add:
- `Table.from_wfs()` method documentation
- `ops.from_wfs()` function documentation
- Python code examples with and without bbox filtering

### 3. `CLAUDE.md` (Project Instructions)

Update CLI command table to include:
```
gpio extract wfs    | Extract from WFS services
```

---

## Dependencies

### Add to `pyproject.toml`

```toml
[project]
dependencies = [
    ...
    "owslib>=0.29.0",  # OGC web services (WFS, WMS, CSW)
]
```

**Why OWSLib:**
- Standard library for OGC services (maintained by OSGeo)
- Handles GetCapabilities XML parsing
- Handles DescribeFeatureType XSD parsing
- Provides CRS negotiation helpers
- Battle-tested with diverse WFS implementations

---

## Follow-Up Issues (Deferred)

Create these GitHub issues after implementation:

### Issue 1: WFS Authentication Support
- Basic HTTP Auth (username/password)
- API key support (query param or header)
- Token-based auth (like ArcGIS)
- Rationale: Requires access to private WFS servers for testing

### Issue 2: Server-Side Attribute Filtering
- Translate SQL WHERE clauses to CQL_FILTER
- Use OWSLib's `fes` module for OGC Filter Encoding
- Support basic operators: `=`, `>`, `<`, `LIKE`, `IN`, `AND`, `OR`
- Rationale: Complex feature, needs careful SQL→CQL parser design

### Issue 3: WFS 2.0 Support
- Support WFS 2.0 specific features (improved paging, filter encoding)
- Test against modern WFS 2.0 servers
- Rationale: WFS 1.x covers vast majority of existing services

---

## Critical Files Reference

Study these files for implementation patterns:

1. **`geoparquet_io/core/arcgis.py`** (1227 lines)
   - HTTP client setup and retry logic (lines 80-214)
   - Pagination with `fetch_all_features()` generator (lines 543-716)
   - Streaming pattern `_stream_features_to_parquet()` (lines 846-956)
   - GeoJSON→Arrow conversion via DuckDB (lines 794-844)

2. **`geoparquet_io/core/extract_bigquery.py`** (935 lines)
   - Bbox strategy logic `_determine_bbox_strategy()` (lines 624-658)
   - Filter construction `_build_bbox_filters()` (lines 659-687)
   - Auto mode row count threshold pattern

3. **`geoparquet_io/core/common.py`** (4036 lines)
   - Reusable utilities: DuckDB connection, CRS parsing, file handling
   - `get_duckdb_connection()` - Essential for geometry parsing
   - `parse_crs_string_to_projjson()` - CRS metadata conversion

4. **`geoparquet_io/cli/main.py`** (5400 lines)
   - ArcGIS command pattern (lines 2310-2512)
   - BigQuery command pattern (lines 2515-2674)
   - Decorator usage for consistent CLI options

5. **`tests/test_arcgis.py`**
   - Mock-based unit tests
   - Network integration test patterns
   - Edge case coverage (empty results, auth failures, pagination)

---

## Verification Steps

After implementation, verify end-to-end:

1. **Basic Extraction (Public WFS)**
   ```bash
   gpio extract wfs https://gis1.usgs.gov/arcgis/services/padus3_0/MapServer/WFSServer \
       padus3_0:PADUS3_0Combined_Proclamation_Marine \
       usgs_test.parquet --limit 100

   # Verify output
   gpio inspect meta usgs_test.parquet
   gpio inspect head usgs_test.parquet
   ```

2. **Bbox Filtering (Server-Side)**
   ```bash
   gpio extract wfs https://gis1.usgs.gov/arcgis/services/padus3_0/MapServer/WFSServer \
       padus3_0:PADUS3_0Combined_Proclamation_Marine \
       bbox_test.parquet --bbox -122.5,37.5,-122.0,38.0 --bbox-mode server
   ```

3. **List Layers**
   ```bash
   gpio extract wfs https://gis1.usgs.gov/arcgis/services/padus3_0/MapServer/WFSServer
   # Should show available layers with descriptions
   ```

4. **Python API**
   ```python
   import geoparquet_io as gpio

   table = gpio.Table.from_wfs(
       'https://gis1.usgs.gov/arcgis/services/padus3_0/MapServer/WFSServer',
       'padus3_0:PADUS3_0Combined_Proclamation_Marine',
       limit=100
   )
   table.add_bbox().sort_hilbert().write('output.parquet')
   ```

5. **Run Tests**
   ```bash
   # Fast tests only (unit + mocked)
   uv run pytest tests/test_wfs.py -m "not network and not slow" -v

   # All tests (including network)
   uv run pytest tests/test_wfs.py -v

   # With coverage
   uv run pytest tests/test_wfs.py --cov=geoparquet_io.core.wfs --cov-report=term-missing
   ```

6. **Documentation Check**
   - Verify examples work as documented
   - Check CLI help text: `gpio extract wfs --help`
   - Test error messages are helpful (bad URL, missing typename, etc.)

---

## Implementation Checklist

### Core Module
- [ ] Create `geoparquet_io/core/wfs.py`
- [ ] Data structures: `WFSLayerInfo`, `WFSCapabilities`
- [ ] HTTP client setup (reuse ArcGIS pattern)
- [ ] Capability parsing with OWSLib
- [ ] Layer info and DescribeFeatureType parsing
- [ ] Bbox filter construction (server + local)
- [ ] Pagination logic with progress tracking
- [ ] Format detection and negotiation
- [ ] GeoJSON→Arrow conversion (via DuckDB)
- [ ] GML→Arrow conversion (via DuckDB)
- [ ] CRS negotiation logic
- [ ] Streaming to Parquet function
- [ ] `wfs_to_table()` main function
- [ ] `convert_wfs_to_geoparquet()` CLI wrapper
- [ ] `list_available_layers()` helper

### CLI Command
- [ ] Add `@extract.command(name="wfs")` to `cli/main.py`
- [ ] All option decorators
- [ ] Docstring with examples
- [ ] List layers mode (typename optional)
- [ ] Error handling and user-friendly messages

### Python API
- [ ] Add `ops.from_wfs()` to `api/ops.py`
- [ ] Add `Table.from_wfs()` to `api/table.py`
- [ ] Docstrings with examples

### Tests
- [ ] Create `tests/test_wfs.py`
- [ ] Unit tests: bbox strategy, filter construction, format detection, CRS negotiation
- [ ] Mock-based tests: capability parsing, pagination, error handling
- [ ] Integration tests (marked `@pytest.mark.network`)
- [ ] Edge case coverage: empty results, missing geometry, namespace resolution

### Documentation
- [ ] Update `docs/guide/extract.md` with WFS section
- [ ] Update `docs/api/python-api.md` with WFS API
- [ ] Update `CLAUDE.md` CLI command table
- [ ] Add examples for common public WFS services

### Dependencies
- [ ] Add `owslib>=0.29.0` to `pyproject.toml`
- [ ] Test installation: `uv pip install -e .`

### Verification
- [ ] Extract from public WFS (USGS)
- [ ] Test bbox filtering (server + local modes)
- [ ] Test list layers mode
- [ ] Test Python API
- [ ] Run all tests (unit + integration)
- [ ] Check test coverage (>80% for new code)
- [ ] Verify complexity grade A (`xenon --max-absolute=A geoparquet_io/core/wfs.py`)

### Follow-Up Issues
- [ ] Create "WFS Authentication Support" issue
- [ ] Create "Server-Side Attribute Filtering (CQL)" issue
- [ ] Create "WFS 2.0 Support" issue

---

## Estimates

- **Lines of Code**: ~2100-2600 total
  - Core module: ~1000-1200 lines
  - Tests: ~800-1000 lines
  - CLI/API: ~100 lines
  - Docs: ~200 lines

- **Test Coverage Target**: >80% for new code (following project standard)

- **Complexity Target**: Grade A (maintained through modular design, each function 30-40 lines max)

- **Development Time**: ~15-22 hours
  - Core module: 8-10 hours
  - Tests: 4-6 hours
  - CLI/API: 1-2 hours
  - Documentation: 2-4 hours
