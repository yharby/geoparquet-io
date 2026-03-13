"""
Unit and integration tests for _wrap_query_with_crs() SQL generation.

Unit tests validate the pure SQL-wrapping logic (escaping, no-op for default CRS).
Integration tests verify the generated SQL executes correctly via DuckDB COPY TO,
confirming DuckDB 1.5+ resolves minimal PROJJSON to full canonical PROJJSON and
writes CRS natively into the Parquet schema.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema
from geoparquet_io.core.write_strategies.duckdb_kv import _wrap_query_with_crs

# --- Test CRS constants ---

EPSG_5070_PROJJSON = {
    "type": "ProjectedCRS",
    "name": "NAD83 / Conus Albers",
    "id": {"authority": "EPSG", "code": 5070},
}

EPSG_4326_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84",
    "id": {"authority": "EPSG", "code": 4326},
}

OGC_CRS84_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "id": {"authority": "OGC", "code": "CRS84"},
}

BASE_QUERY = "SELECT * FROM source_table"


# --- Fixtures ---


@pytest.fixture
def spatial_con():
    """DuckDB connection with spatial extension loaded."""
    con = get_duckdb_connection(load_spatial=True)
    con.execute("""
        CREATE TABLE test_src AS
        SELECT ST_Point(1.0, 2.0) AS geometry, 42 AS value
    """)
    yield con
    con.close()


# --- No-op cases: query returned unchanged ---


class TestWrapQueryNoOp:
    """Cases where _wrap_query_with_crs should return the query unchanged."""

    def test_none_crs(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", None)
        assert result == BASE_QUERY

    def test_empty_dict_crs(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", {})
        assert result == BASE_QUERY

    def test_default_crs_epsg_4326(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", EPSG_4326_PROJJSON)
        assert result == BASE_QUERY

    def test_default_crs_ogc_crs84(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", OGC_CRS84_PROJJSON)
        assert result == BASE_QUERY


# --- Wrapping cases: query should be wrapped with ST_SetCRS ---


class TestWrapQueryWithCRS:
    """Cases where _wrap_query_with_crs should wrap the query."""

    def test_non_default_crs_wraps_query(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", EPSG_5070_PROJJSON)

        assert "ST_SetCRS" in result
        assert '"geometry"' in result
        assert json.dumps(EPSG_5070_PROJJSON) in result
        assert BASE_QUERY in result

    def test_wrapped_query_structure(self):
        """Verify the SQL structure: SELECT * REPLACE (...) FROM (original)."""
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", EPSG_5070_PROJJSON)

        assert "SELECT * REPLACE" in result
        assert f"FROM ({BASE_QUERY})" in result

    def test_custom_geometry_column_name(self):
        result = _wrap_query_with_crs(BASE_QUERY, "geom", EPSG_5070_PROJJSON)

        assert '"geom"' in result
        assert "ST_SetCRS" in result

    def test_geometry_column_with_double_quotes_escaped(self):
        """Column names with quotes must be double-escaped in SQL."""
        result = _wrap_query_with_crs(BASE_QUERY, 'my"geom', EPSG_5070_PROJJSON)

        # Double-quote inside identifier → doubled: "my""geom"
        assert '"my""geom"' in result

    def test_crs_json_with_single_quotes_escaped(self):
        """Single quotes in CRS JSON must be escaped for SQL string literals."""
        crs_with_quote = {
            "type": "ProjectedCRS",
            "name": "NAD83 / Conus Albers (it's projected)",
            "id": {"authority": "EPSG", "code": 5070},
        }
        result = _wrap_query_with_crs(BASE_QUERY, "geometry", crs_with_quote)

        # Single quote → doubled for SQL: it''s
        assert "it''s" in result
        # Should NOT have an unescaped single quote breaking the SQL string
        crs_json_in_result = result.split("ST_SetCRS")[1]
        assert "it''s projected" in crs_json_in_result


# --- Integration: verify ST_SetCRS SQL is valid DuckDB syntax ---


class TestWrapQueryDuckDBExecution:
    """Verify the generated SQL actually executes in DuckDB.

    DuckDB Python bindings can't deserialize GEOMETRY('EPSG:5070') back to Python
    (NotImplementedException), so we test via COPY TO — which is the actual
    production code path.
    """

    def test_parquet_geo_only_path(self, spatial_con, tmp_path):
        """Matches parquet-geo-only production path: GEOPARQUET_VERSION 'NONE'.

        This is the DuckDBKVStrategy._write_parquet_geo_only path — no geo KV
        metadata, CRS only in Parquet schema via ST_SetCRS.
        """
        output_file = str(tmp_path / "pgo_output.parquet")
        query = "SELECT * FROM test_src"
        wrapped = _wrap_query_with_crs(query, "geometry", EPSG_5070_PROJJSON)

        spatial_con.execute(f"""
            COPY ({wrapped}) TO '{output_file}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
        """)

        pf = pq.ParquetFile(output_file)

        # CRS in Parquet schema
        schema_str = str(pf.metadata.schema)
        geom_details = parse_geometry_type_from_schema("geometry", schema_str)
        assert geom_details is not None
        assert "crs" in geom_details
        assert geom_details["crs"]["id"]["code"] == 5070

        # No GeoParquet metadata (parquet-geo-only has none)
        metadata = pf.schema_arrow.metadata or {}
        assert b"geo" not in metadata

    def test_v2_plain_copy_path(self, spatial_con, tmp_path):
        """Matches v2.0 production path: GEOPARQUET_VERSION 'V2' via _plain_copy_to.

        DuckDB writes native geometry + v2 geo metadata natively. ST_SetCRS
        ensures CRS appears in both the Parquet schema and the auto-generated
        geo KV metadata.
        """
        output_file = str(tmp_path / "v2_output.parquet")
        query = "SELECT * FROM test_src"
        wrapped = _wrap_query_with_crs(query, "geometry", EPSG_5070_PROJJSON)

        spatial_con.execute(f"""
            COPY ({wrapped}) TO '{output_file}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
        """)

        pf = pq.ParquetFile(output_file)

        # CRS in Parquet schema
        schema_str = str(pf.metadata.schema)
        geom_details = parse_geometry_type_from_schema("geometry", schema_str)
        assert geom_details is not None
        assert "crs" in geom_details
        assert geom_details["crs"]["id"]["code"] == 5070

        # CRS in DuckDB-generated GeoParquet 'geo' KV metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        geo_crs = geo_meta["columns"]["geometry"].get("crs")
        assert geo_crs is not None
        assert geo_crs["id"]["code"] == 5070

        # Schema CRS and metadata CRS should be identical
        assert geom_details["crs"] == geo_crs

    def test_kv_strategy_path(self, spatial_con, tmp_path):
        """Matches DuckDBKVStrategy._write_with_geo_metadata path for v2.0.

        Uses GEOPARQUET_VERSION 'NONE' + manual KV_METADATA — the strategy
        writes its own geo metadata rather than relying on DuckDB's auto-generated one.
        """
        output_file = str(tmp_path / "kv_output.parquet")
        query = "SELECT * FROM test_src"
        wrapped = _wrap_query_with_crs(query, "geometry", EPSG_5070_PROJJSON)

        # Simulate what DuckDBKVStrategy does: manual geo metadata via KV_METADATA
        geo_meta = {
            "version": "2.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "geometry",
                    "geometry_types": ["Point"],
                    "crs": EPSG_5070_PROJJSON,
                }
            },
        }
        geo_meta_escaped = json.dumps(geo_meta).replace("'", "''")

        spatial_con.execute(f"""
            COPY ({wrapped}) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD,
             GEOPARQUET_VERSION 'NONE',
             KV_METADATA {{geo: '{geo_meta_escaped}'}})
        """)

        pf = pq.ParquetFile(output_file)

        # CRS in Parquet schema (from ST_SetCRS)
        schema_str = str(pf.metadata.schema)
        geom_details = parse_geometry_type_from_schema("geometry", schema_str)
        assert geom_details is not None
        assert "crs" in geom_details
        assert geom_details["crs"]["id"]["code"] == 5070

        # CRS in manually-written GeoParquet 'geo' KV metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata
        geo_meta_read = json.loads(metadata[b"geo"].decode("utf-8"))
        geo_crs = geo_meta_read["columns"]["geometry"].get("crs")
        assert geo_crs is not None
        assert geo_crs["id"]["code"] == 5070

    def test_duckdb_expands_minimal_projjson(self, spatial_con, tmp_path):
        """DuckDB resolves minimal PROJJSON (3 keys) to full canonical PROJJSON."""
        output_file = str(tmp_path / "expanded.parquet")
        query = "SELECT * FROM test_src"
        wrapped = _wrap_query_with_crs(query, "geometry", EPSG_5070_PROJJSON)

        spatial_con.execute(f"""
            COPY ({wrapped}) TO '{output_file}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
        """)

        pf = pq.ParquetFile(output_file)
        schema_str = str(pf.metadata.schema)
        geom_details = parse_geometry_type_from_schema("geometry", schema_str)
        written_crs = geom_details["crs"]

        # Input had 3 keys; DuckDB should expand to full PROJJSON
        assert len(written_crs) > len(EPSG_5070_PROJJSON)
        assert "$schema" in written_crs
        assert "coordinate_system" in written_crs
        # EPSG code must survive the expansion
        assert written_crs["id"]["code"] == 5070

    def test_default_crs_no_wrapping(self, spatial_con, tmp_path):
        """Default CRS (EPSG:4326) does NOT wrap query — no ST_SetCRS call."""
        output_file = str(tmp_path / "default_crs.parquet")
        query = "SELECT * FROM test_src"
        wrapped = _wrap_query_with_crs(query, "geometry", EPSG_4326_PROJJSON)

        assert wrapped == query

        spatial_con.execute(f"""
            COPY ({wrapped}) TO '{output_file}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
        """)

        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 1
