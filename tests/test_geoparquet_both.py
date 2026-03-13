"""
Tests for the 'both' GeoParquet version option.

'both' = native Parquet geometry + v1 GeoParquet metadata.
Maximum compatibility: old readers see v1 metadata + WKB fallback,
new readers get native geometry (shredding, ~3x compression).
"""

import json

import pyarrow.parquet as pq

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema
from tests.conftest import has_geoparquet_metadata, has_native_geo_types


class TestBothVersionConfig:
    """Verify 'both' is properly registered in GEOPARQUET_VERSIONS."""

    def test_both_in_version_map(self):
        from geoparquet_io.core.common import GEOPARQUET_VERSIONS

        assert "both" in GEOPARQUET_VERSIONS

    def test_both_uses_none_duckdb_param(self):
        """'both' uses GEOPARQUET_VERSION 'NONE' so project writes its own KV metadata."""
        from geoparquet_io.core.common import GEOPARQUET_VERSIONS

        assert GEOPARQUET_VERSIONS["both"]["duckdb_param"] == "NONE"

    def test_both_uses_v1_metadata_version(self):
        from geoparquet_io.core.common import GEOPARQUET_VERSIONS

        assert GEOPARQUET_VERSIONS["both"]["metadata_version"] == "1.1.0"

    def test_both_requires_metadata_rewrite(self):
        from geoparquet_io.core.common import GEOPARQUET_VERSIONS

        assert GEOPARQUET_VERSIONS["both"]["rewrite_metadata"] is True

    def test_needs_metadata_rewrite_returns_true(self):
        from geoparquet_io.core.write_strategies.base import needs_metadata_rewrite

        assert needs_metadata_rewrite("both", None) is True


class TestBothOutputFormat:
    """Verify 'both' writes native geometry + v1 GeoParquet metadata."""

    def test_both_has_native_geometry(self, fields_5070_file, temp_output_file):
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        assert has_native_geo_types(temp_output_file)

    def test_both_has_geoparquet_metadata(self, fields_5070_file, temp_output_file):
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        assert has_geoparquet_metadata(temp_output_file)

    def test_both_metadata_is_v1(self, fields_5070_file, temp_output_file):
        """Metadata version should be 1.1.0 (v1 format for backward compat)."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        assert geo_meta["version"] == "1.1.0"

    def test_both_metadata_has_bbox(self, fields_5070_file, temp_output_file):
        """Metadata should include bbox (enriched by project, not bare DuckDB output)."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        col_meta = geo_meta["columns"]["geometry"]
        assert "bbox" in col_meta

    def test_both_metadata_has_geometry_types(self, fields_5070_file, temp_output_file):
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        col_meta = geo_meta["columns"]["geometry"]
        assert "geometry_types" in col_meta
        assert len(col_meta["geometry_types"]) > 0


class TestBothCRSHandling:
    """Verify CRS is correctly handled in 'both' mode."""

    def test_both_with_non_default_crs_in_schema(self, fields_5070_file, temp_output_file):
        """Non-default CRS should appear in Parquet schema via ST_SetCRS."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        pf = pq.ParquetFile(temp_output_file)
        schema_str = str(pf.metadata.schema)
        geom_details = parse_geometry_type_from_schema("geometry", schema_str)
        assert geom_details is not None
        assert "crs" in geom_details
        assert geom_details["crs"]["id"]["code"] == 5070

    def test_both_with_non_default_crs_in_metadata(self, fields_5070_file, temp_output_file):
        """Non-default CRS should also appear in geo KV metadata."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        col_meta = geo_meta["columns"]["geometry"]
        assert "crs" in col_meta
        assert col_meta["crs"]["id"]["code"] == 5070

    def test_both_with_default_crs(self, fields_geom_type_only_file, temp_output_file):
        """Default CRS should not trigger ST_SetCRS wrapping."""
        from geoparquet_io.core.common import is_default_crs

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="both",
        )

        assert has_native_geo_types(temp_output_file)
        assert has_geoparquet_metadata(temp_output_file)

        # If CRS is present, it should be default
        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        metadata_crs = geo_meta["columns"]["geometry"].get("crs")
        if metadata_crs:
            assert is_default_crs(metadata_crs)


class TestBothCLI:
    """Verify 'both' is accepted as a CLI option."""

    def test_both_in_cli_choices(self):
        """'both' should be a valid --geoparquet-version choice."""
        import click.testing

        from geoparquet_io.cli.main import cli

        runner = click.testing.CliRunner()
        # Use --help to check the option is listed, or try a dry run
        result = runner.invoke(cli, ["convert", "geoparquet", "--help"])
        assert "both" in result.output
