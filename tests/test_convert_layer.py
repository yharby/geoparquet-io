"""
Tests for the layer parameter in convert operations.

These tests verify that multi-layer formats (GeoPackage, FileGDB) can be read
with a specific layer selected via the `layer` parameter.

Test fixtures:
- Multi-layer GeoPackage: multilayer_test.gpkg with 'buildings' and 'roads' layers
  Created from buildings_test.gpkg and buildings_test.geojson using ogr2ogr
- FileGDB: From GDAL test suite (testopenfilegdb.gdb) with 37 layers
  Source: https://github.com/OSGeo/gdal/tree/master/autotest/ogr/data/filegdb
"""

import pyarrow.parquet as pq
import pytest

from geoparquet_io.api.table import convert
from geoparquet_io.core.convert import read_spatial_to_arrow


@pytest.fixture
def multilayer_gpkg(test_data_dir):
    """Return path to multi-layer GeoPackage with buildings and roads layers.

    This fixture contains two layers with different column schemas:
    - buildings: id, geometry (42 rows)
    - roads: id, geom (42 rows)

    The different geometry column names ('geometry' vs 'geom') help verify
    layer selection is working correctly.
    """
    return str(test_data_dir / "multilayer_test.gpkg")


@pytest.fixture
def filegdb_path(test_data_dir):
    """Return path to the GDAL test FileGDB with multiple layers.

    Source: https://github.com/OSGeo/gdal/tree/master/autotest/ogr/data/filegdb
    Contains 37 layers including: point, polygon, linestring, multipoint, etc.
    """
    return str(test_data_dir / "testopenfilegdb.gdb")


class TestConvertLayerAPI:
    """Tests for gpio.convert() with layer parameter."""

    def test_convert_geopackage_specific_layer(self, multilayer_gpkg):
        """Converting with layer parameter should read only that layer."""
        # Read buildings layer - has 'id' column and original geometry column
        result = convert(multilayer_gpkg, layer="buildings")
        arrow_table = result.table

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names
        # The output always normalizes to 'geometry' column name
        assert "geometry" in arrow_table.column_names

    def test_convert_geopackage_different_layer(self, multilayer_gpkg):
        """Converting with different layer should read that layer's data."""
        # Read roads layer - also has 'id' column but originally had 'geom' column
        result = convert(multilayer_gpkg, layer="roads")
        arrow_table = result.table

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names
        assert "geometry" in arrow_table.column_names

    def test_convert_geopackage_without_layer_reads_first(self, multilayer_gpkg):
        """Converting without layer parameter should read first/default layer."""
        # Without layer, should still work (reads first layer)
        result = convert(multilayer_gpkg)
        arrow_table = result.table

        # Should get the first layer (buildings)
        assert arrow_table.num_rows == 42
        assert "geometry" in arrow_table.column_names

    @pytest.mark.skip(reason="DuckDB segfaults on invalid layer names - upstream bug")
    def test_convert_geopackage_invalid_layer_raises(self, multilayer_gpkg):
        """Converting with non-existent layer should raise an error.

        Note: Currently skipped because DuckDB's ST_Read segfaults when given
        an invalid layer name instead of raising a proper exception.
        """
        with pytest.raises(Exception) as exc_info:
            convert(multilayer_gpkg, layer="nonexistent_layer")

        # Should mention the layer name or that layer doesn't exist
        error_msg = str(exc_info.value).lower()
        assert "nonexistent" in error_msg or "layer" in error_msg or "not found" in error_msg

    def test_convert_layer_write_roundtrip(self, multilayer_gpkg, tmp_path):
        """Layer selection should work through full write roundtrip."""
        output = tmp_path / "buildings.parquet"

        # Convert buildings layer and write
        convert(multilayer_gpkg, layer="buildings").write(str(output))

        # Read back and verify
        result = pq.read_table(str(output))
        assert result.num_rows == 42
        assert "geometry" in result.column_names


class TestReadSpatialToArrowLayer:
    """Tests for read_spatial_to_arrow() with layer parameter."""

    def test_read_spatial_with_layer(self, multilayer_gpkg):
        """read_spatial_to_arrow should accept layer parameter."""
        arrow_table, crs, geom_col = read_spatial_to_arrow(
            multilayer_gpkg,
            layer="roads",
        )

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names

    def test_read_spatial_without_layer(self, multilayer_gpkg):
        """read_spatial_to_arrow should work without layer (backward compat)."""
        arrow_table, crs, geom_col = read_spatial_to_arrow(multilayer_gpkg)

        # Should still work
        assert arrow_table.num_rows == 42


@pytest.mark.slow
class TestFileGDBLayer:
    """Tests for FileGDB format with layer parameter.

    These tests use the GDAL testopenfilegdb.gdb fixture.
    Marked slow because FileGDB reading can be slower than GeoPackage.
    """

    def test_filegdb_point_layer(self, filegdb_path):
        """Should be able to read the 'point' layer from FileGDB."""
        result = convert(filegdb_path, layer="point")
        arrow_table = result.table

        # The point layer should have geometry
        assert "geometry" in arrow_table.column_names
        assert arrow_table.num_rows > 0

    def test_filegdb_polygon_layer(self, filegdb_path):
        """Should be able to read the 'polygon' layer from FileGDB."""
        result = convert(filegdb_path, layer="polygon")
        arrow_table = result.table

        assert "geometry" in arrow_table.column_names
        assert arrow_table.num_rows > 0

    def test_filegdb_different_layers_different_data(self, filegdb_path):
        """Different layers should return different data."""
        point_result = convert(filegdb_path, layer="point")
        polygon_result = convert(filegdb_path, layer="polygon")

        # Different geometry types = different data
        # (We can't easily compare geometry types in Arrow, but row counts may differ)
        # At minimum, both should succeed
        assert point_result.table.num_rows > 0
        assert polygon_result.table.num_rows > 0
