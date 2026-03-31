"""
Tests for the list_layers() function.

Tests verify that multi-layer formats (GeoPackage, FileGDB) have their layers
enumerated correctly, and that single-layer formats return None.
"""

import pytest

from geoparquet_io.core.layers import list_layers as list_layers_core


@pytest.fixture
def multilayer_gpkg(test_data_dir):
    """Return path to multi-layer GeoPackage with buildings and roads layers."""
    return str(test_data_dir / "multilayer_test.gpkg")


@pytest.fixture
def single_layer_gpkg(test_data_dir):
    """Return path to single-layer GeoPackage (buildings_test.gpkg)."""
    return str(test_data_dir / "buildings_test.gpkg")


@pytest.fixture
def filegdb_path(test_data_dir):
    """Return path to the GDAL test FileGDB with multiple layers."""
    return str(test_data_dir / "testopenfilegdb.gdb")


@pytest.fixture
def geojson_input(test_data_dir):
    """Return path to the buildings_test.geojson test file."""
    return str(test_data_dir / "buildings_test.geojson")


@pytest.fixture
def shapefile_input(test_data_dir):
    """Return path to the buildings_test.shp test file."""
    return str(test_data_dir / "buildings_test.shp")


class TestListLayersGeoPackage:
    """Tests for list_layers() with GeoPackage files."""

    def test_multi_layer_geopackage_returns_list(self, multilayer_gpkg):
        """Multi-layer GeoPackage should return list of layer names."""
        layers = list_layers_core(multilayer_gpkg)

        assert layers is not None
        assert isinstance(layers, list)
        assert len(layers) >= 2
        # Should contain buildings and roads
        assert "buildings" in layers
        assert "roads" in layers

    def test_single_layer_geopackage_returns_none(self, single_layer_gpkg):
        """Single-layer GeoPackage should return None."""
        layers = list_layers_core(single_layer_gpkg)

        # Single-layer formats return None
        assert layers is None

    def test_layer_order_preserved(self, multilayer_gpkg):
        """Layers should be returned in consistent order."""
        layers1 = list_layers_core(multilayer_gpkg)
        layers2 = list_layers_core(multilayer_gpkg)

        assert layers1 == layers2


@pytest.mark.slow
class TestListLayersFileGDB:
    """Tests for list_layers() with FileGDB files.

    Marked slow because FileGDB reading can be slower than GeoPackage.
    """

    def test_filegdb_returns_list(self, filegdb_path):
        """FileGDB with multiple layers should return list of layer names."""
        layers = list_layers_core(filegdb_path)

        assert layers is not None
        assert isinstance(layers, list)
        assert len(layers) > 1  # Should have multiple layers

    def test_filegdb_contains_expected_layers(self, filegdb_path):
        """FileGDB should contain known layers from GDAL test fixture."""
        layers = list_layers_core(filegdb_path)

        # GDAL test FileGDB should have these layers
        assert "point" in layers
        assert "polygon" in layers


class TestListLayersSingleLayerFormats:
    """Tests for list_layers() with single-layer formats."""

    def test_geojson_returns_none(self, geojson_input):
        """GeoJSON files (single-layer) should return None."""
        layers = list_layers_core(geojson_input)
        assert layers is None

    def test_shapefile_returns_none(self, shapefile_input):
        """Shapefiles (single-layer) should return None."""
        layers = list_layers_core(shapefile_input)
        assert layers is None

    def test_parquet_returns_none(self, places_test_file):
        """GeoParquet files (single-layer) should return None."""
        layers = list_layers_core(places_test_file)
        assert layers is None


class TestListLayersPublicAPI:
    """Tests for the public API: gpio.list_layers()."""

    def test_api_exposed_at_module_level(self):
        """list_layers should be importable from geoparquet_io."""
        import geoparquet_io as gpio

        assert hasattr(gpio, "list_layers")
        assert callable(gpio.list_layers)

    def test_api_returns_same_as_core(self, multilayer_gpkg):
        """Public API should return same result as core function."""
        import geoparquet_io as gpio

        api_result = gpio.list_layers(multilayer_gpkg)
        core_result = list_layers_core(multilayer_gpkg)

        assert api_result == core_result


class TestListLayersEdgeCases:
    """Tests for edge cases and error handling."""

    def test_nonexistent_file_raises(self, tmp_path):
        """Nonexistent file should raise FileNotFoundError."""
        fake_path = str(tmp_path / "nonexistent.gpkg")

        with pytest.raises(FileNotFoundError):
            list_layers_core(fake_path)

    def test_empty_path_raises(self):
        """Empty path should raise ValueError."""
        with pytest.raises((ValueError, FileNotFoundError)):
            list_layers_core("")

    def test_directory_without_gdb_extension_returns_none(self, tmp_path):
        """Regular directory (not .gdb) should return None (unrecognized format)."""
        # A regular directory that exists is treated as an unknown format
        # Unknown formats return None (same as single-layer formats)
        result = list_layers_core(str(tmp_path))
        assert result is None
