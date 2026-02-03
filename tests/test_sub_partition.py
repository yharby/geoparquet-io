"""Tests for sub-partition functionality."""

import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def temp_partition_dir():
    """Create a temp directory with parquet files of varying sizes."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestMinSizeOption:
    """Test --min-size option parsing."""

    def test_min_size_option_exists(self, cli_runner):
        """Verify --min-size option is recognized."""
        result = cli_runner.invoke(partition, ["h3", "--help"])
        assert result.exit_code == 0
        assert "--min-size" in result.output

    def test_in_place_option_exists(self, cli_runner):
        """Verify --in-place option is recognized."""
        result = cli_runner.invoke(partition, ["h3", "--help"])
        assert result.exit_code == 0
        assert "--in-place" in result.output


class TestSubPartitionCore:
    """Test sub_partition core functionality."""

    def test_find_large_files_filters_by_size(self, temp_partition_dir):
        """Test that find_large_files correctly filters by size threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create test files of different sizes
        # Small file: 1KB
        small_data = pa.table({"id": [1], "geometry": [b"POINT(0 0)"]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Large file: create with more rows to exceed threshold
        large_data = pa.table({"id": list(range(10000)), "geometry": [b"POINT(0 0)" * 100] * 10000})
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        pq.write_table(large_data, large_path)

        # Threshold that should only match the large file
        large_size = os.path.getsize(large_path)
        small_size = os.path.getsize(small_path)
        threshold = (large_size + small_size) // 2  # Middle value

        result = find_large_files(temp_partition_dir, min_size_bytes=threshold)

        assert len(result) == 1
        assert result[0] == large_path

    def test_find_large_files_returns_empty_for_no_matches(self, temp_partition_dir):
        """Test that find_large_files returns empty list when no files exceed threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create small file
        small_data = pa.table({"id": [1]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Threshold larger than any file
        result = find_large_files(temp_partition_dir, min_size_bytes=1000000000)

        assert result == []

    def test_find_large_files_recursive(self, temp_partition_dir):
        """Test that find_large_files searches subdirectories."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create nested file
        subdir = os.path.join(temp_partition_dir, "subdir")
        os.makedirs(subdir)
        data = pa.table({"id": list(range(1000))})
        nested_path = os.path.join(subdir, "nested.parquet")
        pq.write_table(data, nested_path)

        result = find_large_files(temp_partition_dir, min_size_bytes=1)

        assert len(result) == 1
        assert result[0] == nested_path
