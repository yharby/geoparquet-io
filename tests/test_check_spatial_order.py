"""Tests for core/check_spatial_order.py module."""

from geoparquet_io.core.check_spatial_order import (
    _bboxes_overlap,
    check_spatial_order,
    check_spatial_order_bbox_stats,
)


class TestCheckSpatialOrder:
    """Tests for check_spatial_order function."""

    def test_returns_results(self, places_test_file):
        """Test check_spatial_order with return_results=True."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)
        assert "passed" in result

    def test_with_verbose(self, places_test_file):
        """Test check_spatial_order with verbose flag."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=True,
            return_results=True,
        )
        assert isinstance(result, dict)

    def test_with_small_sample(self, places_test_file):
        """Test check_spatial_order with small sample size."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=10,
            limit_rows=100,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)

    def test_buildings_file(self, buildings_test_file):
        """Test check_spatial_order on buildings file."""
        result = check_spatial_order(
            buildings_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)
        assert "passed" in result

    def test_without_return_results(self, places_test_file):
        """Test check_spatial_order with return_results=False (covers line 144)."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=False,
        )
        # When return_results=False, returns the ratio directly
        assert result is None or isinstance(result, float)

    def test_poorly_ordered_file(self, unsorted_test_file):
        """Test check_spatial_order with poorly ordered file (covers lines 122-123, 131-132)."""
        result = check_spatial_order(
            unsorted_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=True,  # Use verbose to cover line 122-123 output
            return_results=True,
        )
        assert isinstance(result, dict)
        assert result["passed"] is False
        assert result["ratio"] >= 0.5
        assert len(result["issues"]) > 0
        assert len(result["recommendations"]) > 0
        assert "Poor spatial ordering" in result["issues"][0]


class TestBboxOverlap:
    """Tests for _bboxes_overlap helper function."""

    def test_overlapping_bboxes(self):
        """Test bboxes that overlap."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_adjacent_bboxes_touching_edge(self):
        """Test bboxes that touch at an edge (not overlapping)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 0.0, "xmax": 20.0, "ymax": 10.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_disjoint_bboxes(self):
        """Test bboxes that are completely separate."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 20.0, "ymin": 20.0, "xmax": 30.0, "ymax": 30.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_one_bbox_contains_other(self):
        """Test when one bbox completely contains another."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 20.0, "ymax": 20.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_identical_bboxes(self):
        """Test identical bboxes."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_overlap_only_in_x_dimension(self):
        """Test bboxes that overlap in X but not Y (no overlap)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 5.0}
        bbox2 = {"xmin": 5.0, "ymin": 10.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_overlap_only_in_y_dimension(self):
        """Test bboxes that overlap in Y but not X (no overlap)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 5.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_touching_at_corner(self):
        """Test bboxes that touch only at a corner point (not overlapping)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 10.0, "xmax": 20.0, "ymax": 20.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_negative_coordinates(self):
        """Test with negative coordinate values."""
        bbox1 = {"xmin": -10.0, "ymin": -10.0, "xmax": 0.0, "ymax": 0.0}
        bbox2 = {"xmin": -5.0, "ymin": -5.0, "xmax": 5.0, "ymax": 5.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_bbox_ordering_doesnt_matter(self):
        """Test that order of bbox arguments doesn't affect result."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) == _bboxes_overlap(bbox2, bbox1)


class TestCheckSpatialOrderBboxStats:
    """Tests for check_spatial_order_bbox_stats function."""

    def test_returns_dict_structure(self, places_test_file):
        """Test that bbox-stats method returns proper dict structure."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        assert isinstance(result, dict)
        assert "passed" in result
        assert "ratio" in result
        assert "method" in result
        assert result["method"] == "bbox_stats"
        assert "overlap_count" in result
        assert "total_pairs" in result
        assert "issues" in result
        assert "recommendations" in result
        assert "fix_available" in result

    def test_with_spatially_ordered_file(self, places_test_file):
        """Test with file expected to have good spatial ordering."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        # Just verify structure - actual ordering depends on test data
        assert isinstance(result["passed"], bool)
        assert isinstance(result["ratio"], float)
        assert 0.0 <= result["ratio"] <= 1.0
        assert result["overlap_count"] >= 0
        assert result["total_pairs"] >= 0

    def test_with_buildings_file(self, buildings_test_file):
        """Test bbox-stats method with buildings file."""
        result = check_spatial_order_bbox_stats(
            buildings_test_file, verbose=False, return_results=True, quiet=False
        )
        assert isinstance(result, dict)
        assert "passed" in result
        assert "method" in result
        assert result["method"] == "bbox_stats"

    def test_verbose_mode(self, places_test_file):
        """Test bbox-stats method with verbose=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=True, return_results=True, quiet=False
        )
        assert isinstance(result, dict)
        assert result["method"] == "bbox_stats"

    def test_quiet_mode(self, places_test_file):
        """Test bbox-stats method with quiet=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        assert isinstance(result, dict)
        assert result["method"] == "bbox_stats"

    def test_passing_threshold(self, places_test_file):
        """Test that ratio < 0.3 means passed=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        if result["ratio"] < 0.3:
            assert result["passed"] is True
            assert len(result["issues"]) == 0
            assert result["fix_available"] is False
        else:
            assert result["passed"] is False
            assert len(result["issues"]) > 0
            assert result["fix_available"] is True

    def test_failing_threshold(self):
        """Test that ratio >= 0.3 means passed=False."""
        # This test will be valid once we have a poorly ordered test file
        # For now, skip if we don't have such a file
        pass

    def test_issues_and_recommendations_when_failed(self, unsorted_test_file):
        """Test that failing check includes proper issues and recommendations."""
        result = check_spatial_order_bbox_stats(
            unsorted_test_file, verbose=False, return_results=True, quiet=False
        )
        if not result["passed"]:
            assert len(result["issues"]) > 0
            assert len(result["recommendations"]) > 0
            assert "spatial ordering" in str(result["issues"]).lower()
            assert "Hilbert" in str(result["recommendations"])

    def test_without_return_results(self, places_test_file):
        """Test bbox-stats method with return_results=False."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=False, quiet=False
        )
        # Should return ratio directly when return_results=False
        assert result is None or isinstance(result, float)
