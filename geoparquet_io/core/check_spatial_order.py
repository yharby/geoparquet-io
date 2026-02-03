#!/usr/bin/env python3


from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_duckdb_connection,
    needs_httpfs,
    safe_file_url,
)
from geoparquet_io.core.logging_config import debug, progress


def _bboxes_overlap(bbox1: dict, bbox2: dict) -> bool:
    """Check if two bounding boxes overlap.

    Two bounding boxes overlap if they share any interior area.
    Boxes that only touch at edges or corners are not considered overlapping.

    Args:
        bbox1: First bbox dict with xmin, ymin, xmax, ymax
        bbox2: Second bbox dict with xmin, ymin, xmax, ymax

    Returns:
        True if bboxes overlap, False otherwise
    """
    # Boxes overlap if they overlap in BOTH X and Y dimensions
    # X overlap: bbox1.xmax > bbox2.xmin AND bbox2.xmax > bbox1.xmin
    # Y overlap: bbox1.ymax > bbox2.ymin AND bbox2.ymax > bbox1.ymin
    x_overlap = bbox1["xmax"] > bbox2["xmin"] and bbox2["xmax"] > bbox1["xmin"]
    y_overlap = bbox1["ymax"] > bbox2["ymin"] and bbox2["ymax"] > bbox1["ymin"]
    return x_overlap and y_overlap


def _calculate_consecutive_avg(con, safe_url, geometry_column, row_limit, verbose):
    """Calculate average distance between consecutive features."""
    query = f"""
    WITH numbered AS (
        SELECT ROW_NUMBER() OVER () as id, {geometry_column} as geom
        FROM '{safe_url}' {row_limit}
    )
    SELECT AVG(ST_Distance(a.geom, b.geom)) as avg_dist
    FROM numbered a JOIN numbered b ON b.id = a.id + 1;
    """
    if verbose:
        progress("Calculating average distance between consecutive features...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between consecutive features: {avg}")
    return avg


def _calculate_random_avg(con, safe_url, geometry_column, row_limit, random_sample_size, verbose):
    """Calculate average distance between random pairs of features."""
    query = f"""
    WITH sample AS (SELECT {geometry_column} as geom FROM '{safe_url}' {row_limit}),
    random_pairs AS (
        SELECT a.geom as geom1, b.geom as geom2
        FROM (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) a,
             (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) b
        WHERE a.geom != b.geom
    )
    SELECT AVG(ST_Distance(geom1, geom2)) as avg_dist FROM random_pairs;
    """
    if verbose:
        progress(f"Calculating average distance between {random_sample_size} random pairs...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between random features: {avg}")
    return avg


def _build_results_dict(ratio, consecutive_avg, random_avg):
    """Build structured results dictionary for sampling method."""
    passed = ratio is not None and ratio < 0.5
    issues = []
    recommendations = []
    if ratio is not None and ratio >= 0.5:
        issues.append(f"Poor spatial ordering (ratio: {ratio:.2f})")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")
    return {
        "passed": passed,
        "ratio": ratio,
        "consecutive_avg": consecutive_avg,
        "random_avg": random_avg,
        "method": "sampling",
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }


def _get_row_limit_clause(con, safe_url, limit_rows, verbose):
    """Determine row limit clause based on total rows."""
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{safe_url}'").fetchone()[0]
    if verbose:
        debug(f"Total rows in file: {total_rows:,}")

    if total_rows > limit_rows:
        if verbose:
            debug(f"Limiting analysis to first {limit_rows:,} rows")
        return f"LIMIT {limit_rows}"
    return ""


def _print_standalone_results(ratio, consecutive_avg, random_avg):
    """Print results when running as standalone command (not from check_all)."""
    progress("\nResults:")
    debug(f"Average distance between consecutive features: {consecutive_avg}")
    debug(f"Average distance between random features: {random_avg}")
    progress(f"Ratio (consecutive / random): {ratio}")

    if ratio is not None and ratio < 0.5:
        progress("=> Data seems strongly spatially clustered.")
    elif ratio is not None:
        progress("=> Data might not be strongly clustered (or is partially clustered).")


def _print_bbox_stats_results(ratio, overlap_count, total_pairs):
    """Print bbox-stats results when running as standalone command."""
    progress("\nResults:")
    debug(f"Row group pairs analyzed: {total_pairs}")
    debug(f"Overlapping pairs: {overlap_count}")
    progress(f"Overlap ratio: {ratio:.2f}")

    if ratio < 0.3:
        progress("=> Data appears well spatially ordered (low row group overlap).")
    else:
        progress("=> Data may benefit from spatial ordering (high row group overlap).")


def check_spatial_order_bbox_stats(parquet_file, verbose=False, return_results=False, quiet=False):
    """Check spatial ordering using row group bbox statistics.

    This method is faster than sampling because it only reads row group metadata
    instead of actual geometry data. It checks if consecutive row groups have
    overlapping bounding boxes, which indicates poor spatial ordering.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    safe_url = safe_file_url(parquet_file, verbose)

    # Check for bbox column
    has_bbox, bbox_col_name = has_bbox_column(safe_url)
    if not has_bbox or not bbox_col_name:
        raise ValueError(
            f"File {parquet_file} does not have a bbox column. "
            "Use the sampling-based method instead."
        )

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    # Get bbox stats per row group
    row_group_bboxes = get_per_row_group_bbox_stats(safe_url, bbox_col_name)

    if verbose:
        debug(f"Analyzing {len(row_group_bboxes)} row groups")

    # Handle edge cases
    if len(row_group_bboxes) <= 1:
        # Can't meaningfully check ordering with 0 or 1 row groups
        # (need at least 2 groups to compare consecutive pairs).
        # Return ratio=0.0 (perfect ordering) since there's no evidence of poor ordering.
        if verbose:
            debug("Only one or zero row groups - assuming well ordered")
        ratio = 0.0
        overlap_count = 0
        total_pairs = 0
    else:
        # Count overlaps in consecutive row group pairs
        overlap_count = 0
        for i in range(len(row_group_bboxes) - 1):
            bbox1 = row_group_bboxes[i]
            bbox2 = row_group_bboxes[i + 1]
            if _bboxes_overlap(bbox1, bbox2):
                overlap_count += 1
                if verbose:
                    debug(f"Row groups {bbox1['row_group_id']} and {bbox2['row_group_id']} overlap")

        total_pairs = len(row_group_bboxes) - 1
        ratio = overlap_count / total_pairs if total_pairs > 0 else 0.0

        if verbose:
            debug(f"Overlapping pairs: {overlap_count}/{total_pairs}")

    # Pass if < 30% overlap
    passed = ratio < 0.3

    # Build results dict
    issues = []
    recommendations = []
    if not passed:
        issues.append(f"Poor spatial ordering (overlap ratio: {ratio:.2f})")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")

    # Print standalone results if not quiet and not return_results
    if not quiet and not return_results and not verbose:
        _print_bbox_stats_results(ratio, overlap_count, total_pairs)

    if return_results:
        return {
            "passed": passed,
            "ratio": ratio,
            "overlap_count": overlap_count,
            "total_pairs": total_pairs,
            "method": "bbox_stats",
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": not passed,
        }

    return ratio


def check_spatial_order(
    parquet_file, random_sample_size, limit_rows, verbose, return_results=False, quiet=False
):
    """Check if a GeoParquet file is spatially ordered.

    Automatically detects if the file has a bbox column (GeoParquet 2.0+) and uses
    the faster bbox-stats method. Falls back to sampling method for older files.

    Args:
        parquet_file: Path to parquet file
        random_sample_size: Number of rows in each random sample (sampling method only)
        limit_rows: Max number of rows to analyze (sampling method only)
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """
    from geoparquet_io.core.duckdb_metadata import has_bbox_column
    from geoparquet_io.core.logging_config import warn

    safe_url = safe_file_url(parquet_file, verbose)

    # Try bbox-stats method first (faster)
    has_bbox, bbox_col_name = has_bbox_column(safe_url)
    if has_bbox and bbox_col_name:
        if verbose:
            debug(f"Using bbox-stats method (bbox column: {bbox_col_name})")
        try:
            return check_spatial_order_bbox_stats(
                parquet_file, verbose=verbose, return_results=return_results, quiet=quiet
            )
        except (ValueError, KeyError, IndexError) as e:
            # ValueError: Invalid bbox column structure
            # KeyError: Missing expected bbox fields (xmin, ymin, xmax, ymax)
            # IndexError: Empty or malformed row group stats
            if verbose:
                warn(f"Bbox-stats method failed: {e}, falling back to sampling")
            # Fall through to sampling method

    # Fall back to sampling method
    if verbose or not quiet:
        warn(
            "Bbox column not found - using slower sampling method. "
            "For faster checks, add bbox column with 'gpio add bbox'."
        )

    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")
        debug("Using sampling method")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
    try:
        row_limit = _get_row_limit_clause(con, safe_url, limit_rows, verbose)

        consecutive_avg = _calculate_consecutive_avg(
            con, safe_url, geometry_column, row_limit, verbose
        )
        random_avg = _calculate_random_avg(
            con, safe_url, geometry_column, row_limit, random_sample_size, verbose
        )

        ratio = consecutive_avg / random_avg if consecutive_avg and random_avg else None

        if not verbose and not quiet:
            _print_standalone_results(ratio, consecutive_avg, random_avg)

        if return_results:
            return _build_results_dict(ratio, consecutive_avg, random_avg)

        return ratio
    finally:
        con.close()
