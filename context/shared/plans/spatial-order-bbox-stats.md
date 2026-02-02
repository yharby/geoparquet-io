# Implementation Plan: Spatial Order Check Using Bbox Statistics

**Issue**: #109 - Check spatial ordering in geoparquet 2.0 with bbox stats
**Branch**: `feature/spatial-order-bbox-stats`
**Estimate**: 3-5 hours

## Problem Statement

Current spatial order checking uses statistical sampling with expensive `ST_Distance()` calculations on geometry data. For GeoParquet 2.0 files with bbox columns, we can use row group statistics for a much faster (~10-100x) and more accurate check.

## Design Decisions

Based on user feedback:

1. **Metric**: Overlap ratio (0.0-1.0) - percentage of consecutive row group pairs that have overlapping bboxes
2. **Threshold**: < 0.3 (30% overlap) for passing
3. **Fallback**: Auto-fallback to sampling method with warning when bbox column unavailable
4. **Return format**: Same structure as current method, add "method" field to indicate which approach used

## Implementation Steps

### Phase 1: Core Implementation (TDD)

#### Step 1: Write tests for bbox overlap detection helper
- Test bbox overlap detection (overlapping, adjacent, disjoint cases)
- Test edge cases (null values, single row group, empty file)
- **Commit**: "Add tests for bbox overlap detection helper"

#### Step 2: Implement bbox overlap helper function
- Create `_bboxes_overlap()` helper in `check_spatial_order.py`
- Handle struct format: `{"xmin": x1, "ymin": y1, "xmax": x2, "ymax": y2}`
- **Commit**: "Implement bbox overlap detection helper"

#### Step 3: Write tests for bbox-stats-based spatial order check
- Test with spatially ordered file (low overlap)
- Test with unordered file (high overlap)
- Test with GeoParquet 2.0 file (has bbox column)
- Test return structure matches current format
- **Commit**: "Add tests for bbox-stats spatial order check"

#### Step 4: Implement `check_spatial_order_bbox_stats()`
- Use `get_per_row_group_bbox_stats()` from duckdb_metadata
- Calculate overlap ratio for consecutive row groups
- Return dict matching current format with "method": "bbox_stats"
- **Commit**: "Implement bbox-stats-based spatial order check"

### Phase 2: Integration

#### Step 5: Write tests for auto-detection and fallback
- Test auto-detection of bbox column
- Test fallback to sampling when no bbox column
- Test warning message generation
- **Commit**: "Add tests for method auto-detection and fallback"

#### Step 6: Update `check_spatial_order()` with auto-detection
- Detect bbox column presence using `has_bbox_column()`
- Route to bbox-stats method if available
- Show warning and fallback to sampling if unavailable
- Preserve all existing parameters and behavior
- **Commit**: "Add bbox-stats auto-detection to check_spatial_order"

### Phase 3: CLI Integration

#### Step 7: Update CLI command tests
- Test verbose output shows method used
- Test quiet mode works with both methods
- **Commit**: "Add CLI tests for bbox-stats method"

#### Step 8: Update CLI verbose output
- Show which method is being used
- Show row group count for bbox-stats method
- Show overlap percentage
- **Commit**: "Update CLI output for bbox-stats method"

### Phase 4: Documentation

#### Step 9: Update documentation
- Document new bbox-stats method in `docs/guide/check.md`
- Explain fallback behavior
- Note performance improvements
- Update API docs if needed
- **Commit**: "Document bbox-stats spatial order checking"

#### Step 10: Update CHANGELOG
- Add entry for new feature
- **Commit**: "Update CHANGELOG for bbox-stats spatial order check"

## Technical Details

### Key Functions

**New**:
- `_bboxes_overlap(bbox1: dict, bbox2: dict) -> bool`
- `check_spatial_order_bbox_stats(parquet_file, verbose, return_results, quiet) -> dict`

**Modified**:
- `check_spatial_order()` - add auto-detection logic

**Existing (reused)**:
- `get_per_row_group_bbox_stats()` from `duckdb_metadata.py`
- `has_bbox_column()` from `duckdb_metadata.py`

### Algorithm

```python
def check_spatial_order_bbox_stats(parquet_file, ...):
    # Get bbox stats per row group
    row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_column)

    # Count overlaps in consecutive pairs
    overlap_count = 0
    for i in range(len(row_group_bboxes) - 1):
        bbox1 = row_group_bboxes[i]
        bbox2 = row_group_bboxes[i + 1]
        if _bboxes_overlap(bbox1, bbox2):
            overlap_count += 1

    # Calculate overlap ratio
    total_pairs = len(row_group_bboxes) - 1
    ratio = overlap_count / total_pairs if total_pairs > 0 else 0.0

    # Pass if < 30% overlap
    passed = ratio < 0.3

    return {
        "passed": passed,
        "ratio": ratio,
        "overlap_count": overlap_count,
        "total_pairs": total_pairs,
        "method": "bbox_stats",
        "issues": ["Poor spatial ordering..."] if not passed else [],
        "recommendations": ["Apply Hilbert..."] if not passed else [],
        "fix_available": not passed,
    }
```

### Compatibility

Return structure maintains compatibility:
```python
{
    "passed": bool,           # Same
    "ratio": float,           # Same (different meaning but same range)
    "issues": list[str],      # Same
    "recommendations": list,  # Same
    "fix_available": bool,    # Same
    "method": str,            # NEW: "bbox_stats" or "sampling"
    # bbox_stats specific (optional):
    "overlap_count": int,
    "total_pairs": int,
    # sampling specific (optional):
    "consecutive_avg": float,
    "random_avg": float,
}
```

## Testing Strategy

1. **Unit tests**: Bbox overlap detection
2. **Integration tests**: Full check with bbox stats
3. **Fallback tests**: Ensure sampling still works
4. **End-to-end tests**: CLI with verbose/quiet modes
5. **Test files needed**:
   - GeoParquet 2.0 file with good spatial order (low overlap)
   - GeoParquet 2.0 file with poor spatial order (high overlap)
   - GeoParquet 1.x file (no bbox column - for fallback testing)

## Success Criteria

- [ ] All new tests pass
- [ ] Existing spatial order tests still pass
- [ ] Coverage remains > 80%
- [ ] Documentation updated
- [ ] Manual testing shows ~10-100x speedup on large files
- [ ] Fallback works transparently for GeoParquet 1.x files
- [ ] Warning shown when falling back to sampling

## Risks & Mitigations

**Risk**: Breaking existing behavior
**Mitigation**: Preserve all existing function signatures and return formats

**Risk**: Files with bbox column but invalid/missing stats
**Mitigation**: Handle None values gracefully, fallback to sampling if needed

**Risk**: Single row group files
**Mitigation**: Return "passed" automatically (can't check ordering with 1 row group)

## Performance Expectations

**Current method** (sampling):
- Reads N rows (default 500)
- Calculates distances on geometry (expensive)
- Time: ~1-5 seconds for typical files

**New method** (bbox stats):
- Reads row group metadata only (~10-100 row groups typically)
- No geometry processing
- Time: ~0.01-0.1 seconds (10-100x faster)

## Related Issues

- #109 - Parent issue for this work
- Potentially related to spatial index documentation
