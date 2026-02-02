# Issue #221: Persistent Benchmark Baseline Storage

**Issue**: https://github.com/geoparquet/geoparquet-io/issues/221
**Status**: In Progress
**Created**: 2026-02-02

## Summary

Add persistent storage of benchmark baselines to enable trend detection across releases, rather than only point-in-time comparisons.

## Background

The current benchmark infrastructure (#188) compares results manually or per-release, but there's no persistent storage of historical baselines. This makes it difficult to detect gradual performance drift over multiple releases.

## Implementation Strategy

### Storage Approach: GitHub Artifacts (Option A)

We'll use GitHub Actions artifacts for baseline storage because:
- No repository commits required (keeps repo clean)
- Artifacts already uploaded by current workflow
- Built-in artifact management and retention
- Simple to implement with existing infrastructure

**Trade-offs**:
- Artifacts expire after 90 days by default (acceptable for recent trend tracking)
- We'll configure retention to keep last 5-10 releases permanently via artifact retention API

### Architecture

```
.github/workflows/release-benchmark.yml
├── Fetch previous release baseline artifact
├── Run current benchmarks
├── Compare current vs baseline
├── Detect trends across multiple releases
├── Upload current baseline as artifact
└── Update release notes with comparison + trends
```

## Implementation Steps

### Step 1: Add baseline artifact retrieval logic
**File**: `.github/workflows/release-benchmark.yml`

- Add step to download previous release's benchmark artifact
- Handle case where no previous baseline exists (first release)
- Use GitHub API to find latest baseline artifact

### Step 2: Enhance trend analysis in version_benchmark.py
**File**: `scripts/version_benchmark.py`

- Add `--trend-analysis` mode that accepts multiple baseline files
- Implement gradual degradation detection (e.g., 5% per release for 3 releases)
- Generate trend statistics (avg change, max regression, consistency)
- Output trend warnings in structured format for GitHub Actions

### Step 3: Update release workflow to perform trend analysis
**File**: `.github/workflows/release-benchmark.yml`

- Modify workflow to fetch last N releases' baselines (N=5)
- Run trend analysis on historical data
- Flag gradual degradation patterns
- Include trend analysis in release notes

### Step 4: Add baseline management utilities
**File**: `scripts/manage_baselines.py` (new)

- CLI tool to list available baselines
- Download specific baseline artifacts
- Compare arbitrary baselines
- Useful for local development and debugging

### Step 5: Update documentation
**Files**:
- `docs/guide/benchmarking.md` (update)
- `CHANGELOG.md`

- Document baseline storage location (GitHub artifacts)
- Explain trend detection thresholds
- Provide examples of how to interpret trends
- Add troubleshooting guide

### Step 6: Add tests
**File**: `tests/test_version_benchmark.py` (new)

- Test trend analysis logic
- Test multi-baseline comparison
- Test degradation detection thresholds
- Mock artifact downloads for CI

## Acceptance Criteria

- [x] Release workflow automatically compares against previous release baseline *(already implemented)*
- [ ] Workflow downloads last 5 releases' baselines from GitHub artifacts
- [ ] Trend detection flags gradual degradation across releases
- [ ] Baselines persisted and retrievable for at least last 5 releases
- [ ] Documentation updated with baseline storage location and usage
- [ ] Tests added for trend analysis logic

## Technical Details

### Trend Detection Algorithm

```python
def detect_gradual_degradation(baselines: list[dict]) -> list[str]:
    """
    Detect operations showing consistent degradation across releases.

    Threshold: >5% slower per release for 3+ consecutive releases
    """
    warnings = []

    # Group by (file_size, operation)
    for key in get_all_operation_keys(baselines):
        times = [get_time(b, key) for b in baselines]

        # Check for consistent increase
        if len(times) >= 3:
            deltas = [(times[i+1] - times[i]) / times[i] for i in range(len(times)-1)]
            if all(d > 0.05 for d in deltas[-2:]):  # Last 2 deltas > 5%
                warnings.append(f"{key}: Gradual degradation detected")

    return warnings
```

### GitHub Artifact Naming Convention

```
release-benchmark-v{version}
├── results_v{version}.json       # Benchmark results
├── comparison.txt                # Comparison with previous
└── metadata.json                 # Release metadata
```

### Artifact Retention Strategy

- Default retention: 90 days
- For releases: Keep indefinitely via GitHub API call
- Maximum baselines tracked: 10 most recent releases
- Older baselines can be archived to S3 if needed (future enhancement)

## Dependencies

- GitHub Actions artifact API (`actions/download-artifact@v6`)
- GitHub REST API for artifact management
- No new Python dependencies required

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Artifact expiration | Configure indefinite retention for release artifacts |
| Breaking changes in workflow | Maintain backward compatibility, graceful fallback |
| Large artifact storage | Baselines are small JSON files (~50KB each) |
| First release with no baseline | Skip trend analysis, only log warning |

## Timeline

**Estimated**: 1-2 days

- Step 1-3: 4-6 hours (core implementation)
- Step 4: 2 hours (utilities)
- Step 5: 1 hour (documentation)
- Step 6: 2-3 hours (tests)

## Related Issues

- #188 - Benchmark suite (foundation)
- #129 - Performance investigation (would benefit from trend data)

## Future Enhancements (Out of Scope)

- Dashboard page in docs showing historical performance charts
- Slack/Discord notifications for regressions
- S3 backup for long-term baseline storage
- Benchmark comparison UI in GitHub Pages
