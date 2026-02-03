"""Sub-partition functionality for processing directories of parquet files."""

from __future__ import annotations

from pathlib import Path


def find_large_files(
    directory: str,
    min_size_bytes: int,
    recursive: bool = True,
) -> list[str]:
    """
    Find parquet files in a directory that exceed the size threshold.

    Args:
        directory: Directory to search
        min_size_bytes: Minimum file size in bytes
        recursive: Search subdirectories (default: True)

    Returns:
        List of file paths exceeding the threshold, sorted by size descending
    """
    large_files = []
    dir_path = Path(directory)

    pattern = "**/*.parquet" if recursive else "*.parquet"

    for parquet_file in dir_path.glob(pattern):
        if parquet_file.is_file():
            size = parquet_file.stat().st_size
            if size >= min_size_bytes:
                large_files.append((str(parquet_file), size))

    # Sort by size descending (largest first)
    large_files.sort(key=lambda x: x[1], reverse=True)

    return [f[0] for f in large_files]
