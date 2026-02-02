"""
Shared constants for geoparquet-io.

This module defines constants that are shared across multiple modules to ensure
consistency and make it easy to change values in one place.
"""

# Default column name for H3 cell IDs
DEFAULT_H3_COLUMN_NAME = "h3_cell"

# Default column name for A5 cell IDs
DEFAULT_A5_COLUMN_NAME = "a5_cell"

# Default column name for quadkey cells
DEFAULT_QUADKEY_COLUMN_NAME = "quadkey"

# Default resolution (zoom level) for quadkey generation
DEFAULT_QUADKEY_RESOLUTION = 13

# Default resolution for quadkey partitioning (prefix length)
DEFAULT_QUADKEY_PARTITION_RESOLUTION = 9

# Default column name for S2 cell IDs
DEFAULT_S2_COLUMN_NAME = "s2_cell"

# Default S2 level (comparable to H3 resolution 9, ~1.2 km² cells)
DEFAULT_S2_LEVEL = 13

# Default compression level for S2 temp file generation
DEFAULT_S2_COMPRESSION_LEVEL = 15

# Default target rows per partition for S2 auto-resolution
DEFAULT_S2_TARGET_ROWS = 100000
