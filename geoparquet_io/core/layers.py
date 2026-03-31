"""
Layer enumeration for multi-layer spatial formats.

Provides utilities for listing layers in GeoPackage and FileGDB files.
Single-layer formats (GeoJSON, Shapefile, etc.) return None.

Example:
    from geoparquet_io.core.layers import list_layers

    # Multi-layer GeoPackage
    layers = list_layers('multilayer.gpkg')
    # ['buildings', 'roads', 'parcels']

    # FileGDB
    layers = list_layers('data.gdb')
    # ['points', 'lines', 'polygons']

    # Single-layer format
    layers = list_layers('single.geojson')
    # None
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


def _is_geopackage(path: str) -> bool:
    """Check if file is a GeoPackage based on extension."""
    return path.lower().rstrip("/\\").endswith(".gpkg")


def _is_filegdb(path: str) -> bool:
    """Check if path is a FileGDB directory based on extension."""
    return path.lower().rstrip("/\\").endswith(".gdb")


def _list_geopackage_layers(path: str) -> list[str] | None:
    """
    List layers in a GeoPackage file by querying gpkg_contents.

    Args:
        path: Path to the GeoPackage file

    Returns:
        List of layer names, or None if single-layer

    Raises:
        FileNotFoundError: If file doesn't exist
        sqlite3.Error: If file isn't a valid GeoPackage
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"GeoPackage file not found: {path}")

    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cursor = con.execute(
                """
                SELECT table_name FROM gpkg_contents
                WHERE data_type IN ('features', 'tiles')
                ORDER BY table_name
                """
            )
            layers = [row[0] for row in cursor.fetchall()]
        finally:
            con.close()

        # Return None for single-layer files (consistent with API contract)
        if len(layers) <= 1:
            return None

        return layers

    except sqlite3.DatabaseError as e:
        raise ValueError(f"Invalid GeoPackage file: {path}. Error: {e}") from e


def _list_filegdb_layers(path: str) -> list[str] | None:
    """
    List layers in a FileGDB directory using DuckDB's OpenFileGDB driver.

    Args:
        path: Path to the FileGDB directory (.gdb)

    Returns:
        List of layer names, or None if single-layer

    Raises:
        FileNotFoundError: If directory doesn't exist
        RuntimeError: If DuckDB can't read the FileGDB
    """
    if not os.path.isdir(path):
        raise FileNotFoundError(f"FileGDB directory not found: {path}")

    from geoparquet_io.core.common import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    try:
        # Use ST_Drivers to verify OpenFileGDB is available, then query layer names
        # DuckDB's OpenFileGDB driver exposes layer names via ST_Read_Meta
        # but it returns empty for .gdb directories. We need to iterate through
        # .gdbtable files to find layers.

        # Alternative: Use gdal_info style query through DuckDB
        # The most reliable way is to query the GDB_Items table directly
        gdb_items_path = os.path.join(path, "a00000004.gdbtable")

        if not os.path.exists(gdb_items_path):
            raise ValueError(f"Invalid FileGDB: missing catalog table at {path}")

        # ST_Read on the GDB_Items catalog table to get layer names
        # This internal table contains the feature class definitions
        result = con.execute(
            f"""
            SELECT DISTINCT Name
            FROM ST_Read('{gdb_items_path}')
            WHERE Name IS NOT NULL
              AND Name NOT LIKE 'GDB_%'
              AND PhysicalName IS NOT NULL
            ORDER BY Name
            """
        ).fetchall()

        layers = [row[0] for row in result]

        # Return None for single-layer GDBs
        if len(layers) <= 1:
            return None

        return layers

    except Exception as e:
        # Fall back to enumerating .gdbtable files and trying ST_Read on each
        # This is slower but more reliable for edge cases
        try:
            layers = _list_filegdb_layers_fallback(path, con)
            if len(layers) <= 1:
                return None
            return layers
        except Exception:
            raise RuntimeError(
                f"Could not enumerate layers in FileGDB: {path}. Original error: {e}"
            ) from e
    finally:
        con.close()


def _list_filegdb_layers_fallback(path: str, con) -> list[str]:
    """
    Fallback method: enumerate layers by trying ST_Read on each .gdbtable file.

    This is slower but works when the catalog table approach fails.
    """
    gdbtable_files = sorted(
        [f for f in os.listdir(path) if f.endswith(".gdbtable")],
        reverse=True,  # Higher numbers are user tables
    )

    layers = []
    for table_file in gdbtable_files:
        table_path = os.path.join(path, table_file)
        try:
            # Try to get layer metadata
            result = con.execute(f"SELECT * FROM ST_Read_Meta('{table_path}')").fetchone()
            if result and result[0]:  # layer_name is first column
                layer_name = result[0]
                # Skip internal GDB tables
                if not layer_name.startswith("GDB_"):
                    layers.append(layer_name)
        except Exception:
            # Skip files that can't be read as spatial layers
            continue

    return sorted(set(layers))


def list_layers(path: str) -> list[str] | None:
    """
    List layers in a multi-layer spatial file.

    Supports GeoPackage (.gpkg) and FileGDB (.gdb) formats.
    Returns None for single-layer formats (GeoJSON, Shapefile, etc.)
    or files with only one layer.

    Args:
        path: Path to the spatial file or directory

    Returns:
        List of layer names for multi-layer files, or None for single-layer files

    Raises:
        FileNotFoundError: If file/directory doesn't exist
        ValueError: If path is empty or format is unsupported

    Example:
        >>> from geoparquet_io.core.layers import list_layers
        >>> list_layers('multilayer.gpkg')
        ['buildings', 'roads', 'parcels']
        >>> list_layers('single.geojson')
        None
    """
    if not path:
        raise ValueError("Path cannot be empty")

    path_str = str(path)

    # GeoPackage: sqlite-based, use direct query
    if _is_geopackage(path_str):
        return _list_geopackage_layers(path_str)

    # FileGDB: directory-based, use DuckDB
    if _is_filegdb(path_str):
        return _list_filegdb_layers(path_str)

    # Check if file exists for other formats
    path_obj = Path(path_str)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_str}")

    # Single-layer formats: GeoJSON, Shapefile, Parquet, etc.
    # Return None to indicate single-layer
    return None
