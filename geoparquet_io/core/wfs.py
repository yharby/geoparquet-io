"""
WFS (Web Feature Service) to GeoParquet conversion.

This module provides functionality to download features from OGC WFS services
and convert them to GeoParquet format. Supports WFS 1.0.0 and 1.1.0.

Key features:
- Automatic pagination with progress tracking
- Parallel fetching for improved performance
- Server-side and local bbox filtering
- Format auto-detection (GeoJSON preferred, GML fallback)
- CRS negotiation with EPSG variant handling
- Memory-efficient streaming to Parquet
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
import uuid
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pyarrow as pa
import pyarrow.parquet as pq

# Public API
__all__ = [
    "WFSError",
    "WFSLayerInfo",
    "convert_wfs_to_geoparquet",
    "get_layer_info",
    "get_wfs_capabilities",
    "list_available_layers",
    "wfs_to_table",
]

from geoparquet_io.core.common import (
    get_duckdb_connection,
    parse_crs_string_to_projjson,
    write_geoparquet_table,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)


class WFSError(Exception):
    """Exception raised for WFS-related errors."""

    pass


# Default page size for WFS requests (most servers support 1000-2000)
DEFAULT_PAGE_SIZE = 1000

# Maximum recommended workers for parallel fetching
MAX_RECOMMENDED_WORKERS = 10

# GeoJSON output format identifiers (in preference order)
GEOJSON_FORMATS = [
    "application/json",
    "json",
    "geojson",
    "application/geo+json",
    "application/vnd.geo+json",
]

# GML output format identifiers (in preference order)
GML_FORMATS = [
    "gml3",
    "text/xml; subtype=gml/3.1.1",
    "application/gml+xml; version=3.1",
    "gml32",
    "text/xml; subtype=gml/3.2",
    "gml2",
    "text/xml; subtype=gml/2.1.2",
]


@dataclass
class WFSLayerInfo:
    """WFS layer/feature type metadata."""

    typename: str
    title: str | None
    crs_list: list[str]
    default_crs: str | None
    bbox: tuple[float, float, float, float] | None
    geometry_column: str
    available_formats: list[str]


# Module-level HTTP client for connection pooling with thread safety
_shared_http_client = None
_http_client_lock = threading.Lock()

# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 60.0


def _get_shared_http_client(timeout: float = DEFAULT_TIMEOUT):
    """
    Get or create a shared HTTP client for connection pooling.

    Thread-safe: uses a lock to prevent race conditions when
    multiple threads try to create the client simultaneously.

    Reuses TCP connections across requests, saving ~100-200ms per request
    on TLS handshakes.

    Args:
        timeout: Request timeout in seconds (default: 60.0)

    Returns:
        httpx.Client: Shared client with connection pooling enabled
    """
    global _shared_http_client

    with _http_client_lock:
        if _shared_http_client is None:
            try:
                import httpx

                _shared_http_client = httpx.Client(
                    timeout=timeout,
                    follow_redirects=True,
                    http2=False,  # Disabled for compatibility with older WFS servers
                    limits=httpx.Limits(
                        max_connections=20,
                        max_keepalive_connections=20,
                    ),
                )
            except ImportError as e:
                raise WFSError(
                    "httpx is required for WFS extraction. Install with: pip install httpx"
                ) from e

    return _shared_http_client


def _reset_http_client():
    """Reset the shared HTTP client (for testing or cleanup)."""
    global _shared_http_client

    with _http_client_lock:
        if _shared_http_client is not None:
            _shared_http_client.close()
            _shared_http_client = None


def _make_request(
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    accept: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> bytes:
    """
    Make HTTP GET request with retry logic.

    Returns raw bytes to handle both JSON and XML responses.

    Args:
        url: Request URL
        params: Query parameters
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        accept: Accept header value (e.g., "application/json")
        timeout: Request timeout in seconds

    Returns:
        Response content as bytes
    """
    import httpx

    last_exception: Exception | None = None

    headers = {
        "Accept-Encoding": "gzip, deflate",
    }
    if accept:
        headers["Accept"] = accept

    for attempt in range(max_retries):
        try:
            client = _get_shared_http_client(timeout=timeout)
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return bytes(response.content)
        except httpx.RemoteProtocolError as e:
            last_exception = e
            if attempt < max_retries - 1:
                _reset_http_client()
                time.sleep(retry_delay * (attempt + 1))
        except httpx.TimeoutException as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.NetworkError as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                last_exception = e
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = (
                        float(retry_after)
                        if retry_after and retry_after.isdigit()
                        else retry_delay * (attempt + 1)
                    )
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise WFSError("Authentication required. WFS server requires credentials.") from e
            elif status == 403:
                raise WFSError("Access denied. Check your permissions for this WFS service.") from e
            elif status == 404:
                raise WFSError(f"WFS service not found (404). Check the URL: {url}") from e
            raise WFSError(f"HTTP error {status}: {e}") from e

    raise WFSError(f"Request failed after {max_retries} attempts: {last_exception}")


def _clean_service_url(url: str) -> str:
    """
    Clean WFS service URL by removing GetCapabilities parameters.

    Some URLs come with ?service=WFS&request=GetCapabilities which
    interferes with subsequent requests.

    Args:
        url: Input URL (may include query parameters)

    Returns:
        Clean base URL for the WFS service
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    # Remove WFS-specific params that shouldn't persist
    for key in ["service", "request", "version", "typename", "typenames"]:
        params.pop(key, None)
        params.pop(key.upper(), None)

    # Rebuild URL
    new_query = urlencode(params, doseq=True) if params else ""
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            "",
        )
    )


def get_wfs_capabilities(service_url: str, version: str = "1.1.0"):
    """
    Get WFS capabilities using OWSLib.

    Args:
        service_url: WFS service URL
        version: WFS version (1.0.0 or 1.1.0)

    Returns:
        OWSLib WebFeatureService object
    """
    try:
        from owslib.wfs import WebFeatureService
    except ImportError as e:
        raise WFSError(
            "owslib is required for WFS extraction. Install with: pip install owslib"
        ) from e

    clean_url = _clean_service_url(service_url)

    try:
        wfs = WebFeatureService(url=clean_url, version=version)
        return wfs
    except Exception as e:
        error_msg = str(e).lower()
        if "connection" in error_msg or "timeout" in error_msg:
            raise WFSError(f"Could not connect to WFS service: {clean_url}\nError: {e}") from e
        elif "xml" in error_msg or "parse" in error_msg:
            raise WFSError(
                f"Invalid WFS response from: {clean_url}\n"
                f"The server may not be a valid WFS service. Error: {e}"
            ) from e
        else:
            raise WFSError(f"Failed to get WFS capabilities: {e}") from e


def _normalize_crs(crs: str) -> str:
    """
    Normalize CRS string to consistent EPSG format.

    Handles variants like:
    - EPSG:4326
    - urn:ogc:def:crs:EPSG::4326
    - http://www.opengis.net/def/crs/EPSG/0/4326

    Returns:
        Normalized EPSG string (e.g., "EPSG:4326")
    """
    import re

    # Already in simple format
    if re.match(r"^EPSG:\d+$", crs, re.IGNORECASE):
        return crs.upper()

    # URN format: urn:ogc:def:crs:EPSG::4326
    urn_match = re.search(r"EPSG::?(\d+)", crs, re.IGNORECASE)
    if urn_match:
        return f"EPSG:{urn_match.group(1)}"

    # HTTP format: http://www.opengis.net/def/crs/EPSG/0/4326
    http_match = re.search(r"EPSG/\d+/(\d+)", crs, re.IGNORECASE)
    if http_match:
        return f"EPSG:{http_match.group(1)}"

    # CRS84 is equivalent to EPSG:4326 (axis order differs but we handle that)
    if "CRS84" in crs.upper() or "CRS:84" in crs.upper():
        return "EPSG:4326"

    # Return as-is if no pattern matches
    return crs


def _crs_matches(crs1: str, crs2: str) -> bool:
    """Check if two CRS strings represent the same coordinate system."""
    return _normalize_crs(crs1) == _normalize_crs(crs2)


def _detect_geometry_column(wfs, typename: str) -> str:
    """
    Detect geometry column name from DescribeFeatureType.

    Args:
        wfs: OWSLib WebFeatureService object
        typename: Layer typename

    Returns:
        Geometry column name (default: "geometry")
    """
    try:
        schema = wfs.get_schema(typename)
        if schema and "geometry" in schema:
            return str(schema.get("geometry_column", "geometry"))
        # Check for common geometry column names in properties
        if schema and "properties" in schema:
            for prop_name, prop_type in schema["properties"].items():
                if any(
                    geom in str(prop_type).lower()
                    for geom in ["geometry", "point", "line", "polygon", "multi"]
                ):
                    return str(prop_name)
    except Exception:
        pass  # Fall back to default

    return "geometry"


def get_layer_info(service_url: str, typename: str, version: str = "1.1.0") -> WFSLayerInfo:
    """
    Get metadata for a specific WFS layer.

    Args:
        service_url: WFS service URL
        typename: Layer typename (with or without namespace prefix)
        version: WFS version

    Returns:
        WFSLayerInfo dataclass with layer metadata
    """
    wfs = get_wfs_capabilities(service_url, version)

    # Find the layer (handle namespace variations)
    layer = None
    matched_typename = typename

    if typename in wfs.contents:
        layer = wfs.contents[typename]
    else:
        # Try without namespace prefix
        short_name = typename.split(":")[-1] if ":" in typename else typename
        for key in wfs.contents:
            if key.endswith(f":{short_name}") or key == short_name:
                layer = wfs.contents[key]
                matched_typename = key
                break

    if layer is None:
        available = list(wfs.contents.keys())[:10]
        hint = f"\nAvailable layers (first 10): {', '.join(available)}" if available else ""
        raise WFSError(f"Layer '{typename}' not found in WFS service.{hint}")

    # Extract CRS list
    crs_list = []
    default_crs = None

    if hasattr(layer, "crsOptions") and layer.crsOptions:
        crs_list = [str(crs) for crs in layer.crsOptions]
        default_crs = crs_list[0] if crs_list else None

    # Extract bounding box
    bbox = None
    if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84:
        bbox = tuple(layer.boundingBoxWGS84)

    # Detect geometry column
    geometry_column = _detect_geometry_column(wfs, matched_typename)

    # Get available output formats
    available_formats = []
    if hasattr(wfs, "getfeature_output_formats"):
        available_formats = list(wfs.getfeature_output_formats)
    elif hasattr(wfs, "capabilities") and wfs.capabilities:
        # Try to extract from capabilities XML
        try:
            from owslib.util import nspath_eval

            ns = wfs.namespaces
            getfeature = wfs.capabilities.find(
                nspath_eval("wfs:Capability/wfs:Request/wfs:GetFeature", ns)
            )
            if getfeature is not None:
                for fmt in getfeature.findall(nspath_eval("wfs:ResultFormat/*", ns)):
                    available_formats.append(fmt.tag.split("}")[-1])
        except Exception:
            pass

    return WFSLayerInfo(
        typename=matched_typename,
        title=getattr(layer, "title", None),
        crs_list=crs_list,
        default_crs=default_crs,
        bbox=bbox,
        geometry_column=geometry_column,
        available_formats=available_formats,
    )


def list_available_layers(service_url: str, version: str = "1.1.0") -> list[dict]:
    """
    List available layers in a WFS service.

    Args:
        service_url: WFS service URL
        version: WFS version

    Returns:
        List of dicts with layer info (name, typename, title, abstract, bbox)
    """
    wfs = get_wfs_capabilities(service_url, version)

    layers = []
    for typename, layer in wfs.contents.items():
        layers.append(
            {
                "name": typename,  # Alias for consistency with CLI
                "typename": typename,
                "title": getattr(layer, "title", None),
                "abstract": getattr(layer, "abstract", None),
                "bbox": tuple(layer.boundingBoxWGS84)
                if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84
                else None,
            }
        )

    return layers


def _detect_best_output_format(available_formats: list[str]) -> str:
    """
    Detect the best output format from available formats.

    Prefers GeoJSON for faster parsing, falls back to GML.

    Args:
        available_formats: List of format strings from capabilities

    Returns:
        Best format string to request
    """
    available_lower = [f.lower() for f in available_formats]

    # Check for GeoJSON formats (preferred - faster to parse)
    for fmt in GEOJSON_FORMATS:
        if fmt.lower() in available_lower:
            idx = available_lower.index(fmt.lower())
            return available_formats[idx]

    # Check for GML formats
    for fmt in GML_FORMATS:
        if fmt.lower() in available_lower:
            idx = available_lower.index(fmt.lower())
            return available_formats[idx]

    # Fallback to first available or default
    return available_formats[0] if available_formats else "GML3"


def _negotiate_crs(layer_info: WFSLayerInfo, output_crs: str | None = None) -> str:
    """
    Negotiate the best CRS to request from the WFS server.

    Strategy:
    1. If output_crs specified and supported -> use it
    2. Try EPSG:4326 variants (most universal)
    3. Fall back to server default

    Args:
        layer_info: Layer metadata with CRS list
        output_crs: User-requested output CRS

    Returns:
        CRS string to use in requests
    """
    crs_list = layer_info.crs_list

    # If user specified CRS, check if supported
    if output_crs:
        for crs in crs_list:
            if _crs_matches(crs, output_crs):
                debug(f"Using requested CRS: {crs}")
                return crs
        warn(f"Requested CRS '{output_crs}' not in layer's CRS list. Using server default.")

    # Try EPSG:4326 variants
    for crs in crs_list:
        if _crs_matches(crs, "EPSG:4326"):
            debug(f"Using WGS84: {crs}")
            return crs

    # Fall back to default
    if layer_info.default_crs:
        debug(f"Using server default CRS: {layer_info.default_crs}")
        return layer_info.default_crs

    # Last resort
    if crs_list:
        debug(f"Using first available CRS: {crs_list[0]}")
        return crs_list[0]

    return "EPSG:4326"


def _determine_bbox_strategy(
    bbox_mode: str,
    layer_info: WFSLayerInfo,
) -> bool:
    """
    Determine whether to use server-side bbox filtering.

    Unlike BigQuery, WFS doesn't easily expose row counts, so auto mode
    defaults to server-side filtering (conservative for remote services).

    Args:
        bbox_mode: "auto", "server", or "local"
        layer_info: Layer metadata (reserved for future use)

    Returns:
        True if server-side filtering should be used
    """
    # layer_info reserved for future use (e.g., checking server capabilities)
    _ = layer_info

    if bbox_mode == "server":
        debug("Using server-side bbox filter (forced by --bbox-mode server)")
        return True
    if bbox_mode == "local":
        debug("Using local bbox filter (forced by --bbox-mode local)")
        return False

    # Auto mode: default to server-side for WFS
    # WFS servers typically handle spatial filtering efficiently
    debug("Using server-side bbox filter (auto mode for WFS)")
    return True


def _build_bbox_param(
    bbox: tuple[float, float, float, float],
    crs: str,
    version: str = "1.1.0",
) -> str:
    """
    Build WFS bbox parameter string.

    WFS 1.0.0: xmin,ymin,xmax,ymax
    WFS 1.1.0: xmin,ymin,xmax,ymax,crs

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax)
        crs: Coordinate reference system
        version: WFS version

    Returns:
        Bbox parameter string
    """
    xmin, ymin, xmax, ymax = bbox

    if version == "1.0.0":
        return f"{xmin},{ymin},{xmax},{ymax}"
    else:
        # WFS 1.1.0+ includes CRS
        return f"{xmin},{ymin},{xmax},{ymax},{crs}"


def _validate_identifier(name: str) -> str:
    """
    Validate and sanitize a SQL identifier (column name).

    Args:
        name: Column name to validate

    Returns:
        Validated column name

    Raises:
        WFSError: If the name contains invalid characters
    """
    # Only allow alphanumeric, underscore, and standard identifier characters
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        # Check for dangerous patterns
        if '"' in name or "'" in name or ";" in name or "--" in name:
            raise WFSError(f"Invalid geometry column name '{name}': contains unsafe characters")
        # Allow dots for qualified names but escape them
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$", name):
            raise WFSError(f"Invalid geometry column name '{name}': must be a valid identifier")
    return name


def _build_local_bbox_filter(
    bbox: tuple[float, float, float, float],
    geometry_column: str,
) -> str:
    """
    Build DuckDB SQL filter for local bbox filtering.

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax)
        geometry_column: Name of geometry column

    Returns:
        DuckDB ST_Intersects SQL condition

    Raises:
        WFSError: If geometry column name is invalid
    """
    # Validate column name to prevent SQL injection
    safe_column = _validate_identifier(geometry_column)

    xmin, ymin, xmax, ymax = bbox
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    return f"ST_Intersects(\"{safe_column}\", ST_GeomFromText('{wkt}'))"


def _get_feature_count(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
) -> int | None:
    """
    Try to get feature count using resultType=hits.

    This is a WFS 1.1.0+ feature that returns count without fetching data.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version

    Returns:
        Feature count or None if not supported
    """
    if version == "1.0.0":
        return None  # resultType=hits not supported in WFS 1.0.0

    clean_url = _clean_service_url(service_url)
    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeName" if version == "1.0.0" else "typeNames": typename,
        "resultType": "hits",
    }

    try:
        content = _make_request(clean_url, params=params)

        # Parse XML response to find numberOfFeatures
        import re

        match = re.search(rb'numberOfFeatures="(\d+)"', content)
        if match:
            return int(match.group(1))

        # Try alternative attribute name
        match = re.search(rb'numberMatched="(\d+)"', content)
        if match:
            return int(match.group(1))

    except Exception:
        pass

    return None


def fetch_features_page(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    offset: int = 0,
    page_size: int = DEFAULT_PAGE_SIZE,
    output_format: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
) -> bytes:
    """
    Fetch a single page of features from WFS.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        offset: Starting feature index
        page_size: Number of features to fetch
        output_format: Requested output format
        bbox: Optional bounding box filter
        crs: CRS for bbox parameter

    Returns:
        Raw response content (JSON or GML bytes)
    """
    clean_url = _clean_service_url(service_url)

    # Build request parameters
    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeName" if version == "1.0.0" else "typeNames": typename,
        "maxFeatures" if version == "1.0.0" else "count": str(page_size),
    }

    # Add pagination (WFS 1.1.0+)
    if version != "1.0.0" and offset > 0:
        params["startIndex"] = str(offset)

    # Add output format
    if output_format:
        params["outputFormat"] = output_format

    # Add bbox filter
    if bbox and crs:
        params["bbox"] = _build_bbox_param(bbox, crs, version)

    # Determine Accept header based on format
    accept = None
    if output_format:
        if any(fmt in output_format.lower() for fmt in ["json", "geo"]):
            accept = "application/json"
        else:
            accept = "text/xml"

    return _make_request(clean_url, params=params, accept=accept)


def fetch_all_features(
    service_url: str,
    layer_info: WFSLayerInfo,
    version: str = "1.1.0",
    output_format: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_server_bbox: bool = True,
    crs: str | None = None,
    max_features: int | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_workers: int = 1,
) -> Generator[bytes, None, None]:
    """
    Generator that yields pages of WFS features.

    Handles pagination using startIndex/count parameters.

    Args:
        service_url: WFS service URL
        layer_info: Layer metadata
        version: WFS version
        output_format: Requested output format
        bbox: Bounding box filter
        use_server_bbox: Whether to apply bbox server-side
        crs: CRS for requests
        max_features: Maximum total features to return
        page_size: Features per page
        max_workers: Concurrent request workers (1 = sequential)

    Yields:
        Raw response bytes for each page
    """
    # Validate max_workers
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_workers > MAX_RECOMMENDED_WORKERS:
        warn(
            f"max_workers={max_workers} may trigger rate limits. "
            f"Recommended range: 1-{MAX_RECOMMENDED_WORKERS}"
        )

    # Try to get total count for progress reporting
    total_count = _get_feature_count(service_url, layer_info.typename, version)
    if total_count is not None:
        debug(f"Server reports {total_count:,} features")
    if max_features is not None and total_count is not None:
        total_count = min(total_count, max_features)

    # Determine bbox to use in requests
    request_bbox = bbox if (bbox and use_server_bbox) else None

    if max_workers == 1:
        # Sequential fetching
        offset = 0
        fetched = 0
        page_num = 0

        while True:
            # Calculate remaining if we have a limit
            remaining = max_features - fetched if max_features else page_size
            current_size = min(page_size, remaining)

            if current_size <= 0:
                break

            # Progress message
            end = offset + current_size
            if total_count:
                progress(f"Fetching features {offset + 1}-{end} of {total_count:,}...")
            else:
                progress(f"Fetching features {offset + 1}-{end}...")

            content = fetch_features_page(
                service_url,
                layer_info.typename,
                version=version,
                offset=offset,
                page_size=current_size,
                output_format=output_format,
                bbox=request_bbox,
                crs=crs,
            )

            # Check if we got any features using reliable detection
            if not _response_has_features(content):
                break

            yield content

            # Parse to count actual features for accurate tracking
            actual_count = _count_features_in_response(content)
            page_num += 1
            fetched += actual_count if actual_count > 0 else current_size
            offset += current_size

            # WFS 1.0.0 doesn't support pagination - single page only
            if version == "1.0.0":
                break

            # Check if we've hit the limit
            if max_features and fetched >= max_features:
                break

        debug(f"Fetched {fetched:,} features in {page_num} pages")

    else:
        # Parallel fetching with ThreadPoolExecutor
        fetched = 0
        batch_start = 0
        page_num = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                # Check if we've hit limit
                if max_features and fetched >= max_features:
                    break

                # Submit parallel requests
                futures = []

                for i in range(max_workers):
                    offset = batch_start + (i * page_size)

                    # Respect limit
                    if max_features:
                        remaining = max_features - (fetched + i * page_size)
                        if remaining <= 0:
                            break
                        current_size = min(page_size, remaining)
                    else:
                        current_size = page_size

                    end = offset + current_size
                    if total_count:
                        progress(f"Fetching features {offset + 1}-{end} of {total_count:,}...")
                    else:
                        progress(f"Fetching features {offset + 1}-{end}...")

                    future = executor.submit(
                        fetch_features_page,
                        service_url,
                        layer_info.typename,
                        version=version,
                        offset=offset,
                        page_size=current_size,
                        output_format=output_format,
                        bbox=request_bbox,
                        crs=crs,
                    )
                    futures.append((offset, future))

                if not futures:
                    break

                # Collect results in order
                results = []
                has_content = False

                for offset, future in futures:
                    try:
                        content = future.result()
                        if _response_has_features(content):
                            results.append((offset, content))
                            has_content = True
                    except Exception as e:
                        raise WFSError(f"Failed to fetch features at offset {offset}: {e}") from e

                if not has_content:
                    break

                # Sort by offset and yield in order
                results.sort(key=lambda x: x[0])
                batch_feature_count = 0
                for _offset, content in results:
                    yield content
                    page_num += 1
                    # Count actual features for accurate tracking (fallback to page_size per response)
                    count = _count_features_in_response(content)
                    batch_feature_count += count if count > 0 else page_size

                fetched += batch_feature_count
                batch_start += max_workers * page_size

                # WFS 1.0.0 doesn't support pagination
                if version == "1.0.0":
                    break

        debug(f"Fetched features in {page_num} pages using {max_workers} workers")


def _is_geojson_response(content: bytes) -> bool:
    """Check if response content appears to be GeoJSON."""
    try:
        # Quick check for JSON structure
        stripped = content.strip()
        return stripped.startswith(b"{") and b'"type"' in content
    except Exception:
        return False


def _response_has_features(content: bytes) -> bool:
    """
    Check if a WFS response contains actual features.

    This is more reliable than checking byte length, as empty
    FeatureCollections can exceed 50 bytes.

    Args:
        content: Raw WFS response bytes

    Returns:
        True if response contains at least one feature
    """
    if not content:
        return False

    # Check for GeoJSON empty response
    if _is_geojson_response(content):
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                # Check for empty FeatureCollection
                if data.get("type") == "FeatureCollection":
                    features = data.get("features", [])
                    return len(features) > 0
                # Check numberReturned attribute (WFS 2.0 style)
                if data.get("numberReturned") == 0:
                    return False
                # Single feature is valid
                if data.get("type") == "Feature":
                    return True
            return True  # Assume has content if we can't determine
        except json.JSONDecodeError:
            return False

    # Check for GML empty response
    # Look for numberOfFeatures="0" or empty featureMember
    if b'numberOfFeatures="0"' in content:
        return False
    if b'numberReturned="0"' in content:
        return False

    # Check if there's actual feature content (very basic heuristic)
    # GML responses should have featureMember elements
    if b"<gml:featureMember" in content or b"<wfs:member" in content:
        return True

    # For minimal responses, check for common empty indicators
    if len(content) < 200:
        # Very short response - likely empty or error
        if b"<wfs:FeatureCollection" in content:
            # It's a FeatureCollection but very short - probably empty
            if b"featureMember" not in content and b"member>" not in content:
                return False

    # Default: assume it has features if we got a non-trivial response
    return len(content) > 100


def _count_features_in_response(content: bytes) -> int:
    """
    Count the number of features in a WFS response.

    Used for accurate progress tracking instead of assuming page_size.

    Args:
        content: Raw WFS response bytes

    Returns:
        Number of features, or 0 if unable to determine
    """
    if not content:
        return 0

    # Try GeoJSON first (most accurate)
    if _is_geojson_response(content):
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                if data.get("type") == "FeatureCollection":
                    features = data.get("features", [])
                    return len(features)
                # Check numberReturned (WFS 2.0 style)
                if "numberReturned" in data:
                    return int(data["numberReturned"])
                if data.get("type") == "Feature":
                    return 1
        except (json.JSONDecodeError, ValueError):
            pass
        return 0

    # Try to extract count from GML attributes
    # Look for numberOfFeatures or numberReturned
    match = re.search(rb'numberOfFeatures="(\d+)"', content)
    if match:
        return int(match.group(1))

    match = re.search(rb'numberReturned="(\d+)"', content)
    if match:
        return int(match.group(1))

    # Count featureMember elements as fallback
    count = content.count(b"<gml:featureMember")
    if count == 0:
        count = content.count(b"<wfs:member")
    return count


def _parse_geojson_features(content: bytes) -> list[dict]:
    """
    Parse GeoJSON response and extract features.

    Args:
        content: Raw GeoJSON bytes

    Returns:
        List of feature dicts
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise WFSError(f"Failed to parse GeoJSON response: {e}") from e

    if isinstance(data, dict):
        if data.get("type") == "FeatureCollection":
            features = data.get("features", [])
            return list(features) if features else []
        elif data.get("type") == "Feature":
            return [dict(data)]
        elif "features" in data:
            return list(data["features"])

    return []


def _geojson_to_arrow_table(features: list[dict]) -> pa.Table | None:
    """
    Convert GeoJSON features to PyArrow Table with WKB geometry.

    Uses DuckDB's spatial extension for geometry conversion.

    Args:
        features: List of GeoJSON feature dicts

    Returns:
        PyArrow Table with WKB geometry column, or None if empty
    """
    if not features:
        return None

    # Create temporary GeoJSON file for DuckDB
    geojson_collection = json.dumps(
        {
            "type": "FeatureCollection",
            "features": features,
        }
    )

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, f"wfs_page_{uuid.uuid4()}.geojson")

    try:
        with open(temp_file, "w") as f:
            f.write(geojson_collection)

        # Read GeoJSON and convert geometry to WKB
        query = f"""
            SELECT
                ST_AsWKB(geom) as geometry,
                * EXCLUDE (geom, OGC_FID)
            FROM ST_Read('{temp_file}')
        """

        result = con.execute(query).arrow()
        return result.read_all()

    finally:
        con.close()
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def _sanitize_filename(typename: str) -> str:
    """
    Sanitize a typename for use in temp filenames.

    Removes path traversal patterns and unsafe characters.

    Args:
        typename: Layer typename from WFS

    Returns:
        Safe filename component
    """
    # Remove namespace prefix and any path-like components
    name = typename.split(":")[-1] if ":" in typename else typename

    # Remove path traversal patterns
    name = name.replace("..", "")
    name = name.replace("/", "_")
    name = name.replace("\\", "_")

    # Only keep alphanumeric and underscore
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Ensure it has meaningful content (not just underscores)
    # Strip underscores to check if any alphanumeric content remains
    has_content = safe_name.strip("_")
    return safe_name if has_content else "layer"


def _gml_to_arrow_table(content: bytes, typename: str) -> pa.Table | None:
    """
    Convert GML response to PyArrow Table with WKB geometry.

    Uses DuckDB's GDAL-based ST_Read for GML parsing.

    Args:
        content: Raw GML bytes
        typename: Layer typename (used for temp file naming)

    Returns:
        PyArrow Table with WKB geometry column, or None if empty
    """
    if not content or len(content) < 100:
        return None

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    # Use .xml extension - DuckDB/GDAL can auto-detect GML
    safe_name = _sanitize_filename(typename)
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, f"wfs_gml_{safe_name}_{uuid.uuid4()}.xml")

    try:
        with open(temp_file, "wb") as f:
            f.write(content)

        # Read GML and convert geometry to WKB
        query = f"""
            SELECT
                ST_AsWKB(geom) as geometry,
                * EXCLUDE (geom, OGC_FID)
            FROM ST_Read('{temp_file}')
        """

        result = con.execute(query).arrow()
        table = result.read_all()

        # Check if we got any rows
        if table.num_rows == 0:
            return None

        return table

    except Exception as e:
        debug(f"GML parsing error: {e}")
        # Try to provide helpful error message
        if "unsupported" in str(e).lower() or "driver" in str(e).lower():
            raise WFSError(
                f"Could not parse GML response. The format may not be supported.\n"
                f"Try using --output-format with a GeoJSON option if available.\n"
                f"Error: {e}"
            ) from e
        raise

    finally:
        con.close()
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def _parse_response_to_table(
    content: bytes,
    typename: str,
) -> pa.Table | None:
    """
    Parse WFS response (GeoJSON or GML) to Arrow table.

    Args:
        content: Raw response bytes
        typename: Layer typename

    Returns:
        PyArrow Table or None if empty
    """
    if _is_geojson_response(content):
        features = _parse_geojson_features(content)
        return _geojson_to_arrow_table(features)
    else:
        return _gml_to_arrow_table(content, typename)


def _stream_features_to_parquet(
    service_url: str,
    layer_info: WFSLayerInfo,
    output_path: str,
    version: str = "1.1.0",
    output_format: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_server_bbox: bool = True,
    crs: str | None = None,
    max_features: int | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_workers: int = 1,
) -> int:
    """
    Stream WFS features to a Parquet file page by page.

    Memory-efficient: only keeps one page in memory at a time.

    Args:
        service_url: WFS service URL
        layer_info: Layer metadata
        output_path: Output Parquet file path
        version: WFS version
        output_format: Requested output format
        bbox: Bounding box filter
        use_server_bbox: Whether to apply bbox server-side
        crs: CRS for requests
        max_features: Maximum total features
        page_size: Features per page
        max_workers: Concurrent workers

    Returns:
        Number of features written
    """
    writer = None
    target_schema = None
    total_rows = 0
    page_count = 0

    try:
        for content in fetch_all_features(
            service_url,
            layer_info,
            version=version,
            output_format=output_format,
            bbox=bbox,
            use_server_bbox=use_server_bbox,
            crs=crs,
            max_features=max_features,
            page_size=page_size,
            max_workers=max_workers,
        ):
            # Parse this page
            page_table = _parse_response_to_table(content, layer_info.typename)
            if page_table is None or page_table.num_rows == 0:
                continue

            page_count += 1

            # Initialize schema from first page
            if target_schema is None:
                target_schema = page_table.schema
                writer = pq.ParquetWriter(output_path, target_schema)

            # Cast to fixed schema if needed
            if page_table.schema != target_schema:
                try:
                    page_table = page_table.cast(target_schema, safe=True)
                except pa.ArrowInvalid as e:
                    raise WFSError(
                        f"Schema mismatch in page {page_count}. "
                        f"This may indicate inconsistent data from the WFS service. "
                        f"Error: {e}"
                    ) from e

            assert writer is not None  # Initialized above with target_schema
            writer.write_table(page_table)
            total_rows += page_table.num_rows

            # Free memory
            del page_table

        debug(f"Streamed {total_rows:,} features in {page_count} pages")
        return total_rows

    finally:
        if writer is not None:
            writer.close()


def wfs_to_table(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_workers: int = 1,
    verbose: bool = False,
) -> pa.Table:
    """
    Fetch WFS layer as PyArrow Table.

    Uses a memory-efficient two-pass approach:
    1. Stream features page-by-page to a temp parquet file
    2. Read the parquet file back as an Arrow table

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version (1.0.0 or 1.1.0)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        bbox_mode: Bbox strategy ("auto", "server", "local")
        output_crs: Request specific CRS (e.g., "EPSG:4326")
        limit: Maximum features to fetch
        page_size: Features per request
        max_workers: Concurrent requests (1 = sequential)
        verbose: Enable debug output

    Returns:
        PyArrow Table with GeoParquet-compatible geometry
    """
    configure_verbose(verbose)

    # Get layer info
    info("Connecting to WFS service...")
    layer_info = get_layer_info(service_url, typename, version)

    debug(f"Layer: {layer_info.typename}")
    debug(f"Title: {layer_info.title}")
    debug(f"Available CRS: {len(layer_info.crs_list)} options")
    debug(f"Available formats: {layer_info.available_formats}")

    # Negotiate CRS
    crs = _negotiate_crs(layer_info, output_crs)

    # Detect best output format
    output_format = _detect_best_output_format(layer_info.available_formats)
    debug(f"Using output format: {output_format}")

    # Determine bbox strategy
    use_server_bbox = True
    if bbox:
        use_server_bbox = _determine_bbox_strategy(bbox_mode, layer_info)

    # Stream to temp file
    temp_dir = tempfile.gettempdir()
    temp_file = f"{temp_dir}/wfs_{uuid.uuid4()}.parquet"

    try:
        row_count = _stream_features_to_parquet(
            service_url,
            layer_info,
            temp_file,
            version=version,
            output_format=output_format,
            bbox=bbox if use_server_bbox else None,
            use_server_bbox=use_server_bbox,
            crs=crs,
            max_features=limit,
            page_size=page_size,
            max_workers=max_workers,
        )

        if row_count == 0:
            raise WFSError(
                f"No features returned from WFS service for layer '{typename}'.\n"
                "Check that the layer exists and the bbox (if specified) intersects data."
            )

        # Read temp file as Arrow table
        table = pq.read_table(temp_file)

        # Apply local bbox filter if needed
        if bbox and not use_server_bbox:
            debug("Applying local bbox filter...")
            filter_sql = _build_local_bbox_filter(bbox, "geometry")
            con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
            try:
                con.register("features", table)
                filtered = con.execute(f"SELECT * FROM features WHERE {filter_sql}").arrow()
                table = filtered.read_all()
                debug(f"After local filter: {table.num_rows:,} features")
            finally:
                con.close()

        # Add CRS metadata to schema
        projjson = parse_crs_string_to_projjson(_normalize_crs(crs))
        if projjson:
            geo_meta = {
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "crs": projjson,
                    }
                },
            }
            existing_meta = table.schema.metadata or {}
            existing_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")
            table = table.replace_schema_metadata(existing_meta)

        success(f"Fetched {table.num_rows:,} features from WFS")
        return table

    finally:
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def convert_wfs_to_geoparquet(
    service_url: str,
    typename: str,
    output_file: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_workers: int = 1,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> None:
    """
    Extract WFS layer and save as optimized GeoParquet.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        output_file: Output GeoParquet file path
        version: WFS version
        bbox: Bounding box filter
        bbox_mode: Bbox strategy
        output_crs: Request specific CRS
        limit: Maximum features
        page_size: Features per request
        max_workers: Concurrent workers
        skip_hilbert: Skip Hilbert curve sorting
        skip_bbox: Skip adding bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable debug output
    """
    configure_verbose(verbose)

    # Check output file
    output_path = Path(output_file)
    if output_path.exists() and not overwrite:
        raise WFSError(f"Output file exists: {output_file}\nUse --overwrite to replace it.")

    # Fetch data
    table = wfs_to_table(
        service_url,
        typename,
        version=version,
        bbox=bbox,
        bbox_mode=bbox_mode,
        output_crs=output_crs,
        limit=limit,
        page_size=page_size,
        max_workers=max_workers,
        verbose=verbose,
    )

    # Apply Hilbert ordering (unless skipped)
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert curve ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table, geometry_column="geometry")
        debug("Hilbert sort complete")

    # Add bbox column (unless skipped)
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column...")
        from geoparquet_io.core.add_bbox_column import add_bbox_table

        table = add_bbox_table(table, geometry_column="geometry")
        debug("Bbox column added")

    # Write output
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
    )

    success(f"Wrote {table.num_rows:,} features to {output_file}")
