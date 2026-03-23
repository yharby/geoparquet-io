"""
Tests for WFS (Web Feature Service) extraction.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

from json import dumps as json_dumps
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# =============================================================================
# Mock WFS Response Data
# =============================================================================

# WFS 1.1.0 GetCapabilities response with 2 feature types (cities, roads)
MOCK_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:gml="http://www.opengis.net/gml"
    version="1.1.0">

    <ows:ServiceIdentification>
        <ows:Title>Mock WFS Server</ows:Title>
        <ows:Abstract>A mock WFS server for testing geoparquet-io</ows:Abstract>
        <ows:ServiceType>WFS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.1.0</ows:ServiceTypeVersion>
    </ows:ServiceIdentification>

    <ows:ServiceProvider>
        <ows:ProviderName>Test Provider</ows:ProviderName>
    </ows:ServiceProvider>

    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="DescribeFeatureType">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="GetFeature">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="outputFormat">
                <ows:Value>text/xml; subtype=gml/3.1.1</ows:Value>
                <ows:Value>application/json</ows:Value>
                <ows:Value>application/geo+json</ows:Value>
            </ows:Parameter>
        </ows:Operation>
    </ows:OperationsMetadata>

    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>test:cities</wfs:Name>
            <wfs:Title>Cities</wfs:Title>
            <wfs:Abstract>Major cities dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OtherSRS>urn:ogc:def:crs:EPSG::3857</wfs:OtherSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
        <wfs:FeatureType>
            <wfs:Name>test:roads</wfs:Name>
            <wfs:Title>Roads</wfs:Title>
            <wfs:Abstract>Road network dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-125.0 24.0</ows:LowerCorner>
                <ows:UpperCorner>-66.0 50.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>

    <ogc:Filter_Capabilities>
        <ogc:Spatial_Capabilities>
            <ogc:GeometryOperands>
                <ogc:GeometryOperand>gml:Envelope</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Point</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Polygon</ogc:GeometryOperand>
            </ogc:GeometryOperands>
            <ogc:SpatialOperators>
                <ogc:SpatialOperator name="BBOX"/>
                <ogc:SpatialOperator name="Intersects"/>
                <ogc:SpatialOperator name="Within"/>
            </ogc:SpatialOperators>
        </ogc:Spatial_Capabilities>
        <ogc:Scalar_Capabilities>
            <ogc:LogicalOperators/>
            <ogc:ComparisonOperators>
                <ogc:ComparisonOperator>EqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>NotEqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>LessThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>GreaterThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>Like</ogc:ComparisonOperator>
            </ogc:ComparisonOperators>
        </ogc:Scalar_Capabilities>
    </ogc:Filter_Capabilities>
</wfs:WFS_Capabilities>
"""

# GeoJSON FeatureCollection with 3 point features
MOCK_GEOJSON_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {
                "gml_id": "cities.1",
                "name": "San Francisco",
                "population": 884363,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {
                "gml_id": "cities.2",
                "name": "New York",
                "population": 8336817,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {
                "gml_id": "cities.3",
                "name": "London",
                "population": 8982000,
                "country": "UK",
            },
        },
    ],
    "totalFeatures": 3,
    "numberMatched": 3,
    "numberReturned": 3,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}

# GML3 response with 1 feature (WFS 1.1.0 default format)
MOCK_GML_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    numberOfFeatures="1"
    timeStamp="2026-03-23T12:00:00Z">

    <gml:boundedBy>
        <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
            <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
            <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
        </gml:Envelope>
    </gml:boundedBy>

    <gml:featureMember>
        <test:cities gml:id="cities.1">
            <gml:boundedBy>
                <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
                    <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
                </gml:Envelope>
            </gml:boundedBy>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>37.7749 -122.4194</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>San Francisco</test:name>
            <test:population>884363</test:population>
            <test:country>USA</test:country>
        </test:cities>
    </gml:featureMember>
</wfs:FeatureCollection>
"""

# XSD schema response for DescribeFeatureType
MOCK_DESCRIBE_FEATURE_TYPE = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    targetNamespace="http://mock.wfs.server/test"
    elementFormDefault="qualified">

    <xsd:import namespace="http://www.opengis.net/gml"
        schemaLocation="http://schemas.opengis.net/gml/3.1.1/base/gml.xsd"/>

    <xsd:complexType name="citiesType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:PointPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="population" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="country" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="cities" type="test:citiesType"
        substitutionGroup="gml:_Feature"/>

    <xsd:complexType name="roadsType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:MultiLineStringPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="highway_type" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="lanes" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="roads" type="test:roadsType"
        substitutionGroup="gml:_Feature"/>
</xsd:schema>
"""

# Empty FeatureCollection response
MOCK_EMPTY_RESPONSE = {
    "type": "FeatureCollection",
    "features": [],
    "totalFeatures": 0,
    "numberMatched": 0,
    "numberReturned": 0,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture
def mock_capabilities_xml():
    """Return WFS 1.1.0 GetCapabilities XML response.

    Contains 2 feature types:
    - test:cities (Point geometry, global extent)
    - test:roads (LineString geometry, USA extent)

    Supports GML 3.1.1 and JSON output formats.
    """
    return MOCK_CAPABILITIES_XML


@pytest.fixture
def mock_geojson_response():
    """Return GeoJSON FeatureCollection with 3 point features.

    Features represent cities:
    - San Francisco (population: 884363)
    - New York (population: 8336817)
    - London (population: 8982000)

    CRS is EPSG:4326 (WGS84).
    """
    return MOCK_GEOJSON_RESPONSE


@pytest.fixture
def mock_gml_response():
    """Return GML3 response with 1 feature.

    Contains a single city (San Francisco) in GML 3.1.1 format.
    This is the default WFS 1.1.0 GetFeature response format.
    """
    return MOCK_GML_RESPONSE


@pytest.fixture
def mock_describe_feature_type():
    """Return XSD schema response for DescribeFeatureType.

    Describes 2 feature types:
    - cities: Point geometry with name, population, country fields
    - roads: MultiLineString geometry with name, highway_type, lanes fields
    """
    return MOCK_DESCRIBE_FEATURE_TYPE


@pytest.fixture
def mock_empty_response():
    """Return empty GeoJSON FeatureCollection.

    Represents a WFS GetFeature response with no matching features.
    totalFeatures, numberMatched, and numberReturned are all 0.
    """
    return MOCK_EMPTY_RESPONSE


@pytest.fixture
def mock_wfs_url():
    """Return mock WFS server URL."""
    return "http://mock.wfs.server/wfs"


@pytest.fixture
def mock_wfs_responses(
    mock_wfs_url,
    mock_capabilities_xml,
    mock_geojson_response,
    mock_gml_response,
    mock_describe_feature_type,
    mock_empty_response,
):
    """Return dict of all mock WFS responses keyed by request type.

    Useful for setting up comprehensive request mocking.
    """
    return {
        "url": mock_wfs_url,
        "capabilities": mock_capabilities_xml,
        "geojson": mock_geojson_response,
        "gml": mock_gml_response,
        "describe_feature_type": mock_describe_feature_type,
        "empty": mock_empty_response,
    }


# =============================================================================
# Additional Mock Data for Pagination Tests
# =============================================================================

# First page of 2 features (offset 0)
MOCK_GEOJSON_PAGE_1 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {"gml_id": "cities.1", "name": "San Francisco", "population": 884363},
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {"gml_id": "cities.2", "name": "New York", "population": 8336817},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Second page of 2 features (offset 2)
MOCK_GEOJSON_PAGE_2 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {"gml_id": "cities.3", "name": "London", "population": 8982000},
        },
        {
            "type": "Feature",
            "id": "cities.4",
            "geometry": {"type": "Point", "coordinates": [139.6917, 35.6895]},
            "properties": {"gml_id": "cities.4", "name": "Tokyo", "population": 13960000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Third page with 1 feature (offset 4)
MOCK_GEOJSON_PAGE_3 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.5",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"gml_id": "cities.5", "name": "Paris", "population": 2161000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 1,
}

# Capabilities XML with minimal optional fields (missing Abstract, OtherSRS)
MOCK_MINIMAL_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    version="1.1.0">
    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>minimal:layer</wfs:Name>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>
</wfs:WFS_Capabilities>
"""


@pytest.fixture
def mock_geojson_page_1():
    """First page of paginated results."""
    return MOCK_GEOJSON_PAGE_1


@pytest.fixture
def mock_geojson_page_2():
    """Second page of paginated results."""
    return MOCK_GEOJSON_PAGE_2


@pytest.fixture
def mock_geojson_page_3():
    """Third (final) page of paginated results."""
    return MOCK_GEOJSON_PAGE_3


@pytest.fixture
def mock_minimal_capabilities_xml():
    """Minimal capabilities XML with missing optional fields."""
    return MOCK_MINIMAL_CAPABILITIES_XML


# =============================================================================
# Mock-Based Test Classes
# =============================================================================


class TestCapabilityParsing:
    """Tests for GetCapabilities XML parsing."""

    @patch("geoparquet_io.core.wfs._make_request")
    def test_parses_layer_list(self, mock_request, mock_capabilities_xml):
        """Test that GetCapabilities response is parsed to extract layer list."""
        mock_request.return_value = mock_capabilities_xml
        from geoparquet_io.core.wfs import get_wfs_capabilities

        caps = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Should parse 2 layers from the mock XML
        layer_names = [layer.typename for layer in caps.layers]
        assert "test:cities" in layer_names
        assert "test:roads" in layer_names
        assert len(caps.layers) == 2

    @patch("geoparquet_io.core.wfs._make_request")
    def test_extracts_layer_info(self, mock_request, mock_capabilities_xml):
        """Test extraction of layer info: typename, title, CRS list, bbox."""
        mock_request.return_value = mock_capabilities_xml
        from geoparquet_io.core.wfs import get_wfs_capabilities

        caps = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Find cities layer and verify metadata
        cities_layer = next(layer for layer in caps.layers if layer.typename == "test:cities")

        assert cities_layer.title == "Cities"
        assert "urn:ogc:def:crs:EPSG::4326" in cities_layer.crs_list
        assert "urn:ogc:def:crs:EPSG::3857" in cities_layer.crs_list
        assert cities_layer.default_crs == "urn:ogc:def:crs:EPSG::4326"

        # Check bbox (global extent in mock)
        assert cities_layer.bbox == (-180.0, -90.0, 180.0, 90.0)

        # Check roads layer (USA extent in mock)
        roads_layer = next(layer for layer in caps.layers if layer.typename == "test:roads")
        assert roads_layer.bbox == (-125.0, 24.0, -66.0, 50.0)

    @patch("geoparquet_io.core.wfs._make_request")
    def test_extracts_supported_formats(self, mock_request, mock_capabilities_xml):
        """Test extraction of supported output formats from capabilities."""
        mock_request.return_value = mock_capabilities_xml
        from geoparquet_io.core.wfs import get_wfs_capabilities

        caps = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Check GetFeature operation output formats
        assert "application/json" in caps.output_formats
        assert "application/geo+json" in caps.output_formats
        assert "text/xml; subtype=gml/3.1.1" in caps.output_formats

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_missing_optional_fields(self, mock_request, mock_minimal_capabilities_xml):
        """Test parsing capabilities with missing optional fields (Abstract, OtherSRS)."""
        mock_request.return_value = mock_minimal_capabilities_xml
        from geoparquet_io.core.wfs import get_wfs_capabilities

        caps = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Should still parse successfully with minimal data
        assert len(caps.layers) == 1
        layer = caps.layers[0]

        assert layer.typename == "minimal:layer"
        assert layer.title is None  # Title was not provided
        assert layer.default_crs == "urn:ogc:def:crs:EPSG::4326"
        assert layer.bbox is None  # No bounding box in minimal response


class TestPagination:
    """Tests for pagination logic with mocked HTTP."""

    @patch("geoparquet_io.core.wfs._make_request")
    def test_fetches_single_page(self, mock_request, mock_geojson_response):
        """Test fetching features that fit in a single page."""
        mock_request.return_value = json_dumps(mock_geojson_response)
        from geoparquet_io.core.wfs import fetch_all_features

        pages = list(
            fetch_all_features(
                "http://mock.wfs.server/wfs",
                typename="test:cities",
                page_size=1000,
            )
        )

        # Should make exactly 1 request (all 3 features fit in one page)
        assert len(pages) == 1
        assert pages[0]["numberReturned"] == 3

    @patch("geoparquet_io.core.wfs._make_request")
    def test_fetches_multiple_pages(
        self, mock_request, mock_geojson_page_1, mock_geojson_page_2, mock_geojson_page_3
    ):
        """Test fetching features across multiple pages with different offsets."""
        # Return different pages based on call order
        mock_request.side_effect = [
            json_dumps(mock_geojson_page_1),
            json_dumps(mock_geojson_page_2),
            json_dumps(mock_geojson_page_3),
        ]
        from geoparquet_io.core.wfs import fetch_all_features

        pages = list(
            fetch_all_features(
                "http://mock.wfs.server/wfs",
                typename="test:cities",
                page_size=2,
            )
        )

        # Should fetch 3 pages
        assert len(pages) == 3

        # Verify feature counts per page
        assert pages[0]["numberReturned"] == 2
        assert pages[1]["numberReturned"] == 2
        assert pages[2]["numberReturned"] == 1

        # Verify total features
        total_features = sum(len(p["features"]) for p in pages)
        assert total_features == 5

    @patch("geoparquet_io.core.wfs._make_request")
    def test_respects_limit_parameter(self, mock_request, mock_geojson_page_1):
        """Test that limit parameter caps total features fetched."""
        mock_request.return_value = json_dumps(mock_geojson_page_1)
        from geoparquet_io.core.wfs import fetch_all_features

        pages = list(
            fetch_all_features(
                "http://mock.wfs.server/wfs",
                typename="test:cities",
                page_size=1000,
                limit=2,
            )
        )

        # With limit=2, should return at most 2 features
        total_features = sum(len(p["features"]) for p in pages)
        assert total_features <= 2

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_empty_response(self, mock_request, mock_empty_response):
        """Test handling of empty feature collection response."""
        mock_request.return_value = json_dumps(mock_empty_response)
        from geoparquet_io.core.wfs import fetch_all_features

        pages = list(
            fetch_all_features(
                "http://mock.wfs.server/wfs",
                typename="test:empty",
                page_size=1000,
            )
        )

        # Should return one page with zero features
        assert len(pages) == 1
        assert pages[0]["numberReturned"] == 0
        assert len(pages[0]["features"]) == 0

    @patch("geoparquet_io.core.wfs._make_request")
    def test_parallel_fetching_maintains_order(
        self, mock_request, mock_geojson_page_1, mock_geojson_page_2, mock_geojson_page_3
    ):
        """Test that parallel fetching (max_workers > 1) maintains page order."""
        # Mock returns pages potentially out of order due to parallel execution
        mock_request.side_effect = [
            json_dumps(mock_geojson_page_1),
            json_dumps(mock_geojson_page_2),
            json_dumps(mock_geojson_page_3),
        ]
        from geoparquet_io.core.wfs import fetch_all_features

        pages = list(
            fetch_all_features(
                "http://mock.wfs.server/wfs",
                typename="test:cities",
                page_size=2,
                max_workers=3,  # Parallel fetching
            )
        )

        # Pages should be returned in order regardless of parallel execution
        assert len(pages) == 3

        # First page should have San Francisco (first city)
        first_city = pages[0]["features"][0]["properties"]["name"]
        assert first_city == "San Francisco"

        # Last page should have Paris (last city)
        last_city = pages[2]["features"][0]["properties"]["name"]
        assert last_city == "Paris"


class TestGeometryParsing:
    """Tests for GeoJSON/GML to Arrow table conversion."""

    def test_geojson_to_arrow_table(self, mock_geojson_response):
        """Test conversion of GeoJSON FeatureCollection to Arrow table."""
        from geoparquet_io.core.wfs import geojson_to_arrow_table

        table = geojson_to_arrow_table(mock_geojson_response)

        # Should have 3 rows (3 features in mock)
        assert table.num_rows == 3

        # Should have expected columns
        column_names = table.column_names
        assert "name" in column_names
        assert "population" in column_names
        assert "country" in column_names

        # Check data integrity
        names = table.column("name").to_pylist()
        assert "San Francisco" in names
        assert "New York" in names
        assert "London" in names

        populations = table.column("population").to_pylist()
        assert 884363 in populations  # San Francisco
        assert 8336817 in populations  # New York

    def test_gml_to_arrow_table(self, mock_gml_response):
        """Test conversion of GML3 response to Arrow table."""
        from geoparquet_io.core.wfs import gml_to_arrow_table

        table = gml_to_arrow_table(mock_gml_response, typename="test:cities")

        # Should have 1 row (1 feature in mock GML)
        assert table.num_rows == 1

        # Check data
        names = table.column("name").to_pylist()
        assert names[0] == "San Francisco"

        populations = table.column("population").to_pylist()
        assert populations[0] == 884363

    def test_extracts_geometry_column(self, mock_geojson_response):
        """Test that geometry column is properly extracted."""
        from geoparquet_io.core.wfs import geojson_to_arrow_table

        table = geojson_to_arrow_table(mock_geojson_response)

        # Should have a geometry column
        assert "geometry" in table.column_names

        # Geometry should be binary (WKB)
        geom_column = table.column("geometry")
        assert geom_column.type == pa.binary() or geom_column.type == pa.large_binary()

    def test_handles_empty_features(self, mock_empty_response):
        """Test conversion of empty FeatureCollection to Arrow table."""
        from geoparquet_io.core.wfs import geojson_to_arrow_table

        table = geojson_to_arrow_table(mock_empty_response)

        # Should create table with 0 rows but valid schema
        assert table.num_rows == 0


class TestErrorHandling:
    """Tests for HTTP error scenarios."""

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_http_404(self, mock_request):
        """Test handling of HTTP 404 (Not Found) error."""
        import click
        import httpx

        mock_request.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=MagicMock(status_code=404),
        )
        from geoparquet_io.core.wfs import get_wfs_capabilities

        with pytest.raises(click.ClickException, match="404|not found|Not Found"):
            get_wfs_capabilities("http://nonexistent.wfs.server/wfs")

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_http_500_with_retry(self, mock_request, mock_capabilities_xml):
        """Test that HTTP 500 errors trigger retry logic and eventually succeed."""
        import httpx

        # First two calls fail with 500, third succeeds
        mock_request.side_effect = [
            httpx.HTTPStatusError(
                "Server Error",
                request=MagicMock(),
                response=MagicMock(status_code=500),
            ),
            httpx.HTTPStatusError(
                "Server Error",
                request=MagicMock(),
                response=MagicMock(status_code=500),
            ),
            mock_capabilities_xml,  # Third call succeeds
        ]
        from geoparquet_io.core.wfs import get_wfs_capabilities

        # Should succeed after retries
        caps = get_wfs_capabilities("http://mock.wfs.server/wfs")
        assert len(caps.layers) == 2
        assert mock_request.call_count == 3

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_invalid_typename(self, mock_request, mock_capabilities_xml):
        """Test handling of request for non-existent typename."""
        import click

        mock_request.return_value = mock_capabilities_xml
        from geoparquet_io.core.wfs import get_layer_info

        with pytest.raises(click.ClickException, match="not found|invalid|does not exist"):
            get_layer_info("http://mock.wfs.server/wfs", "nonexistent:layer")

    @patch("geoparquet_io.core.wfs._make_request")
    def test_handles_network_timeout(self, mock_request):
        """Test handling of network timeout errors."""
        import click
        import httpx

        mock_request.side_effect = httpx.TimeoutException("Connection timed out")
        from geoparquet_io.core.wfs import get_wfs_capabilities

        with pytest.raises((click.ClickException, httpx.TimeoutException)):
            get_wfs_capabilities("http://slow.wfs.server/wfs")
