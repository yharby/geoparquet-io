"""
Tests for WFS (Web Feature Service) extraction.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

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
