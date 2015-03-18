/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Utilities for creating geometries from coordinates in Elasticsearch
 * geo_point and geo_shape mapping types.
 *
 */
public class ElasticGeometryUtil {

    private final GeometryFactory geometryFactory;

    public ElasticGeometryUtil() {
        this.geometryFactory = new GeometryFactory();
    }

    public Geometry createGeometry(Object value) {
        final Geometry geometry;
        if (Map.class.isAssignableFrom(value.getClass())) {
            geometry = createGeometry((Map<String,Object>) value);
        } else {
            geometry = parseGeoPoint(value);
        }
        return geometry;
    }

    public Geometry createGeometry(final Map<String, Object> properties) {
        final Geometry geometry;
        switch (String.valueOf(properties.get("type")).toUpperCase()) {
        case "POINT": {
            final List<Number> posList;
            posList = (List) properties.get("coordinates");
            final Coordinate coordinate = createCoordinate(posList);
            geometry = geometryFactory.createPoint(coordinate);
            break;
        } case "LINESTRING": {
            final List<List<Number>> posList;
            posList = (List) properties.get("coordinates");
            final Coordinate[] coordinates = createCoordinates(posList);
            geometry = geometryFactory.createLineString(coordinates);
            break;
        } case "POLYGON": {
            final List<List<List<Number>>> posList;
            posList = (List) properties.get("coordinates");
            geometry = createPolygon(posList);
            break;
        } case "MULTIPOINT": {
            final List<List<Number>> posList;
            posList = (List) properties.get("coordinates");
            final Coordinate[] coordinates = createCoordinates(posList);
            geometry = geometryFactory.createMultiPoint(coordinates);
            break;
        } case "MULTILINESTRING": {
            final List<List<List<Number>>> posList;
            posList = (List) properties.get("coordinates");
            final LineString[] lineStrings = new LineString[posList.size()];
            for (int i=0; i<posList.size(); i++) {
                final Coordinate[] coordinates = createCoordinates(posList.get(0));
                lineStrings[i] = geometryFactory.createLineString(coordinates);
            }
            geometry = geometryFactory.createMultiLineString(lineStrings);
            break;
        } case "MULTIPOLYGON": {
            final List<List<List<List<Number>>>> posList;
            posList = (List) properties.get("coordinates");
            final Polygon[] polygons = new Polygon[posList.size()];
            for (int i=0; i<posList.size(); i++) {
                polygons[i] = createPolygon(posList.get(0));
            }
            geometry = geometryFactory.createMultiPolygon(polygons);
            break;
        } case "ENVELOPE": {
            final List<List<Number>> posList;
            posList = (List) properties.get("coordinates");
            final Coordinate[] envelope = createCoordinates(posList);
            final Coordinate[] coordinates = {
                    new Coordinate(envelope[0].x, envelope[0].y),
                    new Coordinate(envelope[0].x, envelope[1].y),
                    new Coordinate(envelope[1].x, envelope[1].y),
                    new Coordinate(envelope[1].x, envelope[0].y),
                    new Coordinate(envelope[0].x, envelope[0].y)};
            final LinearRing shell = geometryFactory.createLinearRing(coordinates);
            final LinearRing[] holes = new LinearRing[0];
            geometry = geometryFactory.createPolygon(shell, holes);
            break;
        } default:
            geometry = null;
            break;
        }
        return geometry;
    }

    public Polygon createPolygon(final List<List<List<Number>>> posList) {
        final Coordinate[] shellCoordinates = createCoordinates(posList.get(0));
        final LinearRing shell = geometryFactory.createLinearRing(shellCoordinates);
        final LinearRing[] holes = new LinearRing[posList.size()-1];
        for (int i=1; i<posList.size(); i++) {
            final Coordinate[] coordinates = createCoordinates(posList.get(i));
            holes[i-1] = geometryFactory.createLinearRing(coordinates);
        }
        return geometryFactory.createPolygon(shell, holes);
    }
    public Coordinate[] createCoordinates(final List<List<Number>> posList) {
        final Coordinate[] coordinates = new Coordinate[posList.size()];
        for (int i=0; i<posList.size(); i++) {
            coordinates[i] = createCoordinate(posList.get(i));
        }
        return coordinates;
    }

    public Coordinate createCoordinate(final List<Number> posList) {
        final double x = posList.get(0).doubleValue();
        final double y = posList.get(1).doubleValue();
        return new Coordinate(x,y);
    }

    public Point parseGeoPoint(Object obj) {
        final Point point;
        if (obj instanceof String) {
            final Pattern geoPointPattern = Pattern.compile("(-*\\d*\\.*\\d*?),(-*\\d*\\.*\\d*?)");
            final Matcher m = geoPointPattern.matcher((String) obj);
            if (m.matches()) {
                final double y = Double.valueOf(m.group(1));
                final double x = Double.valueOf(m.group(2));
                point = geometryFactory.createPoint(new Coordinate(x,y));
            } else {
                point = null;
            }
        } else if (obj instanceof List && ((List) obj).size()==2) {
            final List values = (List) obj;
            if (Number.class.isAssignableFrom(values.get(0).getClass())) {
                final double x = ((Number) values.get(0)).doubleValue();
                final double y = ((Number) values.get(1)).doubleValue();
                point = geometryFactory.createPoint(new Coordinate(x,y));
            } else if (values.get(0) instanceof String) {
                final double x = Double.valueOf((String) values.get(0));
                final double y = Double.valueOf((String) values.get(1));
                point = geometryFactory.createPoint(new Coordinate(x,y));
            } else {
                point = null;
            }
        } else {
            point = null;
        }
        return point;
    }

}
