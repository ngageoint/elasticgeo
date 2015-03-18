/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import static org.junit.Assert.*;
import mil.nga.giat.data.elasticsearch.ElasticGeometryUtil;

import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class ElasticGeometryUtilTest {

    private ElasticGeometryUtil geometryUtil;
    
    private GeometryFactory geometryFactory;
    
    @Before
    public void setUp() {
        geometryUtil = new ElasticGeometryUtil();
        geometryFactory = new GeometryFactory();
    }
    
    @Test
    public void testParseGeoPointNegative() {
        final String value = "-1.2,-3.4";
        final Point point = geometryUtil.parseGeoPoint(value);
        assertTrue(point.equals(geometryFactory.createPoint(new Coordinate(-3.4, -1.2))));
    }
    
    @Test
    public void testGeoPointFraction() {
        final String value = ".01,.23";
        final Point point = geometryUtil.parseGeoPoint(value);
        assertTrue(point.equals(geometryFactory.createPoint(new Coordinate(0.23, 0.01))));
    }
}
