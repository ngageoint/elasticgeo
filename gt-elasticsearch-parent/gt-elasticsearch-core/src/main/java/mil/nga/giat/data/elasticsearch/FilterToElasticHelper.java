/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2009, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.GEOMETRY_TYPE;
import mil.nga.giat.data.elasticsearch.ElasticAttribute.ElasticGeometryType;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.DistanceBufferOperator;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Within;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;

class FilterToElasticHelper {

    protected double lat, lon, distance;

    protected String key;

    protected Literal geometry;

    protected ShapeRelation shapeRelation;

    protected ShapeBuilder shapeBuilder;

    /**
     * Conversion factor from common units to meter
     */
    protected static final Map<String, Double> UNITS_MAP = new HashMap<String, Double>() {
        {
            put("kilometers", 1000.0);
            put("kilometer", 1000.0);
            put("mm", 0.001);
            put("millimeter", 0.001);
            put("mi", 1609.344);
            put("miles", 1609.344);
            put("NM", 1852d);
            put("feet", 0.3048);
            put("ft", 0.3048);
            put("in", 0.0254);
        }
    };

    protected static final Envelope WORLD = new Envelope(-180, 180, -90, 90);

    FilterToElastic delegate;

    public FilterToElasticHelper(FilterToElastic delegate) {
        this.delegate = delegate;
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) {

        if (filter instanceof DistanceBufferOperator) {
            visitDistanceSpatialOperator((DistanceBufferOperator) filter,
                    property, geometry, swapped, extraData);
        } else {
            visitComparisonSpatialOperator(filter, property, geometry,
                    swapped, extraData);
        }
        return extraData;
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1,
            Expression e2, Object extraData) {

        visitBinarySpatialOperator(filter, e1, e2, false, extraData);
        return extraData;
    }

    protected void visitDistanceSpatialOperator(DistanceBufferOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) {

        property.accept(delegate, extraData);
        key = (String) delegate.field;
        geometry.accept(delegate, extraData);
        final Geometry geo = delegate.currentGeometry;
        lat = geo.getCentroid().getY();
        lon = geo.getCentroid().getX();
        final double inputDistance = filter.getDistance();
        final String inputUnits = filter.getDistanceUnits();
        distance = Double.valueOf(toMeters(inputDistance, inputUnits));
    }

    private String toMeters(double distance, String unit) {
        // only geography uses metric units
        if(isCurrentGeography()) {
            Double conversion = UNITS_MAP.get(unit);
            if(conversion != null) {
                return String.valueOf(distance * conversion);
            }
        }

        // in case unknown unit or not geography, use as-is
        return String.valueOf(distance);
    }

    void visitComparisonSpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped, Object extraData) {

        // if geography case, sanitize geometry first
        this.geometry = geometry;
        if(isCurrentGeography()) {
            this.geometry = clipToWorld(geometry);
        }

        visitBinarySpatialOperator(filter, (Expression)property, (Expression)this.geometry, swapped, extraData);
    }

    void visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
            boolean swapped, Object extraData) {

        AttributeDescriptor attType;
        attType = (AttributeDescriptor)e1.evaluate(delegate.featureType);

        ElasticGeometryType geometryType;
        geometryType = (ElasticGeometryType) attType.getUserData().get(GEOMETRY_TYPE);
        if (geometryType == ElasticGeometryType.GEO_POINT) {
            visitGeoPointBinarySpatialOperator(filter, e1, e2, swapped, extraData);                        
        } else {
            visitGeoShapeBinarySpatialOperator(filter, e1, e2, swapped, extraData);            
        }
    }

    void visitGeoShapeBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
            boolean swapped, Object extraData) {

        if (filter instanceof Disjoint) {
            shapeRelation = ShapeRelation.DISJOINT;
        } else if ((!swapped && filter instanceof Within) || (swapped && filter instanceof Contains)) {
            shapeRelation = ShapeRelation.WITHIN;
        } else if (filter instanceof Intersects || filter instanceof BBOX) {
            shapeRelation = ShapeRelation.INTERSECTS;
        } else {
            FilterToElastic.LOGGER.fine(filter.getClass().getSimpleName() 
                    + " is unsupported for geo_shape types");
            shapeRelation = null;
            delegate.fullySupported = false;
        }

        if (shapeRelation != null) {
            e1.accept(delegate, extraData);
            key = (String) delegate.field;
            e2.accept(delegate, extraData);
            shapeBuilder = delegate.currentShapeBuilder;
        }
    }

    void visitGeoPointBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
            boolean swapped, Object extraData) {

        e1.accept(delegate, extraData);
        key = (String) delegate.field;
        e2.accept(delegate, extraData);
    }


    boolean isCurrentGeography() {
        return true;
    }

    protected Literal clipToWorld(Literal geometry) {
        if(geometry != null) {
            Geometry g = geometry.evaluate(null, Geometry.class);
            if(g != null) {
                g.apply(new GeometryComponentFilter() {
                    @Override
                    public void filter(Geometry geom) {
                        geom.apply(new CoordinateFilter() {
                            @Override
                            public void filter(Coordinate coord) {
                                coord.setCoordinate(new Coordinate(clipLon(coord.x),clipLat(coord.y)));
                            }
                        });
                    }
                });
                geometry = CommonFactoryFinder.getFilterFactory(null).literal(g);

            }
        }

        return geometry;
    }

    protected double clipLon(double lon) {
        double x = Math.signum(lon)*(Math.abs(lon)%360);
        return x = x>180 ? x-360 : (x<-180 ? x+360 : x);
    }

    protected double clipLat(double lat) {
        return Math.min(90, Math.max(-90, lat));
    }

    /**
     * Returns true if the geometry covers the entire world
     * @param geometry
     * @return
     */
    protected boolean isWorld(Literal geometry) {
        boolean result = false;
        if(geometry != null) {
            Geometry g = geometry.evaluate(null, Geometry.class);
            if(g != null) {
                result = JTS.toGeometry(WORLD).equalsTopo(g.union());
            }
        }
        return result;
    }

    /**
     * Returns true if the geometry is fully empty
     * @param geometry
     * @return
     */
    protected boolean isEmpty(Literal geometry) {
        boolean result = false;
        if(geometry != null) {
            Geometry g = geometry.evaluate(null, Geometry.class);
            result = g == null || g.isEmpty();
        }
        return result;
    }

}
