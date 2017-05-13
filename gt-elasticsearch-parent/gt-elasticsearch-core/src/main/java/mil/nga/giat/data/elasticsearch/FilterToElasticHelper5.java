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
 */package mil.nga.giat.data.elasticsearch;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.MATCH_ALL;

import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.DistanceBufferOperator;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Within;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

import javax.measure.unit.SI;

class FilterToElasticHelper5 extends FilterToElasticHelper {

    public FilterToElasticHelper5(FilterToElastic delegate) {
        super(delegate);
    }

    @Override
    protected void visitDistanceSpatialOperator(DistanceBufferOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) {

        super.visitDistanceSpatialOperator(filter, property, geometry, swapped, extraData);

        getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", MATCH_ALL,
                "filter", ImmutableMap.of("geo_distance", 
                        ImmutableMap.of("distance", distance+SI.METER.toString(), key, ImmutableList.of(lon,lat)))));

        if ((filter instanceof DWithin && swapped)
                || (filter instanceof Beyond && !swapped)) {
            getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", getDelegate().filterBuilder));
        }
    }

    @Override
    protected void visitComparisonSpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped, Object extraData) {

        super.visitComparisonSpatialOperator(filter, property, geometry, swapped, extraData);

        // if geography case, sanitize geometry first
        if(isCurrentGeography()) {
            if(isWorld(this.geometry)) {
                // nothing to filter in this case
                getDelegate().filterBuilder = MATCH_ALL;
                return;
            } else if(isEmpty(this.geometry)) {
                if(!(filter instanceof Disjoint)) {
                    getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", MATCH_ALL));
                } else {
                    getDelegate().filterBuilder = MATCH_ALL;
                }
                return;
            }
        }

        visitBinarySpatialOperator(filter, (Expression)property, (Expression)this.geometry, swapped, extraData);
    }

    @Override
    protected void visitGeoShapeBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
            boolean swapped, Object extraData) {

        super.visitGeoShapeBinarySpatialOperator(filter, e1, e2, swapped, extraData);

        if (shapeRelation != null && shapeBuilder != null) {
            getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", MATCH_ALL,
                    "filter", ImmutableMap.of("geo_shape", 
                            ImmutableMap.of(key, ImmutableMap.of("shape", shapeBuilder, "relation", shapeRelation)))));
        } else {
            getDelegate().filterBuilder = MATCH_ALL;
        }
    }

    @Override
    protected void visitGeoPointBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
            boolean swapped, Object extraData) {

        super.visitGeoPointBinarySpatialOperator(filter, e1, e2, swapped, extraData);

        final Geometry geometry = delegate.currentGeometry;

        if (geometry instanceof Polygon &&
                ((!swapped && filter instanceof Within) 
                        || (swapped && filter instanceof Contains)
                        || filter instanceof Intersects)) {
            final Polygon polygon = (Polygon) geometry;
            final List<List<Double>> points = new ArrayList<>();
            for (final Coordinate coordinate : polygon.getCoordinates()) {
                points.add(ImmutableList.of(coordinate.x, coordinate.y));
            }
            getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", MATCH_ALL,
                    "filter", ImmutableMap.of("geo_polygon", 
                            ImmutableMap.of(key, ImmutableMap.of("points", points)))));
        } else if (filter instanceof BBOX) {
            final Envelope envelope = geometry.getEnvelopeInternal();
            final double minY = clipLat(envelope.getMinY());
            final double maxY = clipLat(envelope.getMaxY());
            final double minX, maxX;
            if (envelope.getWidth() < 360) {
                minX = clipLon(envelope.getMinX());
                maxX = clipLon(envelope.getMaxX());
            } else {
                minX = -180;
                maxX = 180;
            }
            getDelegate().filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", MATCH_ALL,
                    "filter", ImmutableMap.of("geo_bounding_box", ImmutableMap.of(key, 
                            ImmutableMap.of("top_left", ImmutableList.of(minX, maxY), 
                                    "bottom_right", ImmutableList.of(maxX, minY))))));
        } else {
            FilterToElastic.LOGGER.fine(filter.getClass().getSimpleName() 
                    + " is unsupported for geo_point types");
            delegate.fullySupported = false;
            getDelegate().filterBuilder = MATCH_ALL;
        }
    }

    private FilterToElastic5 getDelegate() {
        return (FilterToElastic5) delegate;
    }

}
