/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;

import static mil.nga.giat.data.elasticsearch.ElasticLayerConfiguration.DATE_FORMAT;
import static mil.nga.giat.data.elasticsearch.ElasticLayerConfiguration.FULL_NAME;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * FeatureReader access to the Elasticsearch index.
 *
 */
public class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final ContentState state;

    private final SimpleFeatureType featureType;

    private SimpleFeatureBuilder builder;

    private Iterator<SearchHit> searchHitIterator;

    private ElasticGeometryUtil geometryUtil;

    public ElasticFeatureReader(ContentState contentState, SearchResponse response) {
        state = contentState;
        featureType = state.getFeatureType();
        searchHitIterator = response.getHits().iterator();
        builder = new SimpleFeatureBuilder(featureType);
        geometryUtil = new ElasticGeometryUtil();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }

    @Override
    public SimpleFeature next() {
        return readFeature();
    }

    private SimpleFeature readFeature() {
        if (!searchHitIterator.hasNext()) {
            return null;
        }
        final SearchHit hit = searchHitIterator.next();
        final SimpleFeatureType type = getFeatureType();
        final Map<String, Object> source = hit.getSource();

        final GeometryDescriptor geometryDescriptor;
        geometryDescriptor = featureType.getGeometryDescriptor();
        final String geometryName;
        if (geometryDescriptor != null) {
            geometryName = geometryDescriptor.getName().getLocalPart();
        } else {
            geometryName = null;
        }

        for (final AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
            final String name = descriptor.getType().getName().getLocalPart();
            final String sourceName = (String) descriptor.getUserData().get(FULL_NAME);
            final boolean isGeometry = name.equals(geometryName);
            Object value = readProperty(source, sourceName, isGeometry);
            if (value == null && source.containsKey("properties")) {
                // added to support ogr2ogr output
                value = readProperty(source, "properties." + name, isGeometry);
            }
            if (value != null && Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
                final Geometry geometry = geometryUtil.createGeometry(value);
                builder.set(name, geometry);
            } else if (value != null && Date.class.isAssignableFrom(descriptor.getType().getBinding())) {
                DateTimeFormatter dateFormatter = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();
                final String format = (String) descriptor.getUserData().get(DATE_FORMAT);
                if (format != null) {
                    dateFormatter = DateTimeFormat.forPattern(format).withZoneUTC();
                }
                Date date = dateFormatter.parseDateTime((String) value).toDate();
                builder.set(name, date);
            } else {
                builder.set(name, value);
            }
        }

        final String typeName = state.getEntry().getTypeName();
        final SimpleFeature feature;
        feature = builder.buildFeature(typeName + "." + hit.getId());
        return feature;
    }

    @Override
    public boolean hasNext() {
        return searchHitIterator.hasNext();
    }

    @Override
    public void close() {
        builder = null;
        searchHitIterator = null;
    }

    private Object readProperty(Map<String, Object> source, String propertyName, boolean isGeometry) {
        Object value = null;
        final String[] keys = propertyName.split("\\.");
        for (int i=0; i<keys.length; i++) {
            Object entry = source.get(keys[i]);
            if (!isGeometry && entry instanceof List && !((List<?>) entry).isEmpty()) {
                final List<?> list = (List<?>) entry;
                if (Map.class.isAssignableFrom(list.get(0).getClass())) {
                    // TODO: Add support for nested object arrays
                    entry = list.get(0);
                } else {
                    entry = Joiner.on(';').join((List<?>) entry);
                }
            }
            if (i<keys.length-1 && entry != null && Map.class.isAssignableFrom(entry.getClass())) {
                source = (Map) entry;
            } else {
                value = entry;
            }
        }
        return value;
    }

}
