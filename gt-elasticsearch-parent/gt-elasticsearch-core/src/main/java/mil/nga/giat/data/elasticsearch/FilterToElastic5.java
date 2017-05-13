/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.DATE_FORMAT;
import static mil.nga.giat.data.elasticsearch.ElasticConstants.MATCH_ALL;

import org.geotools.data.Query;
import org.geotools.geojson.geom.GeometryJSON;
import org.joda.time.format.DateTimeFormatter;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.BinaryLogicOperator;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.BinaryTemporalOperator;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class FilterToElastic5 extends FilterToElastic {

    public static DateTimeFormatter DEFAULT_DATE_FORMATTER = Joda.forPattern("date_optional_time").printer();

    protected Map<String,Object> filterBuilder;

    private DateTimeFormatter dateFormatter;

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final ObjectReader mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);

    public FilterToElastic5() {
        filterBuilder = MATCH_ALL;
        nativeQueryBuilder = ImmutableMap.of("match_all", Collections.EMPTY_MAP);
        helper = new FilterToElasticHelper5(this);
    }


    // BEGIN IMPLEMENTING org.opengis.filter.FilterVisitor METHODS

    /**
     * Writes the FilterBuilder for the ExcludeFilter.
     * 
     * @param filter the filter to be visited
     */
    public Object visit(ExcludeFilter filter, Object extraData) {
        filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", MATCH_ALL));
        return extraData;
    }

    /**
     * Writes the FilterBuilder for the IncludeFilter.
     * 
     * @param filter the filter to be visited
     *  
     */
    public Object visit(IncludeFilter filter, Object extraData) {
        filterBuilder = MATCH_ALL;
        return extraData;
    }

    public Object visit(PropertyIsBetween filter, Object extraData) {
        super.visit(filter, extraData);

        filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gte", lower, "lte", upper)));
        if(nested) {
            filterBuilder = ImmutableMap.of("nested", ImmutableMap.of("path", path, "query", filterBuilder));
        }

        return extraData;
    }

    public Object visit(PropertyIsLike filter, Object extraData) {
        super.visit(filter, extraData);

        if (analyzed) {
            // use query string query for analyzed fields
            filterBuilder = ImmutableMap.of("query_string", ImmutableMap.of("query", pattern, "default_field", key));
        } else {
            // default to regexp query
            filterBuilder = ImmutableMap.of("regexp", ImmutableMap.of(key, pattern));
        }
        if (nested) {
            filterBuilder = ImmutableMap.of("nested", ImmutableMap.of("path", path, "query", filterBuilder));
        }

        return extraData;
    }

    public Object visit(Not filter, Object extraData) {
        super.visit(filter, extraData);

        if(filter.getFilter() instanceof PropertyIsNull) {
            filterBuilder = ImmutableMap.of("exists", ImmutableMap.of("field", field));
        } else {
            filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", filterBuilder));
        }
        return extraData;
    }

    @Override
    protected Object visit(BinaryLogicOperator filter, Object extraData) {
        LOGGER.finest("exporting LogicFilter");

        final List<Map<String,Object>> filters = new ArrayList<>();
        for (final Filter child : filter.getChildren()) {
            child.accept(this, extraData);
            filters.add(filterBuilder);
        }
        if (extraData.equals("AND")) {
            filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", filters));
        } else if (extraData.equals("OR")) {
            filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("should", filters));
        }
        return extraData;
    }

    protected void visitBinaryComparisonOperator(BinaryComparisonOperator filter, Object extraData) {
        super.visitBinaryComparisonOperator(filter, extraData);

        if (type.equals("=")) {
            filterBuilder = ImmutableMap.of("term", ImmutableMap.of(key, field));
        } else if (type.equals("!=")) {
            filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", ImmutableMap.of("term", ImmutableMap.of(key, field))));
        } else if (type.equals(">")) {
            filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gt", field)));
        } else if (type.equals(">=")) {
            filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gte", field)));
        } else if (type.equals("<")) {
            filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lt", field)));
        } else if (type.equals("<=")) {
            filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lte", field)));
        }

        if (nested) {
            filterBuilder = ImmutableMap.of("nested", ImmutableMap.of("path", path, "query", filterBuilder));
        }
    }

    public Object visit(PropertyIsNull filter, Object extraData) {
        super.visit(filter, extraData);

        filterBuilder = ImmutableMap.of("bool", ImmutableMap.of("must_not", ImmutableMap.of("exists", ImmutableMap.of("field", field))));
        return extraData;
    }

    public Object visit(Id filter, Object extraData) {
        super.visit(filter, extraData);

        filterBuilder = ImmutableMap.of("ids", ImmutableMap.of("values", ids));
        return extraData;
    }

    protected Object visitBinaryTemporalOperator(BinaryTemporalOperator filter, 
            PropertyName property, Literal temporal, boolean swapped, Object extraData) { 

        super.visitBinaryTemporalOperator(filter, property, temporal, swapped, extraData);

        if (filter instanceof After || filter instanceof Before) {
            if (period != null) {
                if ((op.equals(" > ") && !swapped) || (op.equals(" < ") && swapped)) {
                    filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gt", end)));
                } else {
                    filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lt", begin)));
                }
            }
            else {
                if (op.equals(" < ") || swapped) {
                    filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lt", field)));
                } else {
                    filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gt", field)));
                }
            }
        }
        else if (filter instanceof Begins || filter instanceof Ends || 
                filter instanceof BegunBy || filter instanceof EndedBy ) {

            filterBuilder = ImmutableMap.of("term", ImmutableMap.of(key, field));
        }
        else if (filter instanceof During || filter instanceof TContains){
            filterBuilder = ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gt", lower, "lt", field)));
        }
        else if (filter instanceof TEquals) {
            filterBuilder = ImmutableMap.of("term", ImmutableMap.of(key, field));
        }

        if (nested) {
            filterBuilder = ImmutableMap.of("nested", ImmutableMap.of("path", path, "query", filterBuilder));
        }

        return extraData;
    }

    // END IMPLEMENTING org.opengis.filter.FilterVisitor METHODS


    // START IMPLEMENTING org.opengis.filter.ExpressionVisitor METHODS

    protected void writeLiteral(Object literal) {
        super.writeLiteral(literal);

        if (Date.class.isAssignableFrom(literal.getClass())) {
            field = dateFormatter.print(((Date) literal).getTime());
        }
    }

    protected void visitLiteralGeometry(Literal expression) throws IOException {
        super.visitLiteralGeometry(expression);

        final String geoJson = new GeometryJSON().toString(currentGeometry);
        currentShapeBuilder = mapReader.readValue(geoJson);
    }

    // END IMPLEMENTING org.opengis.filter.ExpressionVisitor METHODS

    @Override
    protected void updateDateFormatter(AttributeDescriptor attType) {
        dateFormatter = DEFAULT_DATE_FORMATTER;
        if (attType != null) {
            final String format = (String) attType.getUserData().get(DATE_FORMAT);
            if (format != null) {
                dateFormatter = Joda.forPattern(format).printer();
            }
        }
    }

    protected void addViewParams(Query query) {
        super.addViewParams(query);

        if (parameters != null) {
            if (nativeOnly) {
                LOGGER.fine("Ignoring GeoServer filter (Elasticsearch native query/post filter only)");
                filterBuilder = MATCH_ALL;
            }
            for (final Map.Entry<String, String> entry : parameters.entrySet()) {
                if (entry.getKey().equalsIgnoreCase("q")) {
                    final String value = entry.getValue();
                    try {
                        nativeQueryBuilder = mapReader.readValue(value);
                    } catch (IOException e) {
                        throw new FilterToElasticException("Unable to read query view parameter",e);
                    }
                }
                if (entry.getKey().equalsIgnoreCase("a")) {
                    final ObjectMapper mapper = new ObjectMapper();
                    try {
                        final TypeReference<Map<String, Map<String,Map<String,Object>>>> type;
                        type = new TypeReference<Map<String, Map<String,Map<String,Object>>>>() {};
                        this.aggregations = mapper.readValue(entry.getValue(), type);
                    } catch (IOException e) {
                        throw new FilterToElasticException("Unable to parse aggregation",e);
                    }
                }
            }
        }
    }

    @Override
    public Map<String,Object> getQueryBuilder() {
        final Map<String,Object> queryBuilder;
        if (nativeQueryBuilder.equals(MATCH_ALL)) {
            queryBuilder = filterBuilder;
        } else if (filterBuilder.equals(MATCH_ALL)) {
            queryBuilder = nativeQueryBuilder;
        } else {
            queryBuilder = ImmutableMap.of("bool", ImmutableMap.of("must", ImmutableList.of(nativeQueryBuilder, filterBuilder)));
        }
        return queryBuilder;
    }

}
