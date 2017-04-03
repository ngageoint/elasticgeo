/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.process.elasticsearch;

import java.util.Map;

public class MetricGeoHashGrid extends GeoHashGrid {

    private final static String METRIC_KEY = "metric";
    private final static String VALUE_KEY = "value";

    @Override
    public Number computeCellValue(Map<String,Object> bucket) {
        return super.pluckMetricValue(bucket, METRIC_KEY, VALUE_KEY);
    }

}
