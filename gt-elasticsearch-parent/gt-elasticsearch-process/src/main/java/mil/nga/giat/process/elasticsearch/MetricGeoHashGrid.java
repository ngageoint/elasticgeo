/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.process.elasticsearch;

import java.util.logging.Logger;
import java.util.Map;

import org.geotools.util.logging.Logging;

public class MetricGeoHashGrid extends GeoHashGrid {

    private final static Logger LOGGER = Logging.getLogger(MetricGeoHashGrid.class);

    private final static String METRIC_KEY = "metric";
    private final static String VALUE_KEY = "value";

    @Override
    public Number computeCellValue(Map<String,Object> bucket) {
        if (!bucket.containsKey(METRIC_KEY)) {
          LOGGER.warning("Unable to pull raster value from cell, geogrid bucket does not contain required key:" + METRIC_KEY);
          throw new IllegalArgumentException();
        }
        Map<String,Object> metric = (Map<String,Object>) bucket.get(METRIC_KEY);
        if (!metric.containsKey(VALUE_KEY)) {
          LOGGER.warning("Unable to pull raster value from cell, metric does not contain required key:" + VALUE_KEY);
          throw new IllegalArgumentException();
        }
        return (Number) metric.get(VALUE_KEY);
    }

}
