/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.Map;

import org.geotools.geometry.jts.ReferencedEnvelope;

import com.github.davidmoten.geo.GeoHash;

public class GeohashUtil {

    public static int computePrecision(ReferencedEnvelope envelope, long maxBins) {
        return computePrecision(envelope, maxBins, 1);
    }

    private static int computePrecision(ReferencedEnvelope envelope, long maxBins, int n) {
        final double bins = computeSize(envelope, n);
        final double previousBins = computeSize(envelope, n-1);
        if (bins > maxBins && bins-maxBins > maxBins-previousBins) {
            return n-1;
        } else if (bins > maxBins) {
            return n;
        } else {
            return computePrecision(envelope, maxBins, n+1);
        }
    }

    private static double computeSize(ReferencedEnvelope envelope, int n) {
        final double minx = Math.max(-180, envelope.getMinX());
        final double maxx = Math.min(180, envelope.getMaxX());
        final double miny = Math.max(-90, envelope.getMinY());
        final double maxy = Math.min(90, envelope.getMaxY());
        return (maxx-minx)/GeoHash.widthDegrees(n)*(maxy-miny)/GeoHash.heightDegrees(n);
    }

    public static void updateGridAggregationPrecision(Map<String,Map<String,Map<String,Object>>> aggregations, int precision) {
        aggregations.values().stream().filter(a->a.containsKey("geohash_grid")).forEach(a -> {
            a.get("geohash_grid").put("precision", precision);
        });
    }

}
