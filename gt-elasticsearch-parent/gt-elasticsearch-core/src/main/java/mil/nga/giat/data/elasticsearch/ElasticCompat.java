/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;

public interface ElasticCompat {

    public FilterToElastic newFilterToElastic();

    public String encodeGeohash(double lon, double lat, int level);

    public Coordinate decodeGeohash(String geohash);

    public ElasticClient createClient(String host, int port) throws IOException;

    public Date parseDateTime(String datestring, String format);

    public boolean isAnalyzed(Map<String,Object> map);

}
