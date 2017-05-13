/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.geotools.util.logging.Logging;
import org.joda.time.format.DateTimeFormatter;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.Coordinate;

public class ElasticCompat5 implements ElasticCompat {

    private final static Logger LOGGER = Logging.getLogger(ElasticCompat5.class);

    @Override
    public FilterToElastic newFilterToElastic() {
        return new FilterToElastic5();
    }

    @Override
    public String encodeGeohash(double lon, double lat, int level) {
        return GeoHash.encodeHash(lat, lon, level);
    }

    @Override
    public Coordinate decodeGeohash(String geohash) {
        final LatLong latLon = GeoHash.decodeHash(geohash);
        return new Coordinate(latLon.getLon(), latLon.getLat());
    }

    @Override
    public ElasticClient createClient(String host, int port) throws IOException {
        ElasticClient elasticClient = null;
        try {
            final RestClient client = RestClient.builder(new HttpHost(host, port, "http")).build();
            final Response response = client.performRequest("GET", "/", Collections.<String, String>emptyMap());
            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new IOException();
            }
            elasticClient = new RestElasticClient(client);
            LOGGER.fine("Created REST client: " + client);
        } catch (Exception e) {
            throw new IOException("Unable to create REST client", e);
        }
        return elasticClient;
    }

    @Override
    public Date parseDateTime(String datestring, String format) {
        final DateTimeFormatter dateFormatter = Joda.forPattern(format).parser();
        return dateFormatter.parseDateTime((String) datestring).toDate();
    }

    @Override
    public boolean isAnalyzed(Map<String, Object> map) {
        boolean analyzed = false;
        Object value = map.get("type");
        if (value != null && value instanceof String && ((String) value).equals("text")) {
            analyzed = true;
        }
        return analyzed;
    }

}
