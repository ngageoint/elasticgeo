package mil.nga.giat.data.elasticsearch;

import java.util.Date;
import java.util.Map;

import com.spatial4j.core.io.GeohashUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.joda.time.format.DateTimeFormatter;

public class ElasticCompat2 implements ElasticCompat {

    @Override
    public FilterToElastic newFilterToElastic() {
        return new FilterToElastic2();
    }

    @Override
    public Settings createSettings(Object... params) {
        return Settings.builder().put(params).build();
    }

    @Override
    public String encodeGeohash(double lon, double lat, int level) {
        return GeohashUtils.encodeLatLon(lat, lon, level);
    }

    @Override
    public GeoPoint decodeGeohash(String geohash) {
        return GeoPoint.fromGeohash(geohash);
    }

    @Override
    public Client createClient(Settings settings, TransportAddress... addresses) {
        return TransportClient.builder().settings(settings).build().addTransportAddresses(addresses);
    }

    @Override
    public Date parseDateTime(String datestring, String format) {
        final DateTimeFormatter dateFormatter = Joda.forPattern(format).parser();
        return dateFormatter.parseDateTime((String) datestring).toDate();
    }

    @Override
    public boolean isAnalyzed(Map<String, Object> map) {
        final String index = (String) map.get("index");
        return index == null || index.equals("analyzed");
    }

    @Override
    public void addField(SearchRequestBuilder builder, String name) {
        builder.addField(name);
    }

}
