package mil.nga.giat.data.elasticsearch;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.format.DateTimeFormatter;

public class ElasticCompat5 implements ElasticCompat {

    @Override
    public FilterToElastic newFilterToElastic() {
        return new FilterToElastic5();
    }

    @Override
    public Settings createSettings(Object... params) {
        return Settings.builder().put(params).build();
    }

    @Override
    public String encodeGeohash(double lon, double lat, int level) {
        return GeoHashUtils.stringEncode(lon, lat, level);
    }

    @Override
    public GeoPoint decodeGeohash(String geohash) {
        return GeoPoint.fromGeohash(geohash);
    }

    @Override
    public Client createClient(Settings settings, TransportAddress... addresses) {
        TransportClient tc = new PreBuiltTransportClient(settings);
        return tc.addTransportAddresses(addresses);
    }

    @Override
    public Date parseDateTime(String datestring, String format) {
        final DateTimeFormatter dateFormatter = Joda.forPattern(format).parser();
        return dateFormatter.parseDateTime((String) datestring).toDate();
    }

    @Override
    public boolean isAnalyzed(Map<String, Object> map) {
        final String index = (String) map.get("index");
        return index != null && index.equals("analyzed");
    }

    @Override
    public void addField(SearchRequestBuilder builder, String name) {
        builder.storedFields(name);
    }

}
