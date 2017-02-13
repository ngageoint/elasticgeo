package mil.nga.giat.data.elasticsearch;

import org.geotools.geometry.jts.ReferencedEnvelope;
import static org.geotools.geometry.jts.ReferencedEnvelope.EVERYTHING;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

public class GeohashUtilTest {

    @Test
    public void testComputePrecision() {
        assertEquals(1, GeohashUtil.computePrecision(new ReferencedEnvelope(-180,180,-90,90,null), 32));
        assertEquals(2, GeohashUtil.computePrecision(new ReferencedEnvelope(-180,180,-90,90,null), 1024));
        assertEquals(3, GeohashUtil.computePrecision(new ReferencedEnvelope(-180,180,-90,90,null), 32768));
        
        assertEquals(2, GeohashUtil.computePrecision(new ReferencedEnvelope(-180,180,-90,90,null), 1000));
        assertEquals(2, GeohashUtil.computePrecision(new ReferencedEnvelope(-180,180,-90,90,null), 1500));
        
        assertEquals(1, GeohashUtil.computePrecision(new ReferencedEnvelope(EVERYTHING.getMinX(),EVERYTHING.getMaxX(),EVERYTHING.getMinY(),EVERYTHING.getMaxY(),null), 32));
    }

    @Test
    public void updatePrecision() {
        final Map<String,Object> geohashGridAgg = new HashMap<>();
        geohashGridAgg.putAll(ImmutableMap.of("field","name","precision",0));
        final Map<String,Map<String,Map<String,Object>>> aggregations = ImmutableMap.of("first",ImmutableMap.of("geohash_grid",geohashGridAgg));
        final Map<String,Object> expected = ImmutableMap.of("first",ImmutableMap.of("geohash_grid",ImmutableMap.of("field","name","precision",2)));
        GeohashUtil.updateGridAggregationPrecision(aggregations, 2);
        assertEquals(expected, aggregations);
    }

}
