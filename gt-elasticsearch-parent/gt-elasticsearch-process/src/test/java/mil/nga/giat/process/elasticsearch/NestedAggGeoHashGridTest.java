/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.process.elasticsearch;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class NestedAggGeoHashGridTest {

    private NestedAggGeoHashGrid geohashGrid;

    @Before
    public void setup() {
        this.geohashGrid = new NestedAggGeoHashGrid();
    }

    @Test
    public void testSetParams() {
         geohashGrid.setParams(null);
         assertEquals(NestedAggGeoHashGrid.DEFAULT_AGG_KEY, geohashGrid.getNestedAggKey());
         assertNull(geohashGrid.getMetricKey());
         assertNull(geohashGrid.getValueKey());
         assertEquals(NestedAggGeoHashGrid.SELECT_LARGEST, geohashGrid.getSelectionStrategy());
         assertEquals(NestedAggGeoHashGrid.RASTER_FROM_VALUE, geohashGrid.getRasterStrategy());
         assertNull(geohashGrid.getTermsMap());
    }
    
   @Test
    public void testSetParams_withParams() {
             String aggKey = "myagg";
             String metricKey = "mymetric";
             String valueKey = "myvalue";
         List<String> params = new ArrayList<String>();
         params.add(aggKey);
         params.add(metricKey);
         params.add(valueKey);
         params.add(NestedAggGeoHashGrid.SELECT_SMALLEST);
         params.add(NestedAggGeoHashGrid.RASTER_FROM_KEY);
         params.add("key1:1;key2:2");
         geohashGrid.setParams(params);
         assertEquals(aggKey, geohashGrid.getNestedAggKey());
         assertEquals(metricKey, geohashGrid.getMetricKey());
         assertEquals(valueKey, geohashGrid.getValueKey());
         assertEquals(NestedAggGeoHashGrid.SELECT_SMALLEST, geohashGrid.getSelectionStrategy());
         assertEquals(NestedAggGeoHashGrid.RASTER_FROM_KEY, geohashGrid.getRasterStrategy());
         Map<String, Integer> termsMap = geohashGrid.getTermsMap();
         assertEquals(2, termsMap.size());
         assertEquals(new Integer(1), (Integer) termsMap.get("key1"));
         assertEquals(new Integer(2), (Integer) termsMap.get("key2"));
    }
   
   @Test
   public void testSetParams_ignoreInvalidParams() {
             String aggKey = "myagg";
             String metricKey = "mymetric";
             String valueKey = "myvalue";
             List<String> params = new ArrayList<String>();
             params.add(aggKey);
             params.add(metricKey);
             params.add(valueKey);
             params.add("invalid token");
             params.add("invalid token");
             geohashGrid.setParams(params);
             assertEquals(aggKey, geohashGrid.getNestedAggKey());
             assertEquals(metricKey, geohashGrid.getMetricKey());
             assertEquals(valueKey, geohashGrid.getValueKey());
             assertEquals(NestedAggGeoHashGrid.SELECT_LARGEST, geohashGrid.getSelectionStrategy());
         assertEquals(NestedAggGeoHashGrid.RASTER_FROM_VALUE, geohashGrid.getRasterStrategy());
         assertNull(geohashGrid.getTermsMap());
   }
    
    @Test(expected=IllegalArgumentException.class)
    public void testSetParams_notEnoughParameters() {
         geohashGrid.setParams(new ArrayList<String>());
    }
}
