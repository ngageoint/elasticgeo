/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2014, Open Source Geospatial Foundation (OSGeo)
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
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.temporal.object.DefaultInstant;
import org.geotools.temporal.object.DefaultPeriod;
import org.geotools.temporal.object.DefaultPosition;
import org.junit.After;
import org.junit.Before;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.temporal.Instant;
import org.opengis.temporal.Period;

public class ElasticTestSupport {

    protected static final Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(ElasticTestSupport.class);

    private static final String LINE_SEPARATOR = "line.separator";

    private static final String PROPERTIES_FILE = "elasticsearch.properties";

    private static final String TEST_FILE = "wifiAccessPoint.json";

    private static final String ACTIVE_MAPPINGS_FILE = "active_mappings.json";

    private static final String INACTIVE_MAPPINGS_FILE = "inactive_mappings.json";

    private static final Pattern STATUS_PATTERN = Pattern.compile(".*\"status_s\"\\s*:\\s*\"(.*?)\".*");

    private static final Pattern ID_PATTERN = Pattern.compile(".*\"id\"\\s*:\\s*\"(\\d+)\".*");

    protected static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss");

    protected static final int numShards = 1;

    protected static final int numReplicas = 0;

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected static final ObjectReader mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);

    protected String layerName = "active";

    protected int SOURCE_SRID = 4326;

    protected String indexName;

    protected String clusterName;

    protected int port;

    protected boolean scrollEnabled;

    protected long scrollSize;

    protected Path path;

    protected int activeNumShards;

    protected ElasticFeatureSource featureSource;

    protected static ElasticDataStore dataStore;

    protected ElasticLayerConfiguration config;

    protected List<ElasticAttribute> attributes;

    protected RestElasticClient client;

    @Before
    public void beforeTest() throws Exception {
        Properties properties = new Properties();
        InputStream inputStream = ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE);
        properties.load(inputStream);
        indexName = properties.getProperty("index_name");
        clusterName = properties.getProperty("cluster_name");
        scrollEnabled = Boolean.valueOf(properties.getProperty("scroll_enabled"));
        scrollSize = Long.valueOf(properties.getProperty("scroll_size"));

        port = 9200;
        client = new RestElasticClient(RestClient.builder(new HttpHost("localhost", port, "http")).build());

        try {
            client.performRequest("DELETE", "/" + indexName, null);
            client.performRequest("POST", "/_refresh", null);
        } catch (IOException e) {
            LOGGER.fine("Unable to clear test index (may not exist");
        }
        Map<String,Serializable> params = createConnectionParams();
        ElasticDataStoreFactory factory = new ElasticDataStoreFactory();
        dataStore = (ElasticDataStore) factory.createDataStore(params);
        createIndices();
    }

    @After
    public void afterTest() throws Exception {
        dataStore.dispose();
        client.close();
    }

    protected void createIndices() throws IOException {
        // create index and add mappings
        Map<String,Object> settings = new HashMap<>();
        settings.put("settings", ImmutableMap.of("number_of_shards", numShards, "number_of_replicas", numReplicas));
        Map<String,Object> mappings = new HashMap<>();
        settings.put("mappings", mappings);
        try (Scanner s = new Scanner(ClassLoader.getSystemResourceAsStream(ACTIVE_MAPPINGS_FILE))) {
            s.useDelimiter("\\A");
            Map<String,Object> source = mapReader.readValue(s.next());
            mappings.put("active", source);
        }
        try (Scanner s = new Scanner(ClassLoader.getSystemResourceAsStream(INACTIVE_MAPPINGS_FILE))) {
            s.useDelimiter("\\A");
            Map<String,Object> source = mapReader.readValue(s.next());
            mappings.put("not-active", source);
        }
        client.performRequest("PUT", "/" + indexName, settings);

        // index documents
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(TEST_FILE); Scanner scanner = new Scanner(inputStream)) {
            String eol = System.getProperty(LINE_SEPARATOR);
            scanner.useDelimiter(eol);
            while (scanner.hasNext()) {
                final String line = scanner.next();
                if (!line.startsWith("#")) {
                    final Matcher idMatcher = ID_PATTERN.matcher(line);
                    final String id;
                    if (idMatcher.matches()) {
                        id = idMatcher.group(1);
                    } else {
                        id = null;
                    }
                    final String layerName;
                    final Matcher statusMatcher = STATUS_PATTERN.matcher(line);
                    if (statusMatcher.matches()) {
                        layerName = statusMatcher.group(1);
                    } else {
                        layerName = null;
                    }
                    
                    Map<String,Object> source = mapReader.readValue(line);
                    client.performRequest("POST", "/" + indexName + "/" + layerName + "/" + id, source);
                }
            }

            client.performRequest("POST", "/" + indexName + "/_refresh", null);
        }
    }

    protected Map<String,Serializable> createConnectionParams() {
        Map<String,Serializable> params = new HashMap<>();
        params.put(ElasticDataStoreFactory.HOSTPORT.key, port);
        params.put(ElasticDataStoreFactory.INDEX_NAME.key, indexName);
        params.put(ElasticDataStoreFactory.SCROLL_ENABLED.key, scrollEnabled);
        params.put(ElasticDataStoreFactory.SCROLL_SIZE.key, scrollSize);
        return params;
    }

    protected void init() throws Exception {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        init(this.layerName);
    }

    protected void init(String layerName) throws Exception {
        init(layerName, "geo");
    }

    protected void init(String layerName, String geometryField) throws Exception {
        this.layerName = layerName;
        attributes = dataStore.getElasticAttributes(new NameImpl(this.layerName));
        config = new ElasticLayerConfiguration(layerName);
        List<ElasticAttribute> layerAttributes = new ArrayList<>();
        for (ElasticAttribute attribute : attributes) {
            attribute.setUse(true);
            if (geometryField.equals(attribute.getName())) {
                ElasticAttribute copy = new ElasticAttribute(attribute);
                copy.setDefaultGeometry(true);
                layerAttributes.add(copy);
            } else {
                layerAttributes.add(attribute);
            }
        }
        config.getAttributes().clear();
        config.getAttributes().addAll(layerAttributes);
        dataStore.setLayerConfiguration(config);
        featureSource = (ElasticFeatureSource) dataStore.getFeatureSource(this.layerName);
    }

    protected Date date(String date) throws ParseException {
        return DATE_FORMAT.parse(date);
    }

    protected Instant instant(String d) throws ParseException {
        return new DefaultInstant(new DefaultPosition(date(d)));
    }

    protected Period period(String d1, String d2) throws ParseException {
        return new DefaultPeriod(instant(d1), instant(d2));
    }

    protected List<SimpleFeature> readFeatures(SimpleFeatureIterator iterator) {
        final List<SimpleFeature> features = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                features.add(iterator.next());
            }
        } finally {
            iterator.close();
        }
        return features;
    }

}
