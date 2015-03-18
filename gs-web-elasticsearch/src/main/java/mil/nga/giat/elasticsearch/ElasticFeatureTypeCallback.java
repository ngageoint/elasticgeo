/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package mil.nga.giat.elasticsearch;

import java.io.IOException;
import javax.security.auth.callback.Callback;
import mil.nga.giat.data.elasticsearch.ElasticDataStore;
import mil.nga.giat.data.elasticsearch.ElasticLayerConfiguration;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.FeatureTypeCallback;
import org.geotools.data.DataAccess;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;

/**
 * 
 * Implementation of FeatureTypeInitializer extension point to initialize 
 * Elasticsearch datastore.
 * 
 * @see {@link FeatureTypeCallback}
 * 
 */
public class ElasticFeatureTypeCallback implements FeatureTypeCallback {

    @Override
    public boolean canHandle(FeatureTypeInfo info,
            DataAccess<? extends FeatureType, ? extends Feature> dataAccess) {
        if (dataAccess instanceof ElasticDataStore) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean initialize(FeatureTypeInfo info,
            DataAccess<? extends FeatureType, ? extends Feature> dataAccess, Name temporaryName)
                    throws IOException {
        ElasticLayerConfiguration configuration = (ElasticLayerConfiguration) info.getMetadata().get(
                ElasticLayerConfiguration.KEY);
        if (configuration != null) {
            ElasticDataStore dataStore = (ElasticDataStore) dataAccess;
            dataStore.setElasticConfigurations(configuration);
        }
        // we never use the temp name
        return false;
    }

    @Override
    public void dispose(FeatureTypeInfo info,
            DataAccess<? extends FeatureType, ? extends Feature> dataAccess, Name temporaryName)
                    throws IOException {
        ElasticLayerConfiguration configuration = (ElasticLayerConfiguration) info.getMetadata().get(
                ElasticLayerConfiguration.KEY);
        ElasticDataStore dataStore = (ElasticDataStore) dataAccess;
        dataStore.getElasticConfigurations().remove(configuration.getLayerName());
    }

    @Override
    public void flush(FeatureTypeInfo info,
            DataAccess<? extends FeatureType, ? extends Feature> dataAccess) throws IOException {
        // nothing to do
    }

}
