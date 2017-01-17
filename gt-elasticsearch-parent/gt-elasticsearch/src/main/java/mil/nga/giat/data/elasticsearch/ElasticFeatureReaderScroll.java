package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class ElasticFeatureReaderScroll implements FeatureReader<SimpleFeatureType, SimpleFeature> {
    
    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReaderScroll.class);
    
    private final ContentState contentState;
    
    private final int maxFeatures;
    
    private String nextScrollId;
    
    private ElasticFeatureReader delegate;
    
    private int numFeatures;
    
    private boolean lastScroll;

    private Set<String> scrollIds;
    
    public ElasticFeatureReaderScroll(ContentState contentState, SearchResponse searchResponse, int maxFeatures) {
        this.contentState = contentState;
        this.maxFeatures = maxFeatures;
        this.numFeatures = 0;
        this.scrollIds = new HashSet<>();
        processResponse(searchResponse);
    }
    
    private void advanceScroll() {
        final ElasticDataStore dataStore;
        dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
        final SearchScrollRequestBuilder scrollRequest = dataStore.getClient().prepareSearchScroll(nextScrollId);
        if (dataStore.getScrollTime() != null) {
            scrollRequest.setScroll(TimeValue.timeValueSeconds(dataStore.getScrollTime()));
        }
        processResponse(scrollRequest.execute().actionGet());
    }

    private void processResponse(SearchResponse searchResponse) {
        final int numHits = searchResponse.getHits().hits().length;
        final List<SearchHit> hits;
        if (numFeatures+numHits <= maxFeatures) {
            hits = Arrays.asList(searchResponse.getHits().hits());
        } else {
            final int n = maxFeatures-numFeatures;
            hits = Arrays.asList(searchResponse.getHits().hits()).subList(0,n);
        }
        delegate = new ElasticFeatureReader(contentState, hits.iterator());
        nextScrollId = searchResponse.getScrollId();
        lastScroll = numHits == 0 || numFeatures+hits.size()>=maxFeatures;
        LOGGER.fine("Scoll numHits=" + hits.size() + " (total=" + numFeatures+hits.size());
        scrollIds.add(nextScrollId);
    }
    
    @Override
    public SimpleFeatureType getFeatureType() {
        return delegate.getFeatureType();
    }

    @Override
    public SimpleFeature next() throws IOException {
        final SimpleFeature feature;
        if (hasNext()) {
            numFeatures++;
            feature = delegate.next();
        } else {
            throw new NoSuchElementException();
        }
        return feature;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (!delegate.hasNext() && !lastScroll) {
            advanceScroll();
        }
        return (delegate.hasNext() || !lastScroll) && numFeatures<maxFeatures;
    }

    @Override
    public void close() throws IOException {
        if (!scrollIds.isEmpty()) {
            final ElasticDataStore dataStore;
            dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
            final ClearScrollRequestBuilder clearScrollRequest = dataStore.getClient().prepareClearScroll();
            for (final String scrollId : scrollIds) {
                clearScrollRequest.addScrollId(scrollId);
            }
            clearScrollRequest.execute().actionGet();
        }
        delegate.close();
    }

}
