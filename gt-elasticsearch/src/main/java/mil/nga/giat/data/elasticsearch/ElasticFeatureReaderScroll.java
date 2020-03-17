/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

class ElasticFeatureReaderScroll implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReaderScroll.class);

    private final ContentState contentState;

    /** The size of scrolling in this reader. Can not be changed. */
    private final int scrollSize;
    
    /** The total number of hits this reader can return if read to the end. */
    private final int totalHits;
    
    /** Is this reader paging. */
    private final boolean paging;

    /** The number of hits available in the current scroll. */
    private int hits;
    
    /** The total number of hits that have been read so far. */
    private int totalRead;

    /** The ID of the next scroll. */
    private String nextScrollId;
    
    private Set<String> scrollIds = new HashSet<>();

    /** Our delegate ElasticFeatureReader for each set of scrolled hits. */
    private ElasticFeatureReader delegate;

    /** Is this the final scroll to reach the totalHits. */
    private boolean finalScroll;
    
    public ElasticFeatureReaderScroll(ContentState contentState, ElasticResponse searchResponse, int size, int limit, boolean paging) {
        this.contentState = contentState;
        this.scrollSize = size;
        this.totalHits = limit;
        this.paging = paging;
        processResponse(searchResponse);
    }

    private void advanceScroll() throws IOException {
        final ElasticDataStore dataStore;
        dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
        processResponse(dataStore.getClient().scroll(nextScrollId, dataStore.getScrollTime()));
    }

    private void processResponse(ElasticResponse searchResponse) {
    	this.hits = searchResponse.getNumHits();
    	if(this.totalRead + this.hits >= this.totalHits) {
    		this.hits = this.totalHits - this.totalRead;
    		this.finalScroll = true;
    	}
        LOGGER.fine(String.format("Scroll numHits=%d total=%d", this.hits, this.totalRead + this.hits));
        
        final List<ElasticHit> hits = searchResponse.getResults().getHits().subList(0, this.hits);
        final Map<String, ElasticAggregation> aggs = searchResponse.getAggregations();
        
        delegate = new ElasticFeatureReader(contentState, hits, aggs, 0);
        nextScrollId = searchResponse.getScrollId();
        scrollIds.add(this.nextScrollId);
    }

    /**
     * Get the scroll size of this reader.
     * 
     * @return int
     */
    public int getScrollSize() {
    	return this.scrollSize;
    }
    
    /**
     * Get the size of the hits available from the current scroll.
     * 
     * @return int
     */
    public int getHitsSize() {
    	return this.hits;
    }
    
    @Override
    public SimpleFeatureType getFeatureType() {
        return this.contentState.getFeatureType();
    }

    @Override
    public SimpleFeature next() throws IOException {
        if (hasNext()) {
            totalRead++;
            return delegate.next();
        }

        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() throws IOException {
    	
		/*
		 * when getting more than scrollSize hits we can exhaust the delegate and
		 * still have more hits to get
		 */
        if (this.delegate != null && this.delegate.hasNext())
        	return true;
        
        /* in that case ... advance scroll and try for more */
        if(!this.paging && !this.finalScroll) {
        	advanceScroll();
        	return hasNext();
        }
        
        return false;
    }

	/**
	 * GeoServer will close this reader after each client request. But when paging
	 * it will be cached in the users HTTP session.
	 */
    @Override
    public void close() throws IOException {
    	this.delegate.close();
    	this.delegate = null;

		if (this.paging) {
			if (!this.finalScroll)
				advanceScroll();
			else
				ElasticFeatureSource.clearSession();
		}
    }

	/**
	 * Shutdown this reader by closing our delegate {@link ElasticFeatureReader} and
	 * clearing all scrolls.
	 */
    public void shutdown() {
    	if(this.delegate != null)
    		this.delegate.close();
    	
    	if(!this.scrollIds.isEmpty()) {
    		try {
        		ElasticDataStore eds = (ElasticDataStore)this.contentState.getEntry().getDataStore();
				eds.getClient().clearScroll(this.scrollIds);
			} catch (IOException e) {
				LOGGER.info(String.format("Could not close a scroll due to %s: %s", e.getClass().getName(),
						e.getMessage()));
			}
    	}
    	
    	this.finalScroll = true;
    }
    
}
