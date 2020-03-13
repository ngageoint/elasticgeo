/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.MDC;
import org.geotools.data.FeatureReader;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Provides access to a specific type within the Elasticsearch index described
 * by the associated data store.
 * 
 * For large feature sets, this source can either scroll or page the results.
 * Although the {@link ElasticFeatureReaderScroll} uses an ES scroll either way,
 * scrolling and paging are very different operations.
 * 
 * Scrolled results are automatically returned to a single client request when
 * the Query selects more results than the
 * {@link ElasticDataStore#getScrollSize()}. This allows getting more records
 * from ES than the default 10k limit but is still capped by the layer
 * per-request max feature limit.
 * 
 * Paged results are returned to a sequence of client requests, activated by
 * using the WFS 2.0 STARTINDEX and COUNT parameters. This allows getting more
 * results from a layer than the per-request max features limit (in COUNT size
 * chunks). The COUNT provided by the client overrides the scroll size, so it is
 * the client's responsibility to use a size below 10k (or the ES limit). Also,
 * paged results must begin with STARTINDEX=0 and must advance through
 * STARTINDEXs in constant COUNT intervals. That is, the pages can not be
 * reversed, skipped or randomly accessed, and page size can not be changed.
 * 
 * NOTE: To support paged results, the {@link ElasticFeatureReaderScroll} is
 * cached in the users {@link HttpSession}. A {@link CompletableFuture} is also
 * created and asynchronously run to close that scroll if the client does not
 * read subsequent pages. This means the client must support session tracking 
 * (i.e. a session cookie).
 * 
 * NOTE: This gives the impression that we could work in a multi-GeoServer
 * environment, but almost certainly the scroll and the timeout would not
 * serialize and migrate with the session.
 *
 */
class ElasticFeatureSource extends ContentFeatureSource {

	private final static Logger LOGGER = Logging.getLogger(ElasticFeatureSource.class);

	private Boolean filterFullySupported;

	/** HTTP session key for storing the {@link ElasticFeatureReaderScroll} */
	private final static String SESSION_KEY_READER = ElasticFeatureSource.class.getName() + ".reader";

	/** HTTP session key for storing the {@link SessionClearer} */
	private final static String SESSION_KEY_TIMEOUT = ElasticFeatureSource.class.getName() + ".timeout";

	/** The total number of hits available from the {@link Query} */
	private Integer totalHits;

	public ElasticFeatureSource(ContentEntry entry, Query query) throws IOException {
		super(entry, query);

		final ElasticDataStore dataStore = getDataStore();
		if (dataStore.getLayerConfigurations().get(entry.getName().getLocalPart()) == null) {
			final List<ElasticAttribute> attributes = dataStore.getElasticAttributes(entry.getName());
			final ElasticLayerConfiguration config = new ElasticLayerConfiguration(entry.getName().getLocalPart());
			config.getAttributes().addAll(attributes);
			dataStore.setLayerConfiguration(config);
		}
	}

	/**
	 * Access parent datastore
	 */
	public ElasticDataStore getDataStore() {
		return (ElasticDataStore) super.getDataStore();
	}

	/**
	 * Implementation that generates the total bounds
	 */
	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		LOGGER.fine("getBoundsInternal");
		final CoordinateReferenceSystem crs = getSchema().getCoordinateReferenceSystem();
		final ReferencedEnvelope bounds = new ReferencedEnvelope(crs);

		try (FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = getReaderInternal(query)) {
			while (featureReader.hasNext()) {
				final SimpleFeature feature = featureReader.next();
				bounds.include(feature.getBounds());
			}
		}
		return bounds;
	}

	@Override
	protected int getCountInternal(Query query) throws IOException {
		LOGGER.fine("getCountInternal");
		return getHitCount(query);
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
		LOGGER.fine("getReaderInternal");
		FeatureReader<SimpleFeatureType, SimpleFeature> reader;
		try {
			final ElasticDataStore dataStore = getDataStore();
			final String docType = dataStore.getDocType(entry.getName());
			final boolean scroll = isScroll(query, dataStore) || isPaging(query);
			final ElasticRequest searchRequest = prepareSearchRequest(query, scroll);
			if (scroll)
				reader = getScroll(query, dataStore, docType, searchRequest);
			else {
				ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType, searchRequest);
				reader = new ElasticFeatureReader(getState(), sr);
				if (!filterFullySupported)
					reader = new FilteringFeatureReader<>(reader, query.getFilter());
			}

			int hits = getHitCount(query);
			if (isPaging(query))
				LOGGER.info(String.format("Read from %s/%s: %d@%d/%d", dataStore.getIndexName(), docType, hits,
						query.getStartIndex(), this.totalHits));
			else
				LOGGER.info(String.format("Read from %s/%s: %d", dataStore.getIndexName(), docType, hits));

		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			throw new IOException("Error executing query search", e);
		}
		return reader;
	}

	/**
	 * Get the number of hits available from the {@link Query} at this time.
	 * 
	 * @param query Query
	 * @return int
	 * @throws IOException
	 */
	protected int getHitCount(Query query) throws IOException {
		
		if (this.totalHits == null) {
			final ElasticRequest searchRequest = prepareSearchRequest(query, false);
			try {
				if (!filterFullySupported) {
					try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query)) {
						this.totalHits = 0;
						while (reader.hasNext()) {
							reader.next();
							this.totalHits++;
						}
					}
				} else {
					searchRequest.setSize(0);
					final ElasticDataStore dataStore = getDataStore();
					final String docType = dataStore.getDocType(entry.getName());
					final ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType,
							searchRequest);
					this.totalHits = (int) sr.getTotalNumHits();
				}
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
				throw new IOException("Error executing count search", e);
			}
		}

		/* when max is unlimited ... return all we have */
		if (query.isMaxFeaturesUnlimited())
			return this.totalHits;

		/* when paging ... return the current page size */
		if (isPaging(query) && !isPageOne(query))
			return unwrapPager(getPager(query.getStartIndex(), query.getMaxFeatures())).getHitsSize();

		/* otherwise ... return smaller of total or max */
		return Math.min(this.totalHits, getMaxSize(query));
	}

	private ElasticRequest prepareSearchRequest(Query query, boolean scroll) throws IOException {
		String naturalSortOrder = SortOrder.ASCENDING.toSQL().toLowerCase();
		final ElasticRequest searchRequest = new ElasticRequest();
		final ElasticDataStore dataStore = getDataStore();
		final String docType = dataStore.getDocType(entry.getName());

		LOGGER.fine("Preparing " + docType + " (" + entry.getName() + ") query");
		if (scroll)
			searchRequest.setScroll(dataStore.getScrollTime());
		else {
			if (query.getSortBy() != null) {
				for (final SortBy sort : query.getSortBy()) {
					final String sortOrder = sort.getSortOrder().toSQL().toLowerCase();
					if (sort.getPropertyName() != null) {
						final String name = sort.getPropertyName().getPropertyName();
						searchRequest.addSort(name, sortOrder);
					} else {
						naturalSortOrder = sortOrder;
					}
				}
			}
		}

		searchRequest.setSize(getScrollSize(query, dataStore));

		if (dataStore.isSourceFilteringEnabled()) {
			if (query.getProperties() != Query.ALL_PROPERTIES) {
				for (String property : query.getPropertyNames()) {
					searchRequest.addSourceInclude(property);
				}
			} else {
				// add source includes
				setSourceIncludes(searchRequest);
			}
		}

		// add query and post filter
		final FilterToElastic filterToElastic = new FilterToElastic();
		filterToElastic.setFeatureType(buildFeatureType());
		filterToElastic.encode(query);
		filterFullySupported = filterToElastic.getFullySupported();
		if (!filterFullySupported) {
			LOGGER.fine("Filter is not fully supported by native Elasticsearch."
					+ " Additional post-query filtering will be performed.");
		}
		final Map<String, Object> queryBuilder = filterToElastic.getQueryBuilder();

		final Map<String, Object> nativeQueryBuilder = filterToElastic.getNativeQueryBuilder();

		searchRequest.setQuery(queryBuilder);

		if (isSort(query) && nativeQueryBuilder.equals(ElasticConstants.MATCH_ALL)) {
			final String sortKey = dataStore.getClient().getVersion() < 7 ? "_uid" : "_id";
			searchRequest.addSort(sortKey, naturalSortOrder);
		}

		if (filterToElastic.getAggregations() != null) {
			final Map<String, Map<String, Map<String, Object>>> aggregations = filterToElastic.getAggregations();
			final Envelope envelope = (Envelope) query.getFilter().accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR,
					null);
			final long gridSize;
			if (dataStore.getGridSize() != null) {
				gridSize = dataStore.getGridSize();
			} else {
				gridSize = (Long) ElasticDataStoreFactory.GRID_SIZE.getDefaultValue();
			}
			final double gridThreshold;
			if (dataStore.getGridThreshold() != null) {
				gridThreshold = dataStore.getGridThreshold();
			} else {
				gridThreshold = (Double) ElasticDataStoreFactory.GRID_THRESHOLD.getDefaultValue();
			}
			final int precision = GeohashUtil.computePrecision(envelope, gridSize, gridThreshold);
			LOGGER.fine("Updating GeoHash grid aggregation precision to " + precision);
			GeohashUtil.updateGridAggregationPrecision(aggregations, precision);
			searchRequest.setAggregations(aggregations);
			searchRequest.setSize(0);
		}

		return searchRequest;
	}

	private void setSourceIncludes(final ElasticRequest searchRequest) throws IOException {
		final ElasticDataStore dataStore = getDataStore();
		final List<ElasticAttribute> attributes = dataStore.getElasticAttributes(entry.getName());
		for (final ElasticAttribute attribute : attributes) {
			if (attribute.isUse() && attribute.isStored()) {
				searchRequest.addField(attribute.getName());
			} else if (attribute.isUse()) {
				searchRequest.addSourceInclude(attribute.getName());
			}
		}
	}

	/**
	 * Is this Query sorted.
	 * 
	 * @param query Query
	 * @return boolean
	 */
	private static boolean isSort(Query query) {
		return query.getSortBy() != null && query.getSortBy().length > 0;
	}

	/**
	 * Is this Query paging.
	 * 
	 * @param query Query
	 * @return boolean
	 */
	private static boolean isPaging(Query query) {
		return query.getStartIndex() != null;
	}

	/**
	 * Is this the first page of a paging Query
	 * 
	 * @param query Query
	 * @return boolean
	 */
	private static boolean isPageOne(Query query) {
		return isPaging(query) && query.getStartIndex() == 0;
	}

	/**
	 * Does this query need to be scrolled.
	 * 
	 * @param query Query
	 * @param eds   ElasticDataStore
	 * @return boolean
	 */
	private boolean isScroll(Query query, ElasticDataStore eds) throws IOException {
		return getHitCount(query) > eds.getScrollSize() && !isPaging(query);
	}

	/**
	 * Get the maximum size of data the query is allowed to return.
	 * 
	 * @param query Query
	 * @return int
	 */
	private static int getMaxSize(Query query) {
		return query.getMaxFeatures() < 0 ? Integer.MAX_VALUE : query.getMaxFeatures();
	}

	/**
	 * Get the size for ES scroll. When paging, this is the COUNT provided to the {@link Query},
	 * otherwise it is the configured {@link ElasticDataStore#getScrollSize()}.
	 * 
	 * @param query Query
	 * @param eds   ElasticDataStore
	 * @return int
	 */
	private static int getScrollSize(Query query, ElasticDataStore eds) {
		return isPaging(query) ? getMaxSize(query) : eds.getScrollSize().intValue();
	}

	@Override
	protected SimpleFeatureType buildFeatureType() {
		final ElasticDataStore ds = getDataStore();
		final ElasticLayerConfiguration layerConfig;
		layerConfig = ds.getLayerConfigurations().get(entry.getTypeName());
		final List<ElasticAttribute> attributes;
		if (layerConfig != null) {
			attributes = layerConfig.getAttributes();
		} else {
			attributes = null;
		}

		final ElasticFeatureTypeBuilder typeBuilder;
		typeBuilder = new ElasticFeatureTypeBuilder(attributes, entry.getName());
		return typeBuilder.buildFeatureType();
	}

	@Override
	protected boolean canLimit() {
		return true;
	}

	@Override
	protected boolean canOffset() {
		return true;
	}

	@Override
	protected boolean canFilter() {
		return true;
	}

	@Override
	protected boolean canSort() {
		return true;
	}

	/**
	 * Get the scroll reader (which may be wrapped in a filter). This will create
	 * the reader or get the one cached in the users session as appropriate.
	 * 
	 * @param query         Query
	 * @param eds           ElasticDataStore
	 * @param docType       String
	 * @param searchRequest ElasticRequest
	 * @return
	 * @throws IOException
	 */
	private FeatureReader<SimpleFeatureType, SimpleFeature> getScroll(Query query, ElasticDataStore eds, String docType,
			ElasticRequest searchRequest) throws IOException {

		FeatureReader<SimpleFeatureType, SimpleFeature> reader = null;
		boolean paging = isPaging(query);
		if (paging && !isPageOne(query))
			reader = getPager(query.getStartIndex(), query.getMaxFeatures());
		else {
			int limit = paging ? this.totalHits : getHitCount(query);
			ElasticResponse sr = eds.getClient().search(eds.getIndexName(), docType, searchRequest);
			reader = new ElasticFeatureReaderScroll(getState(), sr, getScrollSize(query, eds), limit, paging);

			if (!this.filterFullySupported)
				reader = new FilteringFeatureReader<SimpleFeatureType, SimpleFeature>(reader, query.getFilter());
		}

		if (paging)
			cachePager(query, eds, reader);

		return reader;
	}

	/**
	 * Get the paging reader cached in the users session for the STARTINDEX and
	 * COUNT.
	 * 
	 * @param start Integer
	 * @param count Integer
	 * @return FeatureReader of SimpleFeatureType, SimpleFeature
	 * @throws IOException
	 */
	private static FeatureReader<SimpleFeatureType, SimpleFeature> getPager(int index, int count)
			throws IOException {
		Entry<Integer, FeatureReader<SimpleFeatureType, SimpleFeature>> e = getPagerEntry(getSession());
		
		int position = e.getKey();
		if (index != position)
			throw new IOException(String.format("Reader is at %d not %d!", position, index));

		FeatureReader<SimpleFeatureType, SimpleFeature> fr = e.getValue();

		int size = unwrapPager(fr).getScrollSize();
		if (size != count)
			throw new IOException(String.format("Page size is %d not %d!", size, count));

		return fr;
	}

	/**
	 * Get the Map Entry with a paging reader cached in the users session.
	 * 
	 * @param ses HttpSession
	 * @return Entry of Integer and FeatureReader of SimpleFeatureType,
	 *         SimpleFeature
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private static Entry<Integer, FeatureReader<SimpleFeatureType, SimpleFeature>> getPagerEntry(HttpSession ses)
			throws IOException {
		Object o = ses.getAttribute(SESSION_KEY_READER);
		if (o instanceof Map) {
			Map<?, ?> m = (Map<Integer, ?>) o;
			return (Entry<Integer, FeatureReader<SimpleFeatureType, SimpleFeature>>) m.entrySet().iterator().next();
		}

		throw new IOException("Cached scroll not found!");
	}

	/**
	 * Unwrap the scroll from the reader.
	 * 
	 * @param reader FeatureReader
	 * @return ElasticFeatureReaderScroll
	 * @throws IOException
	 */
	private static ElasticFeatureReaderScroll unwrapPager(FeatureReader<?, ?> reader) throws IOException {
		if (reader instanceof ElasticFeatureReaderScroll)
			return (ElasticFeatureReaderScroll) reader;

		if (reader instanceof FilteringFeatureReader) {
			Object o = ((FilteringFeatureReader<?, ?>) reader).getDelegate();
			if (o instanceof ElasticFeatureReaderScroll)
				return (ElasticFeatureReaderScroll) o;
		}

		throw new IOException("Could not unwrap scroll!");
	}

	/**
	 * Cache the paging reader in the users session at the next STARTINDEX. Ensure
	 * any other reader already cached is shutdown, and schedule a clear in the
	 * event the next page is not retrieved before scroll timeout.
	 * 
	 * @param query Query
	 * @param eds   ElasticDataStore
	 * @param fr    FeatureReader of SimpleFeatureType, SimpleFeature
	 * @throws IOException
	 */
	private static void cachePager(Query query, ElasticDataStore eds,
			FeatureReader<SimpleFeatureType, SimpleFeature> fr) throws IOException {

		/* do a partial clear of the session -- skipping this pager */
		HttpSession ses = getSession();
		clearSession(ses, fr);

		/* cache this pager at the next STARTINDEX */
		Integer nextIndex = query.getStartIndex() + unwrapPager(fr).getScrollSize();
		ses.setAttribute(SESSION_KEY_READER, Collections.singletonMap(nextIndex, fr));

		/* schedule a timeout on this pager */
		SessionClearer clearer = ReaderScrollTimeout.INSTANCE.scheduleClear(ses, eds.getScrollTime(), TimeUnit.SECONDS);
		ses.setAttribute(SESSION_KEY_TIMEOUT, clearer);
	}

	/**
	 * Paging can only be supported with an {@link HttpSession}. This will get the
	 * existing one, or create one if there is none by sending <code>true</code> to
	 * {@link HttpServletRequest#getSession()}.
	 * 
	 * @return HttpSession
	 */
	private static HttpSession getSession() {
		ServletRequestAttributes sra = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
		return sra.getRequest().getSession(true);
	}

	/**
	 * Clear our objects saved in the users {@link HttpSession}. This is called by
	 * the {@link ElasticFeatureReaderScroll#close()} but ONLY when it has read the final
	 * scroll (GeoServer closes it after every client call when paging).
	 */
	static void clearSession() {
		try {
			clearSession(getSession(), null);
		} catch (Throwable e) {
			if(LOGGER.isLoggable(Level.FINE))
				LOGGER.log(Level.FINE, "Could not clear HTTP session.", e);
		}
	}

	/**
	 * Clear our objects saved in the users {@link HttpSession} by canceling any
	 * {@link SessionClearer} and closing the {@link FeatureReader}s unless it is
	 * the one to skip (we'll be using the same one for all paging requests).
	 * 
	 * @param ses  HttpSession
	 * @param skip FeatureReader of SimpleFeatureType and SimpleFeature
	 */
	static void clearSession(HttpSession ses, FeatureReader<SimpleFeatureType, SimpleFeature> skip) {
		/* cancel any SessionClearer */
		Object o = ses.getAttribute(SESSION_KEY_TIMEOUT);
		if (o instanceof SessionClearer)
			((SessionClearer) o).canceled = true;
		ses.setAttribute(SESSION_KEY_TIMEOUT, null);

		/* shutdown any cached pager other than the skip one */
		try {
			Entry<Integer, FeatureReader<SimpleFeatureType, SimpleFeature>> e = getPagerEntry(ses);
			FeatureReader<SimpleFeatureType, SimpleFeature> fr = e.getValue();
			if (fr != skip)
				unwrapPager(fr).shutdown();
		} catch (IOException e) {
			if (ses.getAttribute(SESSION_KEY_READER) != null)
				LOGGER.log(Level.WARNING, "Failed to close a scroll reader!", e);
		}
		ses.setAttribute(SESSION_KEY_READER, null);
	}

	/**
	 * Use a {@link ScheduledExecutorService} to run {@link SessionClearer} after a
	 * timeout.
	 * 
	 * @author jsteen
	 *
	 */
	static class ReaderScrollTimeout implements Closeable {

		/** Our singleton instance */
		public static ReaderScrollTimeout INSTANCE = new ReaderScrollTimeout();

		/** Our Executor */
		private ScheduledExecutorService exc;

		/**
		 * Private constructor to enforce singleton nature
		 */
		private ReaderScrollTimeout() {
			this.exc = Executors.newScheduledThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable run) {
					Thread thread = new Thread(run);
					thread.setDaemon(true);
					return thread;
				}
			});
		}

		/**
		 * Schedule a SessionClearer to run at timeout.
		 * 
		 * @param ses     HttpSession
		 * @param timeout long
		 * @param unit    TimeUnit
		 * @return SessionClearer
		 */
		public SessionClearer scheduleClear(HttpSession ses, long timeout, TimeUnit unit) {
			SessionClearer sc = new SessionClearer(ses);
			this.exc.schedule(sc, timeout, unit);
			return sc;
		}

		@Override
		public void close() {
			this.exc.shutdownNow();
		}
	}

	/**
	 * This {@link Runnable} will call
	 * {@link ElasticFeatureSource#clearSession(HttpSession, FeatureReader)} unless
	 * it is canceled before it executes.
	 * 
	 * @author jsteen
	 *
	 */
	static class SessionClearer implements Runnable {

		/** Our Logger */
		static Logger LOGGER = Logging.getLogger(SessionClearer.class);

		/** Our canceled flag */
		boolean canceled;

		/** The session to clear */
		HttpSession session;

		/** The authentication when scheduled */
		Authentication auth;

		/**
		 * Constructor
		 * 
		 * @param ses
		 */
		public SessionClearer(HttpSession ses) {
			this.session = ses;
			this.auth = SecurityContextHolder.getContext().getAuthentication();
		}

		@Override
		public void run() {
			if (this.canceled)
				return;

			if (this.auth != null)
				MDC.put("user", this.auth.getName());

			ElasticFeatureSource.clearSession(this.session, null);
			LOGGER.warning("Shutdown scroll on timeout!");
		}
	}
}
