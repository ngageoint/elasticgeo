/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

public class TransportElasticResponse extends ElasticResponse {

    private SearchResponse response;

    public TransportElasticResponse(SearchResponse response) {
        this.response = response;
    }

    @Override
    public int getNumHits() {
        return response.getHits().getHits().length;
    }

    @Override
    public long getTotalNumHits() {
        return response.getHits().getTotalHits();
    }

    @Override
    public Float getMaxScore() {
        return response.getHits().getMaxScore();
    }

    @Override
    public ElasticResults getResults() {
        // TODO Might prefer a wrapper iterator here but scroll reader uses sublist to enforce pagination
        final ElasticResults elasticHits = new ElasticResults();
        final List<ElasticHit> hits = new ArrayList<>();
        for (final SearchHit hit : response.getHits().getHits()) {
            hits.add(new TransportElasticHit(hit));
        }
        elasticHits.setHits(hits);
        return elasticHits;
    }

    @Override
    public String getScrollId() {
        return response.getScrollId();
    }

}
