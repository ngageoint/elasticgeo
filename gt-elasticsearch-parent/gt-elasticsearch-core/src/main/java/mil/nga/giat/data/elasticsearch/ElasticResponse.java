/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class ElasticResponse {

    @JsonProperty("hits")
    private ElasticResults results;

    private Map<String,ElasticAggregation> aggregations;

    @JsonProperty("_scroll_id")
    private String scrollId;

    public ElasticResults getResults() {
        return results;
    }

    public void setResults(ElasticResults results) {
        this.results = results;
    }

    public Map<String,ElasticAggregation> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String,ElasticAggregation> aggregations) {
        this.aggregations = aggregations;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public int getNumHits() {
        return results.getHits().size();
    }

    public long getTotalNumHits() {
        return results.getTotal();
    }

    public Float getMaxScore() {
        return results.getMaxScore();
    }

}
