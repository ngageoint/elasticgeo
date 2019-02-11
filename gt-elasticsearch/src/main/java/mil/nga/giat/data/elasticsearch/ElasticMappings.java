/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@SuppressWarnings("unused")
class ElasticMappings {

    private Map<String,Mapping> mappings;

    public Map<String, Mapping> getMappings() {
        return mappings;
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class Mapping {

        private Map<String,Object> properties;

        public Map<String, Object> getProperties() {
            return properties;
        }
    }

}
