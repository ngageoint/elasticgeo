/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.process.elasticsearch;

public class BasicGeoHashGrid extends GeoHashGrid {

    @Override
    public void populate() {
        buckets.stream().forEach(bucket -> updateCell((String) bucket.get("key"), (Number) bucket.get("doc_count")));
    }

}
