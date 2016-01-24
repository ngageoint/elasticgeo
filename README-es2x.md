# ElasticGeo - Elasticsearch 2.x

This is the Elasticsearch 2.x version of the ElasticGeo plugin for GeoServer. 

Changes to support Elasticsearch 2.x include:
1. Support non-backward compatible changes to Elasticsearch API, such as removal of FilterBuilder and ImmutableSettings
2. Change from using Postfilter style queries to FilteredQuery style queries.  This will enable Elasticsearch to take advantage of Filter cache.
3. Minor changes to ensure tests use elasticsearch.properties in all tests, so as to use test cluster_name
4. Removal of guava classes from gs-elasticsearch jar

This plugin version was developed to work with geoserver-2.7.3.

# Installation
Installation is mostly the same as the ElasticGeo plugin, https://github.com/ngageoint/elasticgeo/blob/master/gs-web-elasticsearch/doc/index.rst

For geoserver-2.7.3, you will need to replace guava-17.jar with guava-18.jar in the geoserver WEB-INF/lib.  

# License
This software inherits all licenses and notices of the parent ElasticGeo project.  With the addition that this software distributed under these Licenses is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

See https://github.com/ngageoint/elasticgeo for more information.

