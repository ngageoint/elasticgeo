/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */

package mil.nga.giat.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.data.elasticsearch.ElasticAttribute;

import org.geoserver.web.wicket.GeoServerDataProvider;

/**
 * 
 * Provide attributes from Elasticsearch fields.
 * 
 */
public class ElasticAttributeProvider extends GeoServerDataProvider<ElasticAttribute> {

    private static final long serialVersionUID = -1021780286733349153L;

    private List<ElasticAttribute> attributes = new ArrayList<ElasticAttribute>();
    
    private Boolean useShortName = false;

    /**
     * Name of field
     */
    protected static final Property<ElasticAttribute> NAME = new BeanProperty<ElasticAttribute>("name",
            "displayName");

    /**
     * Class type of field
     */
    protected static final Property<ElasticAttribute> TYPE = new AbstractProperty<ElasticAttribute>(
            "type") {

        private static final long serialVersionUID = 4454312983828267130L;

        @Override
        public Object getPropertyValue(ElasticAttribute item) {
            if (item.getType() != null) {
                return item.getType().getSimpleName();
            }
            return null;
        }

    };

    /**
     * Mark as the default geometry
     */
    protected static final Property<ElasticAttribute> DEFAULT_GEOMETRY = new BeanProperty<ElasticAttribute>(
            "defaultGeometry", "defaultGeometry");

    /**
     * SRID of geometric field
     */
    protected static final Property<ElasticAttribute> SRID = new BeanProperty<ElasticAttribute>("srid",
            "srid");

    /**
     * Store if the field is in use in datastore
     */
    protected static final Property<ElasticAttribute> USE = new BeanProperty<ElasticAttribute>("use",
            "use");

    /**
     * Build attribute provider
     * 
     * @param attributes list to use as source for provider
     */
    public ElasticAttributeProvider(List<ElasticAttribute> attributes) {
        this.attributes = attributes;
    }

    @Override
    protected List<org.geoserver.web.wicket.GeoServerDataProvider.Property<ElasticAttribute>> getProperties() {
        return Arrays.asList(USE, NAME, TYPE, DEFAULT_GEOMETRY, SRID);
    }

    @Override
    protected List<ElasticAttribute> getItems() {
        return attributes;
    }
    
    protected void reload(Boolean useShortName) {
        this.useShortName = useShortName;
    }
    
    public Boolean getUseShortName() {
        return useShortName;
    }

}
