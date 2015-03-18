/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */

package mil.nga.giat.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import mil.nga.giat.data.elasticsearch.ElasticAttribute;
import mil.nga.giat.data.elasticsearch.ElasticDataStore;
import mil.nga.giat.data.elasticsearch.ElasticLayerConfiguration;

import org.apache.wicket.Component;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.form.AjaxButton;
import org.apache.wicket.ajax.markup.html.form.AjaxCheckBox;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.IChoiceRenderer;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CatalogBuilder;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.web.GeoServerApplication;
import org.geoserver.web.wicket.GeoServerDataProvider.Property;
import org.geoserver.web.wicket.GeoServerTablePanel;
import org.geoserver.web.wicket.ParamResourceModel;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Class to render and manage the Elasticsearch modal dialog This dialog allow 
 * the user to choice which Elasticsearch attributes include in layers, selects 
 * attribute to use as GEOMETRY.
 */
public abstract class ElasticConfigurationPage extends Panel {

    private static final long serialVersionUID = 5615867383881988931L;

    private static final Logger LOGGER = Logging.getLogger(ElasticConfigurationPage.class);

    private FeedbackPanel feedbackPanel;

    private static final List GEOMETRY_TYPES = Arrays.asList(Geometry.class,
            GeometryCollection.class, Point.class, MultiPoint.class, LineString.class,
            MultiLineString.class, Polygon.class, MultiPolygon.class);

    /**
     * Constructs the dialog to set Elasticsearch attributes and configuration 
     * options.
     * 
     * @see {@link ElasticAttributeProvider}
     * @see {@link ElasticAttribute}
     * 
     */
    public ElasticConfigurationPage(String panelId, final IModel model) {
        super(panelId, model);

        ResourceInfo ri = (ResourceInfo) model.getObject();

        final Form elastic_form = new Form("es_form", new CompoundPropertyModel(this));
        add(elastic_form);

        List<ElasticAttribute> attributes = fillElasticAttributes(ri)
                .getAttributes();
        final ElasticAttributeProvider attProvider = new ElasticAttributeProvider(attributes);

        final GeoServerTablePanel<ElasticAttribute> elasticAttributePanel = getElasticAttributePanel(attProvider);
        elastic_form.add(elasticAttributePanel);

        final Boolean useShortName;
        if (!attributes.isEmpty() && attributes.get(0).getUseShortName() != null) {
            useShortName = attributes.get(0).getUseShortName();
        } else {
            useShortName = false;
        }
        AjaxCheckBox checkBox = new AjaxCheckBox("useShortName", Model.of(useShortName)) {
            @Override
            protected void onUpdate(AjaxRequestTarget target) {
                final boolean useShortName = (Boolean) this.getDefaultModelObject();
                for (final ElasticAttribute attribute : attProvider.getItems()) {
                    attribute.setUseShortName(useShortName);
                }
                attProvider.reload(useShortName);
                target.addComponent(elasticAttributePanel);
            }
        };
        checkBox.setOutputMarkupId(true);
        elastic_form.add(checkBox);

        elastic_form.add(new AjaxButton("es_save") {
            protected void onSubmit(AjaxRequestTarget target, Form form) {
                onSave(target);
            }
        });

        feedbackPanel = new FeedbackPanel("es_feedback");
        feedbackPanel.setOutputMarkupId(true);
        elastic_form.add(feedbackPanel);
    }

    /**
     * Do nothing
     */
    protected void onCancel(AjaxRequestTarget target) {
        done(target, null, null);
    }

    /**
     * Validates Elasticsearch attributes configuration and stores the 
     * Elasticsearch layer configuration into feature type metadata as 
     * {@link ElasticLayerConfiguration#KEY} <br>
     * Validation include the follow rules <li>One attribute must be a GEOMETRY.
     * 
     * @see {@link ElasticLayerConfiguration}
     * @see {@link FeatureTypeInfo#getMetadata}
     */
    protected void onSave(AjaxRequestTarget target) {
        try {
            ResourceInfo ri = (ResourceInfo) getDefaultModel().getObject();
            ElasticLayerConfiguration layerConfiguration = fillElasticAttributes(ri);

            Boolean geomSet = false;
            // Validate configuration
            for (ElasticAttribute att : layerConfiguration.getAttributes()) {
                if (Geometry.class.isAssignableFrom(att.getType()) && att.isUse()) {
                    geomSet = true;
                }
            }
            if (!geomSet) {
                error(new ParamResourceModel("geomEmptyFailure", ElasticConfigurationPage.this)
                .getString());
            }

            Catalog catalog = ((GeoServerApplication) this.getPage().getApplication()).getCatalog();
            LayerInfo layerInfo = catalog.getLayerByName(ri.getQualifiedName());
            FeatureTypeInfo typeInfo;
            Boolean isNew = true;
            if (layerInfo == null) {
                // New
                DataStoreInfo dsInfo = catalog.getStore(ri.getStore().getId(), DataStoreInfo.class);
                ElasticDataStore ds = (ElasticDataStore) dsInfo.getDataStore(null);
                CatalogBuilder builder = new CatalogBuilder(catalog);
                builder.setStore(dsInfo);
                ds.setElasticConfigurations(layerConfiguration);
                typeInfo = builder.buildFeatureType(ds.getFeatureSource(ri.getQualifiedName()));
                typeInfo.getMetadata().put(ElasticLayerConfiguration.KEY, layerConfiguration);
                layerInfo = builder.buildLayer(typeInfo);
            } else {
                // Update
                isNew = false;
                typeInfo = (FeatureTypeInfo) layerInfo.getResource();
                typeInfo.getMetadata().put(ElasticLayerConfiguration.KEY, layerConfiguration);
            }
            done(target, layerInfo, isNew);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            error(new ParamResourceModel("creationFailure", this, e).getString());
        }
    }

    /*
     * Load ElasticLayerConfiguration configuration before shows on table Reloads 
     * Elasticsearch attributes from datastore and merge it with user attributes 
     * configurations
     */
    private ElasticLayerConfiguration fillElasticAttributes(ResourceInfo ri) {
        ElasticLayerConfiguration elasticLayerConfiguration = (ElasticLayerConfiguration) ri.getMetadata()
                .get(ElasticLayerConfiguration.KEY);
        try {
            ArrayList<ElasticAttribute> result = new ArrayList<ElasticAttribute>();
            Map<String, ElasticAttribute> tempMap = new HashMap<String, ElasticAttribute>();
            if (elasticLayerConfiguration != null) {
                for (ElasticAttribute att : elasticLayerConfiguration.getAttributes()) {
                    tempMap.put(att.getName(), att);
                }
            } else {
                tempMap.clear();
                elasticLayerConfiguration = new ElasticLayerConfiguration(new ArrayList<ElasticAttribute>());
                elasticLayerConfiguration.setLayerName(ri.getName());
                ri.getMetadata().put(ElasticLayerConfiguration.KEY, elasticLayerConfiguration);
            }
            ElasticDataStore dataStore = (ElasticDataStore) ((DataStoreInfo) ri.getStore())
                    .getDataStore(new NullProgressListener());
            List<ElasticAttribute> attributes = dataStore
                    .getElasticAttributes(elasticLayerConfiguration.getLayerName());
            for (ElasticAttribute at : attributes) {
                if (tempMap.containsKey(at.getName())) {
                    ElasticAttribute prev = tempMap.get(at.getName());
                    at = prev;
                }
                result.add(at);
            }
            elasticLayerConfiguration.getAttributes().clear();
            elasticLayerConfiguration.getAttributes().addAll(result);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return elasticLayerConfiguration;
    }

    /*
     * Builds attribute table
     */
    private GeoServerTablePanel<ElasticAttribute> getElasticAttributePanel(
            ElasticAttributeProvider attProvider) {
        GeoServerTablePanel<ElasticAttribute> atts = new GeoServerTablePanel<ElasticAttribute>(
                "esAttributes", attProvider) {
            @Override
            protected Component getComponentForProperty(String id, IModel itemModel,
                    Property<ElasticAttribute> property) {
                ElasticAttribute att = (ElasticAttribute) itemModel.getObject();
                boolean isGeometry = att.getType() != null
                        && Geometry.class.isAssignableFrom(att.getType());
                if (property == ElasticAttributeProvider.NAME && isGeometry) {
                    Fragment f = new Fragment(id, "label", ElasticConfigurationPage.this);
                    f.add(new Label("label", att.getDisplayName() + "*"));
                    return f;
                } else if (property == ElasticAttributeProvider.TYPE && isGeometry) {
                    Fragment f = new Fragment(id, "geometry", ElasticConfigurationPage.this);
                    f.add(new DropDownChoice("geometry", new PropertyModel(itemModel, "type"),
                            GEOMETRY_TYPES, new GeometryTypeRenderer()));
                    return f;
                } else if (property == ElasticAttributeProvider.USE) {
                    Fragment f = new Fragment(id, "checkboxUse", ElasticConfigurationPage.this);
                    f.add(new CheckBox("use", new PropertyModel<Boolean>(itemModel, "use")));
                    return f;
                } else if (property == ElasticAttributeProvider.DEFAULT_GEOMETRY) {
                    if (isGeometry) {
                        Fragment f = new Fragment(id, "checkboxDefaultGeometry",
                                ElasticConfigurationPage.this);
                        f.add(new CheckBox("defaultGeometry", new PropertyModel<Boolean>(itemModel,
                                "defaultGeometry")));
                        return f;
                    } else {
                        Fragment f = new Fragment(id, "empty", ElasticConfigurationPage.this);
                        return f;
                    }
                } else if (property == ElasticAttributeProvider.SRID) {
                    if (isGeometry) {
                        Fragment f = new Fragment(id, "label", ElasticConfigurationPage.this);
                        f.add(new Label("label", String.valueOf(att.getSrid())));
                        return f;
                    } else {
                        Fragment f = new Fragment(id, "empty", ElasticConfigurationPage.this);
                        return f;
                    }
                }
                return null;
            }
        };
        atts.setOutputMarkupId(true);
        atts.setFilterVisible(false);
        atts.setSortable(false);
        atts.setPageable(false);
        atts.setOutputMarkupId(true);
        return atts;

    }

    /*
     * Render geometry type select
     */
    private static class GeometryTypeRenderer implements IChoiceRenderer {

        public Object getDisplayValue(Object object) {
            return ((Class) object).getSimpleName();
        }

        public String getIdValue(Object object, int index) {
            return (String) getDisplayValue(object);
        }

    }

    /**
     * Abstract method to implements in panel that opens the dialog to close the dialog itself <br>
     * This method is called after modal executes its operation
     * 
     * @param target ajax response target
     * @param layerInfo contains attribute configuration
     * @param isNew used to communicate to parent if the attributes configuration if for new or for
     *        existing layer
     * 
     * @see {@link #onSave}
     * @see {@link #onCancel}
     * 
     */
    abstract void done(AjaxRequestTarget target, LayerInfo layerInfo, Boolean isNew);

}
