/*
 *  Licensed to GraphHopper GmbH under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for
 *  additional information regarding copyright ownership.
 *
 *  GraphHopper GmbH licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except in
 *  compliance with the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.graphhopper.reader.postgis;

import com.graphhopper.reader.DataReader;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.GraphStorage;
import com.graphhopper.storage.NodeAccess;
import java.io.IOException;
import java.lang.ref.WeakReference;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.geotools.jdbc.JDBCDataStoreFactory;

/**
 * PostgisReader takes care of reading a PostGIS table and writing it to a road network graph
 *
 * @author Vikas Veshishth
 * @author Philip Welch
 * @author Mario Basa
 * @author Robin Boldt
 */
public abstract class PostgisReader implements DataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgisReader.class);

    private final GraphStorage graphStorage;
    private final NodeAccess nodeAccess;
    protected final Graph graph;
    protected EncodingManager encodingManager;

    private final Map<String, String> postgisParams;

    public PostgisReader(GraphHopperStorage ghStorage,
                         Map<String, String> postgisParams) {

        this.graphStorage = ghStorage;
        this.graph = ghStorage;
        this.nodeAccess = graph.getNodeAccess();
        this.encodingManager = ghStorage.getEncodingManager();

        this.postgisParams = postgisParams;
    }

    @Override
    public void readGraph() {
        graphStorage.create(1000);
        processJunctions();
        processRoads();
        finishReading();
    }

    abstract void processJunctions();

    abstract void processRoads();

    /**
     * This method will be called in the end to release the objects
     */
    protected abstract void finishReading();

    protected SimpleFeatureIterator getFeatureIterator(DataStore dataStore, String tableName) {

        if (dataStore == null)
            throw new IllegalArgumentException("DataStore cannot be null for getFeatureIterator");

        LOGGER.info("Getting the feature iterator for " + tableName);

        try {
            SimpleFeatureSource source = dataStore.getFeatureSource(tableName);
            SimpleFeatureType inputType = dataStore.getSchema(tableName);
            Filter filter = getFilter(source);
            
//            Query query = new Query(tableName, )
            SimpleFeatureCollection collection = source.getFeatures(filter);
            SimpleFeatureIterator featureIterator = collection.features();
            
            return featureIterator;

        } catch (IOException e) {
            throw Utils.asUnchecked(e);
        }
    }

    /**
     * Filters can help a lot when you need to limit the results returned from PostGIS.
     * A Filter can be used similar to the WHERE clause in regular SQL statements.
     * It's easy to filter geometries that have a certain attributes, are in certain BBoxes, Polygons, etc.
     * You can find a lot of sample filters here: https://github.com/geotools/geotools/blob/master/docs/src/main/java/org/geotools/main/FilterExamples.java
     * <p>
     * By default, all features are returned.
     */
    protected Filter getFilter(FeatureSource source) {
        return Filter.INCLUDE;
    }

    protected DataStore openPostGisStore() {
        try {
            LOGGER.info("Opening DB connection to " + this.postgisParams.get("dbtype") + " " + this.postgisParams.get("host") + ":" + this.postgisParams.get("port").toString() + " to database " + this.postgisParams.get("database") + " schema " + this.postgisParams.get("schema"));
            
            DataStore ds = DataStoreFinder.getDataStore(this.postgisParams);
            if (ds == null)
                throw new IllegalArgumentException("Error Connecting to Database ");
            return ds;

        } catch (Exception e) {
            throw Utils.asUnchecked(e);
        }
    }

    /**
     * This method can be used to filter features. One way to use it is to filter for features withing a certain BBox
     *
     * @return true if the feature should be accepted
     */
    protected boolean acceptFeature(SimpleFeature feature) {
        return true;
    }

    /**
     * Returns the coordinates of a feature.
     */
    protected List<Coordinate[]> getCoords(SimpleFeature feature) {
        ArrayList<Coordinate[]> ret = new ArrayList<>();
        if (feature == null)
            return ret;
        Object coords = feature.getDefaultGeometry();
        if (coords == null)
            return ret;

        if (coords instanceof LineString) {
            ret.add(((LineString) coords).getCoordinates());
        } else if (coords instanceof MultiLineString) {
            MultiLineString mls = (MultiLineString) coords;
            int n = mls.getNumGeometries();
            for (int i = 0; i < n; i++) {
                ret.add(mls.getGeometryN(i).getCoordinates());
            }
        }

        return ret;
    }

    /*
     * Get longitude using the current long-lat order convention
     */
    protected double lng(Coordinate coordinate) {
        return coordinate.getOrdinate(Coordinate.X);
    }

    /*
     * Get latitude using the current long-lat order convention
     */
    protected double lat(Coordinate coordinate) {
        return coordinate.getOrdinate(Coordinate.Y);
    }
    
    /*
     * Get elevation using the current long-lat order convention
     */
    protected double ele(Coordinate coordinate) {
        return coordinate.getOrdinate(Coordinate.Z);
    }

    protected void saveTowerPosition(int nodeId, Coordinate point) {
        nodeAccess.setNode(nodeId, lat(point), lng(point), ele(point));
    }
}