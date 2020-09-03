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

import com.graphhopper.coll.GHObjectIntHashMap;
import com.graphhopper.reader.DataReader;
import com.graphhopper.reader.PillarInfo;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.dem.EdgeSampling;
import com.graphhopper.reader.dem.ElevationProvider;
import com.graphhopper.reader.dem.GraphElevationSmoothing;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.IntsRef;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistanceCalcEarth;
import com.graphhopper.util.DouglasPeucker;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.Helper;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.GHPoint;
import org.locationtech.jts.geom.Coordinate;
import org.geotools.data.DataStore;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static com.graphhopper.util.Helper.*;

/**
 * Reads OSM data from Postgis and uses it in GraphHopper
 *
 * @author Vikas Veshishth
 * @author Philip Welch
 * @author Mario Basa
 * @author Robin Boldt
 */
    public class OSMPostgisReaderOld extends PostgisReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(OSMPostgisReaderOld.class);

    private static final int COORD_STATE_UNKNOWN = 0;
    private static final int COORD_STATE_PILLAR = -2;
    private static final int FIRST_NODE_ID = 1;
    private final String[] tagsToCopy;
    private File roadsFile;
    private GHObjectIntHashMap<Coordinate> coordState = new GHObjectIntHashMap<>(1000, 0.7f);
    private final DistanceCalc distCalc = DistanceCalcEarth.DIST_EARTH;
    private int nextNodeId = FIRST_NODE_ID;
    protected long zeroCounter = 0;
    private final IntsRef tempRelFlags;
    
    private final NodeAccess nodeAccess;
    protected PillarInfo pillarInfo;
    
    private boolean smoothElevation = false;
    private ElevationProvider eleProvider = ElevationProvider.NOOP;
    
//    private double[] elevations = {};
    private List<Double> elevations = new ArrayList<>();
    private boolean doSimplify = true;
    private final DouglasPeucker simplifyAlgo = new DouglasPeucker();
    private double longEdgeSamplingDistance = 0;

    public OSMPostgisReaderOld(GraphHopperStorage ghStorage, Map<String, String> postgisParams) {
        super(ghStorage, postgisParams);
        
        this.nodeAccess = graph.getNodeAccess();
        
        this.pillarInfo = new PillarInfo(nodeAccess.is3D(), ghStorage.getDirectory());
        
        String tmpTagsToCopy = postgisParams.get("tags_to_copy");
        if (tmpTagsToCopy == null || tmpTagsToCopy.isEmpty()) {
            this.tagsToCopy = new String[]{};
        } else {
            this.tagsToCopy = tmpTagsToCopy.split(",");
        }
        tempRelFlags = encodingManager.createRelationFlags();
        if (tempRelFlags.length != 2)
            throw new IllegalArgumentException("Cannot use relation flags with != 2 integers");
        // TODO relations are set empty by default, add relation handling
        tempRelFlags.ints[0] = (int) 0L;
        tempRelFlags.ints[1] = (int) 0L;
    }

    @Override
    void processJunctions() {
        DataStore dataStore = null;
        FeatureIterator<SimpleFeature> roads = null;
        int tmpJunctionCounter = 0;

        try {
            dataStore = openPostGisStore();
            roads = getFeatureIterator(dataStore, roadsFile.getName());

            HashSet<Coordinate> tmpSet = new HashSet<>();
            while (roads.hasNext()) {
                SimpleFeature road = roads.next();

                if (!acceptFeature(road)) {
                    continue;
                }

                for (Coordinate[] points : getCoords(road)) {
                    tmpSet.clear();
                    for (int i = 0; i < points.length; i++) {
                        Coordinate c = points[i];
                        c.z = getElevation(c.y, c.x);
                        c = roundCoordinate(c);

//                        LOGGER.info("Pillar elevation " + String.valueOf(c.z));

                        // don't add the same coord twice for the same edge - happens with bad geometry, i.e.
                        // duplicate coords or a road which forms a circle (e.g. roundabout)
                        if (tmpSet.contains(c))
                            continue;

                        tmpSet.add(c);

                        // skip if its already a node
                        int state = coordState.get(c);
                        if (state >= FIRST_NODE_ID) {
                            continue;
                        }

                        if (i == 0 || i == points.length - 1 || state == COORD_STATE_PILLAR) {
                            // turn into a node if its the first or last
                            // point, or already appeared in another edge
                            int nodeId = nextNodeId++;
                            coordState.put(c, nodeId);
                            saveTowerPosition(nodeId, c);
                        } else if (state == COORD_STATE_UNKNOWN) {
                            // mark it as a pillar (which may get upgraded
                            // to an edge later)
                            coordState.put(c, COORD_STATE_PILLAR);
                        }

                        if (++tmpJunctionCounter % 100_000 == 0) {
                            LOGGER.info(nf(tmpJunctionCounter) + " (junctions), junctionMap:" + nf(coordState.size())
                                    + " " + Helper.getMemInfo());
                        }
                    }
                }
            }
        } finally {
            if (roads != null) {
                roads.close();
            }
            if (dataStore != null) {
                dataStore.dispose();
            }
        }

        if (nextNodeId == FIRST_NODE_ID)
            throw new IllegalArgumentException("No data found for roads file " + roadsFile);

        LOGGER.info("Number of junction points : " + (nextNodeId - FIRST_NODE_ID));
    }
    
    @Override
    void processRoads() {

        DataStore dataStore = null;
        FeatureIterator<SimpleFeature> roads = null;

        int tmpEdgeCounter = 0;

        try {
            dataStore = openPostGisStore();
            roads = getFeatureIterator(dataStore, roadsFile.getName());

            while (roads.hasNext()) {
                SimpleFeature road = roads.next();

                if (!acceptFeature(road)) {
                    continue;
                }

                for (Coordinate[] points : getCoords(road)) {
                    // Parse all points in the geometry, splitting into
                    // individual GraphHopper edges
                    // whenever we find a node in the list of points
                    Coordinate startTowerPnt = null;
                    List<Coordinate> pillars = new ArrayList<>();
                    for (Coordinate point : points) {
                        point = roundCoordinate(point);
                        if (startTowerPnt == null) {
                            startTowerPnt = point;
                        } else {
                            int state = coordState.get(point);
                            if (state >= FIRST_NODE_ID) {
                                int fromTowerNodeId = coordState.get(startTowerPnt);
                                int toTowerNodeId = state;

                                // get distance and estimated centre
                                GHPoint estmCentre = new GHPoint(
                                        0.5 * (lat(startTowerPnt) + lat(point)),
                                        0.5 * (lng(startTowerPnt) + lng(point)));
                                PointList pillarNodes = new PointList(pillars.size(), nodeAccess.is3D());
                                
                                for (Coordinate pillar : pillars) {
                                    if (pillarNodes.is3D()) {
                                        double lat = lat(pillar);
                                        double lng = lng(pillar);
                                        double ele = Helper.round6(this.getElevation(lat, lng));
//                                        double ele = this.getElevation(lat, lng);
//                                        this.elevations.add(ele);
//                                        LOGGER.info("Pillar elevation " + String.valueOf(ele));
                                        pillarNodes.add(lat, lng, ele);
                                    } else {
                                        pillarNodes.add(lat(pillar), lng(pillar));
                                    }
                                }

                                double distance = getWayLength(startTowerPnt, pillars, road, point);
                                addEdge(fromTowerNodeId, toTowerNodeId, road, distance, estmCentre, pillarNodes);
                                startTowerPnt = point;
                                pillars.clear();

                                if (++tmpEdgeCounter % 1_000_000 == 0) {
                                    LOGGER.info(nf(tmpEdgeCounter) + " (edges) " + Helper.getMemInfo());
                                }
                            } else {
                                pillars.add(point);
                            }
                        }
                    }
                }

            }
        } finally {
            if (roads != null) {
                roads.close();
            }

            if (dataStore != null) {
                dataStore.dispose();
            }
            
            
            
            this.elevations.forEach(l -> {
                if (l > max) {
                    max = l;
                }
                if (l < min) {
                    min = l;
                }
           });
           LOGGER.info("Min/max " + String.valueOf(min) + " " + String.valueOf(max));
        }
    }
    
    double min = 0;
    double max = 0;

    @Override
    protected void finishReading() {
        this.coordState.clear();
        this.coordState = null;
        this.pillarInfo.clear();
        this.encodingManager.releaseParsers();
        this.eleProvider.release();
        LOGGER.info("Finished reading. Zero Counter " + nf(zeroCounter) + " " + Helper.getMemInfo());
    }

    protected double getWayLength(Coordinate start, List<Coordinate> pillars, SimpleFeature road, Coordinate end) {
        double distance = 0;

        Coordinate previous = start;
        for (Coordinate point : pillars) {
            distance += distCalc.calcDist(lat(previous), lng(previous), lat(point), lng(point));
            previous = point;
        }
        distance += distCalc.calcDist(lat(previous), lng(previous), lat(end), lng(end));

        if (distance < 0.0001) {
            // As investigation shows often two paths should have crossed via one identical point
            // but end up in two very close points.
            zeroCounter++;
            distance = 0.0001;
        }

        if (Double.isNaN(distance)) {
            LOGGER.warn("Bug in OSM or GraphHopper. Illegal tower node distance " + distance + " reset to 1m, osm way " + distance);
            distance = 1;
        }

        double maxDistance = (Integer.MAX_VALUE - 1) / 1000d;
        if (Double.isInfinite(distance) || distance > maxDistance) {
            // Too large is very rare and often the wrong tagging. See #435 
            // so we can avoid the complexity of splitting the way for now (new towernodes would be required, splitting up geometry etc)
            LOGGER.warn("Bug in OSM or GraphHopper. Too big tower node distance " + distance + " reset to large value, osm way " + getOSMId(road));
            distance = maxDistance;
        }

        return distance;
    }

    @Override
    public DataReader setFile(File file) {
        this.roadsFile = file;
        return this;
    }

    @Override
    public DataReader setElevationProvider(ElevationProvider ep) {
        if (eleProvider == null)
            throw new IllegalStateException("Use the NOOP elevation provider instead of null or don't call setElevationProvider");

        if (!nodeAccess.is3D() && ElevationProvider.NOOP != eleProvider)
            throw new IllegalStateException("Make sure you graph accepts 3D data");

        this.eleProvider = ep;
        return this;
    }

    @Override
    public DataReader setWorkerThreads(int workerThreads) {
        // Its only single-threaded
        return this;
    }

    @Override
    public DataReader setWayPointMaxDistance(double wayPointMaxDistance) {
        doSimplify = wayPointMaxDistance > 0;
        simplifyAlgo.setMaxDistance(wayPointMaxDistance);
        return this;
    }

    @Override
    public DataReader setSmoothElevation(boolean smoothElevation) {
        this.smoothElevation = smoothElevation;
        return this;
    }

    @Override
    public Date getDataDate() {
        return null;
    }

    private void addEdge(int fromTower, int toTower, SimpleFeature road, double towerNodeDistance, GHPoint estmCentre, PointList pillarNodes) {
        EdgeIteratorState edge = graph.edge(fromTower, toTower);

        // Smooth the elevation before calculating the distance because the distance will be incorrect if calculated afterwards
         // read the OSM id, should never be null
        long wayOsmId = getOSMId(road);
        
        if (this.smoothElevation)
            pillarNodes = GraphElevationSmoothing.smoothElevation(pillarNodes);

        // sample points along long edges
        if (this.longEdgeSamplingDistance < Double.MAX_VALUE && pillarNodes.is3D())
            pillarNodes = EdgeSampling.sample(wayOsmId, pillarNodes, longEdgeSamplingDistance, distCalc, eleProvider);

            // Distance will not  take into effect Elevation - need to fix!
//        double towerNodeDistance = distCalc.calcDistance(pillarNodes);
        
       
//        double towerNodeDistance = getWayLength(startTowerPnt, pillars, point);
        
//        double towerNodeDistance = pillarNodes.calcDistance(distCalc);
        
        // Make a temporary ReaderWay object with the properties we need so we
        // can use the enocding manager
        // We (hopefully don't need the node structure on here as we're only
        // calling the flag
        // encoders, which don't use this...
            ReaderWay way = new ReaderWay(wayOsmId);

//        way.setTag("estimated_distance", towerNodeDistance);
        way.setTag("estimated_center", estmCentre);

        // read the highway type
        Object type = road.getAttribute("fclass");
        if (type != null) {
            way.setTag("highway", type.toString());
        }

        // read maxspeed filtering for 0 which for Geofabrik shapefiles appears
        // to correspond to no tag
        Object maxSpeed = road.getAttribute("maxspeed");
        if (maxSpeed != null && !maxSpeed.toString().trim().equals("0")) {
            way.setTag("maxspeed", maxSpeed.toString());
        }

        for (String tag : tagsToCopy) {
            Object val = road.getAttribute(tag);
            if (val != null) {
                way.setTag(tag, val);
            }
        }

        // read oneway
//        Object oneway = road.getAttribute("oneway");
//        if (oneway != null) {
//            // Geofabrik is using an odd convention for oneway field in
//            // shapefile.
//            // We map back to the standard convention so that tag can be dealt
//            // with correctly by the flag encoder.
//            String val = toLowerCase(oneway.toString().trim());
//            if (val.equals("b")) {
//                // both ways
//                val = "no";
//            } else if (val.equals("t")) {
//                // one way against the direction of digitisation
//                val = "-1";
//            } else if (val.equals("f")) {
//                // one way Forward in the direction of digitisation
//                val = "yes";
//            } else {
//                throw new RuntimeException("Unrecognised value of oneway field \"" + val
//                        + "\" found in road with OSM id " + id);
//            }
//
//            way.setTag("oneway", val);
//        }

        // Process the flags using the encoders
        EncodingManager.AcceptWay acceptWay = new EncodingManager.AcceptWay();
        if (!encodingManager.acceptWay(way, acceptWay)) {
            return;
        }

        IntsRef edgeFlags = encodingManager.handleWayTags(way, acceptWay, tempRelFlags);
        if (edgeFlags.isEmpty())
            return;

        edge.setDistance(towerNodeDistance);
        edge.setFlags(edgeFlags);
        
        if (doSimplify && pillarNodes.size() > 2) {
//            LOGGER.info("Simplifying");
            simplifyAlgo.simplify(pillarNodes);
        }
        
        if (pillarNodes.size() > 2)
            edge.setWayGeometry(pillarNodes);

        encodingManager.applyWayTags(way, edge);
    }

    private long getOSMId(SimpleFeature road) {
        long id = Long.parseLong(road.getAttribute("osm_id").toString());
        return id;
    }

    private Coordinate roundCoordinate(Coordinate c) {
        c.x = Helper.round6(c.x);
        c.y = Helper.round6(c.y);

        if (!Double.isNaN(c.z))
            c.z = Helper.round6(c.z);

        return c;
    }
    
    protected double getElevation(double lat, double lng) {
        return eleProvider.getEle(lat, lng);
    }
    
    @Override
    public DataReader setLongEdgeSamplingDistance(double longEdgeSamplingDistance) {
        this.longEdgeSamplingDistance = longEdgeSamplingDistance;
        return this;
    }
    
    @Override
    public DataReader setWayPointElevationMaxDistance(double elevationWayPointMaxDistance) {
        simplifyAlgo.setElevationMaxDistance(elevationWayPointMaxDistance);
        return this;
    }
    
}