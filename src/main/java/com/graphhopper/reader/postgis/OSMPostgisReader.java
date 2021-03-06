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

import java.util.HashMap;

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIndexedContainer;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongSet;
import com.graphhopper.coll.GHIntLongHashMap;
import com.graphhopper.coll.GHLongHashSet;
import com.graphhopper.coll.GHLongIntBTree;
import com.graphhopper.coll.GHLongLongHashMap;
import com.graphhopper.coll.LongIntMap;
import com.graphhopper.reader.osm.OSMReader;
import com.graphhopper.reader.DataReader;
import com.graphhopper.reader.OSMTurnRelation;
import com.graphhopper.reader.PillarInfo;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.dem.EdgeSampling;
import com.graphhopper.reader.dem.ElevationProvider;
import com.graphhopper.reader.dem.GraphElevationSmoothing;
import com.graphhopper.reader.osm.OSMReaderUtility;
import com.graphhopper.routing.ev.BooleanEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.parsers.TurnCostParser;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.GraphStorage;
import com.graphhopper.storage.IntsRef;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.storage.TurnCostStorage;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistanceCalcEarth;
import com.graphhopper.util.DouglasPeucker;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.Helper;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.GHPoint;
import org.geotools.data.DataStore;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.data.postgis.HStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static com.graphhopper.util.Helper.*;
import com.graphhopper.util.StopWatch;
import java.io.IOException;
import java.util.logging.Level;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/**
 * Reads OSM data from Postgis and uses it in GraphHopper via the standard OSM reader
 *
 * @author Vikas Veshishth
 * @author Philip Welch
 * @author Mario Basa
 * @author Robin Boldt
 */
public class OSMPostgisReader implements DataReader, TurnCostParser.ExternalInternalMap {

    private static final String TYPE_DECODE = "nwr";
    
    protected static final int EMPTY_NODE = -1;
    // pillar node is >= 3
    protected static final int PILLAR_NODE = 1;
    // tower node is <= -3
    protected static final int TOWER_NODE = -2;
    private static final Logger LOGGER = LoggerFactory.getLogger(OSMPostgisReader.class);
    private final GraphStorage ghStorage;
    private final Graph graph;
    private final NodeAccess nodeAccess;
    private final LongIndexedContainer barrierNodeIds = new LongArrayList();
    private final DistanceCalc distCalc = DistanceCalcEarth.DIST_EARTH;
    private final DouglasPeucker simplifyAlgo = new DouglasPeucker();
    private boolean smoothElevation = false;
    private double longEdgeSamplingDistance = 0;
    protected long zeroCounter = 0;
    protected PillarInfo pillarInfo;
    private long locations;
    private long skippedLocations;
    private final EncodingManager encodingManager;
    private int workerThreads = 2;
    // Using the correct Map<Long, Integer> is hard. We need a memory efficient and fast solution for big data sets!
    //
    // very slow: new SparseLongLongArray
    // only append and update possible (no unordered storage like with this doubleParse): new OSMIDMap
    // same here: not applicable as ways introduces the nodes in 'wrong' order: new OSMIDSegmentedMap
    // memory overhead due to open addressing and full rehash:
    //        nodeOsmIdToIndexMap = new BigLongIntMap(expectedNodes, EMPTY);
    // smaller memory overhead for bigger data sets because of avoiding a "rehash"
    // remember how many times a node was used to identify tower nodes
    private LongIntMap osmNodeIdToInternalNodeMap;
    private GHLongLongHashMap osmNodeIdToNodeFlagsMap;
    private GHLongLongHashMap osmWayIdToRouteWeightMap;
    // stores osm way ids used by relations to identify which edge ids needs to be mapped later
    private GHLongHashSet osmWayIdSet = new GHLongHashSet();
    private IntLongMap edgeIdToOsmWayIdMap;
    private boolean doSimplify = true;
    private int nextTowerId = 0;
    private int nextPillarId = 0;
    // negative but increasing to avoid clash with custom created OSM files
    private long newUniqueOsmId = -Long.MAX_VALUE;
    private ElevationProvider eleProvider = ElevationProvider.NOOP;
    private File osmFile;
    private Date osmDataDate;
    private final IntsRef tempRelFlags;
    private final TurnCostStorage tcs;
    
    private Map<String, Object> postgisParams;
    
    public OSMPostgisReader(GraphHopperStorage ghStorage, Map<String, Object> postgisParams) {
        this.postgisParams = postgisParams;
        
        this.ghStorage = ghStorage;
        this.graph = ghStorage;
        this.nodeAccess = graph.getNodeAccess();
        this.encodingManager = ghStorage.getEncodingManager();

        osmNodeIdToInternalNodeMap = new GHLongIntBTree(200);
        osmNodeIdToNodeFlagsMap = new GHLongLongHashMap(200, .5f);
        osmWayIdToRouteWeightMap = new GHLongLongHashMap(200, .5f);
        pillarInfo = new PillarInfo(nodeAccess.is3D(), ghStorage.getDirectory());
        tempRelFlags = encodingManager.createRelationFlags();
        if (tempRelFlags.length != 2)
            throw new IllegalArgumentException("Cannot use relation flags with != 2 integers");

        tcs = graph.getTurnCostStorage();
        
    }
    
    protected DataStore openPostGisStore() {
        try {
//            LOGGER.info("Opening DB connection to " + this.postgisParams.get("dbtype") + " " + this.postgisParams.get("host") + ":" + this.postgisParams.get("port").toString() + " to database " + this.postgisParams.get("database") + " schema " + this.postgisParams.get("schema"));
            
            this.postgisParams.put(JDBCDataStoreFactory.FETCHSIZE.key, 100);
            DataStore ds = DataStoreFinder.getDataStore(this.postgisParams);
            if (ds == null)
                throw new IllegalArgumentException("Error Connecting to Database ");
            return ds;

        } catch (Exception e) {
            throw Utils.asUnchecked(e);
        }
    }

    void prepare() {
        DataStore dataStore = null;
        try {
            dataStore = openPostGisStore();
        } finally {
//            if (roads != null) {
//                roads.close();
//            }
            if (dataStore != null) {
                dataStore.dispose();
            }
        }
    }
    
    @Override
    public void readGraph() throws IOException {
        if (encodingManager == null)
            throw new IllegalStateException("Encoding manager was not set.");

//        if (osmFile == null)
//            throw new IllegalStateException("No OSM file specified");

//        if (!osmFile.exists())
//            throw new IllegalStateException("Your specified OSM file does not exist:" + osmFile.getAbsolutePath());

        DataStore dataStore = openPostGisStore();

        StopWatch sw1 = new StopWatch().start();
        preProcess(dataStore);
        sw1.stop();

        StopWatch sw2 = new StopWatch().start();
        try {
            writeOsmToGraph(dataStore);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(OSMPostgisReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        sw2.stop();
        
        dataStore.dispose();

        LOGGER.info("time pass1:" + (int) sw1.getSeconds() + "s, "
                + "pass2:" + (int) sw2.getSeconds() + "s, "
                + "total:" + (int) (sw1.getSeconds() + sw2.getSeconds()) + "s");
    }

    /**
     * Preprocessing of OSM file to select nodes which are used for highways. This allows a more
     * compact graph data structure.
     */
    void preProcess(DataStore dataStore) {
        LOGGER.info("Starting to process OSM db");
        long tmpWayCounter = 1;
        long tmpRelationCounter = 1;
        FeatureIterator<SimpleFeature>nodes = getFeatureIterator(dataStore, "planet_osm_ways_import");
        
         while (nodes.hasNext()) {
            SimpleFeature node = nodes.next();     

            ReaderWay way = new ReaderWay((long) node.getProperty("osm_id").getValue());
//            LOGGER.info("ATTRIBUTES " + node.getProperty("tags").getValue().getClass());
            long[] wayParsedNodes = Utils.LongsToPrimitive((Long[]) node.getProperty("nodes").getValue());
            way.getNodes().add(wayParsedNodes);
            
            HStore tags = (HStore) node.getProperty("tags").getValue();
            way.setTags(tags);
            
            boolean valid = filterWay(way);
            if (valid) {
//                LongIndexedContainer wayNodes = way.getNodes();
                int s = wayParsedNodes.length;
                for (int index = 0; index < s; index++) {
                    prepareHighwayNode(wayParsedNodes[index]);
                }

                if (++tmpWayCounter % 100_000 == 0) {
                    LOGGER.info(nf(tmpWayCounter) + " (preprocess), osmIdMap:" + nf(getNodeMap().getSize()) + " ("
                            + getNodeMap().getMemoryUsage() + "MB) " + Helper.getMemInfo());
                }
                if (++tmpWayCounter % 1_000_000 == 0) {
                    System.gc();
                }
            }
        }
        nodes.close();
         
        nodes = getFeatureIterator(dataStore, "planet_osm_rels_import");
        while (nodes.hasNext()) {
            SimpleFeature node = nodes.next();

            ReaderRelation relation = new ReaderRelation((long) node.getProperty("id").getValue());
//            LOGGER.info("ATTRIBUTES " + node.getProperty("tags").getValue().getClass());
            
            HStore tags = (HStore) node.getProperty("tags").getValue();
            relation.setTags(tags);
            
            HStore members = (HStore) node.getProperty("members").getValue();
            members.keySet().forEach(key -> {
                String role = members.get(key);
                int type = TYPE_DECODE.indexOf(key.charAt(0));
                long ref = Long.parseLong(key.substring(1));
                
                ReaderRelation.Member member = new ReaderRelation.Member(type, ref, role);
                relation.add(member);
            });
            
            if (!relation.isMetaRelation() && relation.hasTag("type", "route"))
                prepareWaysWithRelationInfo(relation);

            if (relation.hasTag("type", "restriction")) {
                prepareRestrictionRelation(relation);
            }

            if (++tmpRelationCounter % 100_000 == 0) {
                LOGGER.info(nf(tmpRelationCounter) + " (preprocess), osmWayMap:" + nf(getRelFlagsMapSize())
                        + " " + Helper.getMemInfo());
            }
            if (++tmpRelationCounter % 1_000_000 == 0) {
                System.gc();
            }
        }
        nodes.close();
//        try (OSMInput in = openOsmInputFile(osmFile)) {
//            long tmpWayCounter = 1;
//            long tmpRelationCounter = 1;
//            ReaderElement item;
//            while ((item = in.getNext()) != null) {
//                if (item.isType(ReaderElement.WAY)) {
//                    final ReaderWay way = (ReaderWay) item;
//                    boolean valid = filterWay(way);
//                    if (valid) {
//                        LongIndexedContainer wayNodes = way.getNodes();
//                        int s = wayNodes.size();
//                        for (int index = 0; index < s; index++) {
//                            prepareHighwayNode(wayNodes.get(index));
//                        }
//
//                        if (++tmpWayCounter % 10_000_000 == 0) {
//                            LOGGER.info(nf(tmpWayCounter) + " (preprocess), osmIdMap:" + nf(getNodeMap().getSize()) + " ("
//                                    + getNodeMap().getMemoryUsage() + "MB) " + Helper.getMemInfo());
//                        }
//                    }
//                } else if (item.isType(ReaderElement.RELATION)) {
//                    final ReaderRelation relation = (ReaderRelation) item;
//                    if (!relation.isMetaRelation() && relation.hasTag("type", "route"))
//                        prepareWaysWithRelationInfo(relation);
//
//                    if (relation.hasTag("type", "restriction")) {
//                        prepareRestrictionRelation(relation);
//                    }
//
//                    if (++tmpRelationCounter % 100_000 == 0) {
//                        LOGGER.info(nf(tmpRelationCounter) + " (preprocess), osmWayMap:" + nf(getRelFlagsMapSize())
//                                + " " + Helper.getMemInfo());
//                    }
//                } else if (item.isType(ReaderElement.FILEHEADER)) {
//                    final OSMFileHeader fileHeader = (OSMFileHeader) item;
//                    osmDataDate = Helper.createFormatter().parse(fileHeader.getTag("timestamp"));
//                }
//
//            }
//        } catch (Exception ex) {
//            throw new RuntimeException("Problem while parsing file", ex);
//        }
    }

    private void prepareRestrictionRelation(ReaderRelation relation) {
        List<OSMTurnRelation> turnRelations = createTurnRelations(relation);
        for (OSMTurnRelation turnRelation : turnRelations) {
            getOsmWayIdSet().add(turnRelation.getOsmIdFrom());
            getOsmWayIdSet().add(turnRelation.getOsmIdTo());
        }
    }

    /**
     * @return all required osmWayIds to process e.g. relations.
     */
    private LongSet getOsmWayIdSet() {
        return osmWayIdSet;
    }

    private IntLongMap getEdgeIdToOsmWayIdMap() {
        if (edgeIdToOsmWayIdMap == null)
            edgeIdToOsmWayIdMap = new GHIntLongHashMap(getOsmWayIdSet().size(), 0.5f);

        return edgeIdToOsmWayIdMap;
    }

    /**
     * Filter ways but do not analyze properties wayNodes will be filled with participating node ids.
     *
     * @return true the current xml entry is a way entry and has nodes
     */
    boolean filterWay(ReaderWay item) {
        // ignore broken geometry
        if (item.getNodes().size() < 2)
            return false;

        // ignore multipolygon geometry
        if (!item.hasTags())
            return false;
        
        return encodingManager.acceptWay(item, new EncodingManager.AcceptWay());
    }

    /**
     * Creates the graph with edges and nodes from the specified osm file.
     */
    private void writeOsmToGraph(DataStore dataStore) throws InterruptedException {
        int tmp = (int) Math.max(getNodeMap().getSize() / 50, 100);
        LOGGER.info("creating graph. Found nodes (pillar+tower):" + nf(getNodeMap().getSize()) + ", " + Helper.getMemInfo());
        ghStorage.create(tmp);
        long counter = 1;
        
        FeatureIterator<SimpleFeature>nodes = getFeatureIterator(dataStore, "planet_osm_nodes_import");
        
//        try (OSMInput in = openOsmInputFile(osmFile)) {
        LongIntMap nodeFilter = getNodeMap();

//        while ((item = in.getNext()) != null) {
        while (nodes.hasNext()) {
            SimpleFeature node = nodes.next();         

            ReaderNode element = new ReaderNode((long) node.getProperty("osm_id").getValue(), (double) node.getProperty("lat").getValue(), (double) node.getProperty("lon").getValue() );
            
            this.processElement(element, nodeFilter);
            if (++counter % 200_000 == 0) {
                LOGGER.info(nf(counter) + ", locs:" + nf(locations) + " (" + skippedLocations + ") " + Helper.getMemInfo());
//                System.gc();
            }
            if (++counter % 1_000_000 == 0) {
                System.gc();
            }
        }
        nodes.close();
        
        counter = 1;
        nodes = getFeatureIterator(dataStore, "planet_osm_ways_import");
        while (nodes.hasNext()) {
            SimpleFeature node = nodes.next();

            ReaderWay element = new ReaderWay((long) node.getProperty("osm_id").getValue());
//            LOGGER.info("ATTRIBUTES " + node.getProperty("tags").getValue().getClass());
            long[] wayNodes = Utils.LongsToPrimitive((Long[]) node.getProperty("nodes").getValue());
            element.getNodes().add(wayNodes);
            
            HStore tags = (HStore) node.getProperty("tags").getValue();
            element.setTags(tags);
            
//            node.getProperty("hsi").
//            element.setTag("hsi", node.getProperty("hsi").getValue());
            element.setTag("tci", node.getProperty("hsi").getValue());
            element.setTag("amb_rat", node.getProperty("amble_rating").getValue());
            element.setTag("amb_con_rat", node.getProperty("amble_contra_rating").getValue());
//            double hsi = node.getProperty("hsi").getValue();
            
            this.processElement(element, nodeFilter);
            if (++counter % 200_000 == 0) {
                LOGGER.info(nf(counter) + ", locs:" + nf(locations) + " (" + skippedLocations + ") " + Helper.getMemInfo());
//                System.gc();
            }
            if (++counter % 1_000_000 == 0) {
                System.gc();
            }
        }
        nodes.close();
        
        counter = 1;
        nodes = getFeatureIterator(dataStore, "planet_osm_rels_import");
        while (nodes.hasNext()) {
            SimpleFeature node = nodes.next();

            ReaderRelation element = new ReaderRelation((long) node.getProperty("id").getValue());
//            LOGGER.info("ATTRIBUTES " + node.getProperty("tags").getValue().getClass());
            
            HStore tags = (HStore) node.getProperty("tags").getValue();
            element.setTags(tags);
            
            HStore members = (HStore) node.getProperty("members").getValue();
            members.keySet().forEach(key -> {
                String role = members.get(key);
                int type = TYPE_DECODE.indexOf(key.charAt(0));
                long ref = Long.parseLong(key.substring(1));
                
                ReaderRelation.Member member = new ReaderRelation.Member(type, ref, role);
                element.add(member);
            });
            
            this.processElement(element, nodeFilter);
            if (++counter % 200_000 == 0) {
                LOGGER.info(nf(counter) + ", locs:" + nf(locations) + " (" + skippedLocations + ") " + Helper.getMemInfo());
//                System.gc();
            }
            if (++counter % 1_000_000 == 0) {
                System.gc();
            }
        }
        nodes.close();
        
        finishedReading();
        if (graph.getNodes() == 0)
            throw new RuntimeException("Graph after reading OSM must not be empty. Read " + counter + " items and " + locations + " locations");
        
    }
    
    private void processElement(ReaderElement item, LongIntMap nodeFilter) {
        switch (item.getType()) {
            case ReaderElement.NODE:
                if (nodeFilter.get(item.getId()) != EMPTY_NODE) {
                    processNode((ReaderNode) item);
//                } else {
//                    LOGGER.info("Skipping node " + String.valueOf(item.getId()));
                }
                break;

            case ReaderElement.WAY:
//                if (wayStart < 0) {
//                    LOGGER.info(nf(counter) + ", now parsing ways");
//                    wayStart = counter;
//                }
                processWay((ReaderWay) item);
                break;
            case ReaderElement.RELATION:
//                if (relationStart < 0) {
//                    LOGGER.info(nf(counter) + ", now parsing relations");
//                    relationStart = counter;
//                }
                processRelation((ReaderRelation) item);
                break;
            case ReaderElement.FILEHEADER:
                break;
            default:
                throw new IllegalStateException("Unknown type " + item.getType());
        }
        

//        if (in.getUnprocessedElements() > 0)
//            throw new IllegalStateException("Still unprocessed elements in reader queue " + in.getUnprocessedElements());

            // logger.info("storage nodes:" + storage.nodes() + " vs. graph nodes:" + storage.getGraph().nodes());
//        } catch (Exception ex) {
//            throw new RuntimeException("Couldn't process file " + osmFile + ", error: " + ex.getMessage(), ex);
//        }

        
    }
    
    protected FeatureIterator<SimpleFeature> getFeatureIterator(
            DataStore dataStore, String tableName) {

        if (dataStore == null)
            throw new IllegalArgumentException("DataStore cannot be null for getFeatureIterator");

        LOGGER.info("Getting the feature iterator for " + tableName);

        try {
            FeatureSource<SimpleFeatureType, SimpleFeature> source =
                    dataStore.getFeatureSource(tableName);

            Filter filter = getFilter(source);
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
            
            FeatureIterator<SimpleFeature> features = collection.features();
            return features;

        } catch (Exception e) {
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

//    protected OSMInput openOsmInputFile(File osmFile) throws XMLStreamException, IOException {
//        return new OSMInputFile(osmFile).setWorkerThreads(workerThreads).open();
//    }

    /**
     * Process properties, encode flags and create edges for the way.
     */
    protected void processWay(ReaderWay way) {
        if (way.getNodes().size() < 2)
            return;
        
        // ignore multipolygon geometry
        if (!way.hasTags())
            return;

        long wayOsmId = way.getId();

        EncodingManager.AcceptWay acceptWay = new EncodingManager.AcceptWay();
        if (!encodingManager.acceptWay(way, acceptWay))
            return;

        IntsRef relationFlags = getRelFlagsMap(way.getId());

        // TODO move this after we have created the edge and know the coordinates => encodingManager.applyWayTags
        LongArrayList osmNodeIds = way.getNodes();
        // Estimate length of ways containing a route tag e.g. for ferry speed calculation
        int first = getNodeMap().get(osmNodeIds.get(0));
        int last = getNodeMap().get(osmNodeIds.get(osmNodeIds.size() - 1));
        double firstLat = getTmpLatitude(first), firstLon = getTmpLongitude(first);
        double lastLat = getTmpLatitude(last), lastLon = getTmpLongitude(last);
        if (!Double.isNaN(firstLat) && !Double.isNaN(firstLon) && !Double.isNaN(lastLat) && !Double.isNaN(lastLon)) {
            double estimatedDist = distCalc.calcDist(firstLat, firstLon, lastLat, lastLon);
            // Add artificial tag for the estimated distance and center
            way.setTag("estimated_distance", estimatedDist);
            way.setTag("estimated_center", new GHPoint((firstLat + lastLat) / 2, (firstLon + lastLon) / 2));
        }

        if (way.getTag("duration") != null) {
            try {
                long dur = OSMReaderUtility.parseDuration(way.getTag("duration"));
                // Provide the duration value in seconds in an artificial graphhopper specific tag:
                way.setTag("duration:seconds", Long.toString(dur));
            } catch (Exception ex) {
                LOGGER.warn("Parsing error in way with OSMID=" + way.getId() + " : " + ex.getMessage());
            }
        }

        IntsRef edgeFlags = encodingManager.handleWayTags(way, acceptWay, relationFlags);
        if (edgeFlags.isEmpty())
            return;

        List<EdgeIteratorState> createdEdges = new ArrayList<>();
        // look for barriers along the way
        final int size = osmNodeIds.size();
        int lastBarrier = -1;
        for (int i = 0; i < size; i++) {
            long nodeId = osmNodeIds.get(i);
            long nodeFlags = getNodeFlagsMap().get(nodeId);
            // barrier was spotted and the way is passable for that mode of travel
            if (nodeFlags > 0) {
                if (isOnePassable(encodingManager.getAccessEncFromNodeFlags(nodeFlags), edgeFlags)) {
                    // remove barrier to avoid duplicates
                    getNodeFlagsMap().put(nodeId, 0);

                    // create shadow node copy for zero length edge
                    long newNodeId = addBarrierNode(nodeId);
                    if (i > 0) {
                        // start at beginning of array if there was no previous barrier
                        if (lastBarrier < 0)
                            lastBarrier = 0;

                        // add way up to barrier shadow node                        
                        int length = i - lastBarrier + 1;
                        LongArrayList partNodeIds = new LongArrayList();
                        partNodeIds.add(osmNodeIds.buffer, lastBarrier, length);
                        partNodeIds.set(length - 1, newNodeId);
                        createdEdges.addAll(addOSMWay(partNodeIds, edgeFlags, wayOsmId));

                        // create zero length edge for barrier
                        createdEdges.addAll(addBarrierEdge(newNodeId, nodeId, edgeFlags, nodeFlags, wayOsmId));
                    } else {
                        // run edge from real first node to shadow node
                        createdEdges.addAll(addBarrierEdge(nodeId, newNodeId, edgeFlags, nodeFlags, wayOsmId));

                        // exchange first node for created barrier node
                        osmNodeIds.set(0, newNodeId);
                    }
                    // remember barrier for processing the way behind it
                    lastBarrier = i;
                }
            }
        }

        // just add remainder of way to graph if barrier was not the last node
        if (lastBarrier >= 0) {
            if (lastBarrier < size - 1) {
                LongArrayList partNodeIds = new LongArrayList();
                partNodeIds.add(osmNodeIds.buffer, lastBarrier, size - lastBarrier);
                createdEdges.addAll(addOSMWay(partNodeIds, edgeFlags, wayOsmId));
            }
        } else {
            // no barriers - simply add the whole way
            createdEdges.addAll(addOSMWay(way.getNodes(), edgeFlags, wayOsmId));
        }

        for (EdgeIteratorState edge : createdEdges) {
            encodingManager.applyWayTags(way, edge);
        }
    }

    protected void processRelation(ReaderRelation relation) {
        if (tcs != null && relation.hasTag("type", "restriction"))
            storeTurnRelation(createTurnRelations(relation));
    }

    void storeTurnRelation(List<OSMTurnRelation> turnRelations) {
        for (OSMTurnRelation turnRelation : turnRelations) {
            int viaNode = getInternalNodeIdOfOsmNode(turnRelation.getViaOsmNodeId());
            // street with restriction was not included (access or tag limits etc)
            if (viaNode != EMPTY_NODE)
                encodingManager.handleTurnRelationTags(turnRelation, this, graph);
        }
    }

    /**
     * @return OSM way ID from specified edgeId. Only previously stored OSM-way-IDs are returned in
     * order to reduce memory overhead.
     */
    @Override
    public long getOsmIdOfInternalEdge(int edgeId) {
        return getEdgeIdToOsmWayIdMap().get(edgeId);
    }

    @Override
    public int getInternalNodeIdOfOsmNode(long nodeOsmId) {
        int id = getNodeMap().get(nodeOsmId);
        if (id < TOWER_NODE)
            return -id - 3;

        return EMPTY_NODE;
    }

    // TODO remove this ugly stuff via better preprocessing phase! E.g. putting every tags etc into a helper file!
    double getTmpLatitude(int id) {
        if (id == EMPTY_NODE)
            return Double.NaN;
        if (id < TOWER_NODE) {
            // tower node
            id = -id - 3;
            return nodeAccess.getLatitude(id);
        } else if (id > -TOWER_NODE) {
            // pillar node
            id = id - 3;
            return pillarInfo.getLatitude(id);
        } else
            // e.g. if id is not handled from preprocessing (e.g. was ignored via isInBounds)
            return Double.NaN;
    }

    double getTmpLongitude(int id) {
        if (id == EMPTY_NODE)
            return Double.NaN;
        if (id < TOWER_NODE) {
            // tower node
            id = -id - 3;
            return nodeAccess.getLongitude(id);
        } else if (id > -TOWER_NODE) {
            // pillar node
            id = id - 3;
            return pillarInfo.getLon(id);
        } else
            // e.g. if id is not handled from preprocessing (e.g. was ignored via isInBounds)
            return Double.NaN;
    }

    protected void processNode(ReaderNode node) {
        if (isInBounds(node)) {
            addNode(node);

            // analyze node tags for barriers
            if (node.hasTags()) {
                long nodeFlags = encodingManager.handleNodeTags(node);
                if (nodeFlags != 0)
                    getNodeFlagsMap().put(node.getId(), nodeFlags);
            }

            locations++;
        } else {
            skippedLocations++;
        }
    }

    boolean addNode(ReaderNode node) {
        int nodeType = getNodeMap().get(node.getId());
        if (nodeType == EMPTY_NODE)
            return false;

        double lat = node.getLat();
        double lon = node.getLon();
        double ele = getElevation(node);
        if (nodeType == TOWER_NODE) {
            addTowerNode(node.getId(), lat, lon, ele);
        } else if (nodeType == PILLAR_NODE) {
            pillarInfo.setNode(nextPillarId, lat, lon, ele);
            getNodeMap().put(node.getId(), nextPillarId + 3);
            nextPillarId++;
        }
        return true;
    }

    /**
     * The nodeFlags store the encoders to check for accessibility in edgeFlags. E.g. if nodeFlags==3, then the
     * accessibility of the first two encoders will be check in edgeFlags
     */
    private static boolean isOnePassable(List<BooleanEncodedValue> checkEncoders, IntsRef edgeFlags) {
        for (BooleanEncodedValue accessEnc : checkEncoders) {
            if (accessEnc.getBool(false, edgeFlags) || accessEnc.getBool(true, edgeFlags))
                return true;
        }
        return false;
    }

    protected double getElevation(ReaderNode node) {
        return eleProvider.getEle(node.getLat(), node.getLon());
    }

    void prepareWaysWithRelationInfo(ReaderRelation osmRelation) {
        for (ReaderRelation.Member member : osmRelation.getMembers()) {
            if (member.getType() != ReaderRelation.Member.WAY)
                continue;

            long osmId = member.getRef();
            IntsRef oldRelationFlags = getRelFlagsMap(osmId);

            // Check if our new relation data is better compared to the last one
            IntsRef newRelationFlags = encodingManager.handleRelationTags(osmRelation, oldRelationFlags);
            putRelFlagsMap(osmId, newRelationFlags);
        }
    }

    void prepareHighwayNode(long osmId) {
        int tmpGHNodeId = getNodeMap().get(osmId);
        if (tmpGHNodeId == EMPTY_NODE) {
            // osmId is used exactly once
            getNodeMap().put(osmId, PILLAR_NODE);
        } else if (tmpGHNodeId > EMPTY_NODE) {
            // mark node as tower node as it occurred at least twice times
            getNodeMap().put(osmId, TOWER_NODE);
        } else {
            // tmpIndex is already negative (already tower node)
        }
    }

    int addTowerNode(long osmId, double lat, double lon, double ele) {
        if (nodeAccess.is3D())
            nodeAccess.setNode(nextTowerId, lat, lon, ele);
        else
            nodeAccess.setNode(nextTowerId, lat, lon);

        int id = -(nextTowerId + 3);
        getNodeMap().put(osmId, id);
        nextTowerId++;
        return id;
    }

    /**
     * This method creates from an OSM way (via the osm ids) one or more edges in the graph.
     */
    Collection<EdgeIteratorState> addOSMWay(final LongIndexedContainer osmNodeIds, final IntsRef flags, final long wayOsmId) {
        PointList pointList = new PointList(osmNodeIds.size(), nodeAccess.is3D());
        List<EdgeIteratorState> newEdges = new ArrayList<>(5);
        int firstNode = -1;
        int lastIndex = osmNodeIds.size() - 1;
        int lastInBoundsPillarNode = -1;
        try {
            for (int i = 0; i < osmNodeIds.size(); i++) {
                long osmNodeId = osmNodeIds.get(i);
                int tmpNode = getNodeMap().get(osmNodeId);
                if (tmpNode == EMPTY_NODE)
                    continue;

                // skip osmIds with no associated pillar or tower id (e.g. !OSMReader.isBounds)
                if (tmpNode == TOWER_NODE)
                    continue;

                if (tmpNode == PILLAR_NODE) {
                    // In some cases no node information is saved for the specified osmId.
                    // ie. a way references a <node> which does not exist in the current file.
                    // => if the node before was a pillar node then convert into to tower node (as it is also end-standing).
                    if (!pointList.isEmpty() && lastInBoundsPillarNode > -TOWER_NODE) {
                        // transform the pillar node to a tower node
                        tmpNode = lastInBoundsPillarNode;
                        tmpNode = handlePillarNode(tmpNode, osmNodeId, null, true);
                        tmpNode = -tmpNode - 3;
                        if (pointList.getSize() > 1 && firstNode >= 0) {
                            // TOWER node
                            newEdges.add(addEdge(firstNode, tmpNode, pointList, flags, wayOsmId));
                            pointList.clear();
                            pointList.add(nodeAccess, tmpNode);
                        }
                        firstNode = tmpNode;
                        lastInBoundsPillarNode = -1;
                    }
                    continue;
                }

                if (tmpNode <= -TOWER_NODE && tmpNode >= TOWER_NODE)
                    throw new AssertionError("Mapped index not in correct bounds " + tmpNode + ", " + osmNodeId);

                if (tmpNode > -TOWER_NODE) {
                    boolean convertToTowerNode = i == 0 || i == lastIndex;
                    if (!convertToTowerNode) {
                        lastInBoundsPillarNode = tmpNode;
                    }

                    // PILLAR node, but convert to towerNode if end-standing
                    tmpNode = handlePillarNode(tmpNode, osmNodeId, pointList, convertToTowerNode);
                }

                if (tmpNode < TOWER_NODE) {
                    // TOWER node
                    tmpNode = -tmpNode - 3;

                    if (firstNode >= 0 && firstNode == tmpNode) {
                        // loop detected. See #1525 and #1533. Insert last OSM ID as tower node. Do this for all loops so that users can manipulate loops later arbitrarily.
                        long lastOsmNodeId = osmNodeIds.get(i - 1);
                        int lastGHNodeId = getNodeMap().get(lastOsmNodeId);
                        if (lastGHNodeId < TOWER_NODE) {
                            LOGGER.warn("Pillar node " + lastOsmNodeId + " is already a tower node and used in loop, see #1533. " +
                                    "Fix mapping for way " + wayOsmId + ", nodes:" + osmNodeIds);
                            break;
                        }

                        int newEndNode = -handlePillarNode(lastGHNodeId, lastOsmNodeId, pointList, true) - 3;
                        newEdges.add(addEdge(firstNode, newEndNode, pointList, flags, wayOsmId));
                        pointList.clear();
                        pointList.add(nodeAccess, newEndNode);
                        firstNode = newEndNode;
                    }

                    pointList.add(nodeAccess, tmpNode);
                    if (firstNode >= 0) {
                        newEdges.add(addEdge(firstNode, tmpNode, pointList, flags, wayOsmId));
                        pointList.clear();
                        pointList.add(nodeAccess, tmpNode);
                    }
                    firstNode = tmpNode;
                }
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Couldn't properly add edge with osm ids:" + osmNodeIds, ex);
            throw ex;
        }
        return newEdges;
    }

    EdgeIteratorState addEdge(int fromIndex, int toIndex, PointList pointList, IntsRef flags, long wayOsmId) {
        // sanity checks
        if (fromIndex < 0 || toIndex < 0)
            throw new AssertionError("to or from index is invalid for this edge " + fromIndex + "->" + toIndex + ", points:" + pointList);
        if (pointList.getDimension() != nodeAccess.getDimension())
            throw new AssertionError("Dimension does not match for pointList vs. nodeAccess " + pointList.getDimension() + " <-> " + nodeAccess.getDimension());

        // Smooth the elevation before calculating the distance because the distance will be incorrect if calculated afterwards
        if (this.smoothElevation)
            pointList = GraphElevationSmoothing.smoothElevation(pointList);

        // sample points along long edges
        if (this.longEdgeSamplingDistance < Double.MAX_VALUE && pointList.is3D())
            pointList = EdgeSampling.sample(wayOsmId, pointList, longEdgeSamplingDistance, distCalc, eleProvider);

        double towerNodeDistance = distCalc.calcDistance(pointList);

        if (towerNodeDistance < 0.001) {
            // As investigation shows often two paths should have crossed via one identical point 
            // but end up in two very close points.
            zeroCounter++;
            towerNodeDistance = 0.001;
        }

        double maxDistance = (Integer.MAX_VALUE - 1) / 1000d;
        if (Double.isNaN(towerNodeDistance)) {
            LOGGER.warn("Bug in OSM or GraphHopper. Illegal tower node distance " + towerNodeDistance + " reset to 1m, osm way " + wayOsmId);
            towerNodeDistance = 1;
        }

        if (Double.isInfinite(towerNodeDistance) || towerNodeDistance > maxDistance) {
            // Too large is very rare and often the wrong tagging. See #435 
            // so we can avoid the complexity of splitting the way for now (new towernodes would be required, splitting up geometry etc)
            LOGGER.warn("Bug in OSM or GraphHopper. Too big tower node distance " + towerNodeDistance + " reset to large value, osm way " + wayOsmId);
            towerNodeDistance = maxDistance;
        }

        EdgeIteratorState iter = graph.edge(fromIndex, toIndex).setDistance(towerNodeDistance).setFlags(flags);

        if (doSimplify && pointList.size() > 2)
            simplifyAlgo.simplify(pointList);

        // If the entire way is just the first and last point, do not waste space storing an empty way geometry
        if (pointList.size() > 2)
            iter.setWayGeometry(pointList.shallowCopy(1, pointList.size() - 1, false));

        storeOsmWayID(iter.getEdge(), wayOsmId);
        return iter;
    }

    /**
     * Stores only osmWayIds which are required for relations
     */
    protected void storeOsmWayID(int edgeId, long osmWayId) {
        if (getOsmWayIdSet().contains(osmWayId)) {
            getEdgeIdToOsmWayIdMap().put(edgeId, osmWayId);
        }
    }

    /**
     * @return converted tower node
     */
    private int handlePillarNode(int tmpNode, long osmId, PointList pointList, boolean convertToTowerNode) {
        tmpNode = tmpNode - 3;
        double lat = pillarInfo.getLatitude(tmpNode);
        double lon = pillarInfo.getLongitude(tmpNode);
        double ele = pillarInfo.getElevation(tmpNode);
        if (lat == Double.MAX_VALUE || lon == Double.MAX_VALUE)
            throw new RuntimeException("Conversion pillarNode to towerNode already happened!? "
                    + "osmId:" + osmId + " pillarIndex:" + tmpNode);

        if (convertToTowerNode) {
            // convert pillarNode type to towerNode, make pillar values invalid
            pillarInfo.setNode(tmpNode, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
            tmpNode = addTowerNode(osmId, lat, lon, ele);
        } else if (pointList.is3D())
            pointList.add(lat, lon, ele);
        else
            pointList.add(lat, lon);

        return tmpNode;
    }

    protected void finishedReading() {
        printInfo("way");
        pillarInfo.clear();
        encodingManager.releaseParsers();
        eleProvider.release();
        osmNodeIdToInternalNodeMap = null;
        osmNodeIdToNodeFlagsMap = null;
        osmWayIdToRouteWeightMap = null;
        osmWayIdSet = null;
        edgeIdToOsmWayIdMap = null;
    }

    /**
     * Create a copy of the barrier node
     */
    long addBarrierNode(long nodeId) {
        ReaderNode newNode;
        int graphIndex = getNodeMap().get(nodeId);
        if (graphIndex < TOWER_NODE) {
            graphIndex = -graphIndex - 3;
            newNode = new ReaderNode(createNewNodeId(), nodeAccess, graphIndex);
        } else {
            graphIndex = graphIndex - 3;
            newNode = new ReaderNode(createNewNodeId(), pillarInfo, graphIndex);
        }

        final long id = newNode.getId();
        prepareHighwayNode(id);
        addNode(newNode);
        return id;
    }

    private long createNewNodeId() {
        return newUniqueOsmId++;
    }

    /**
     * Add a zero length edge with reduced routing options to the graph.
     */
    Collection<EdgeIteratorState> addBarrierEdge(long fromId, long toId, IntsRef inEdgeFlags, long nodeFlags, long wayOsmId) {
        IntsRef edgeFlags = IntsRef.deepCopyOf(inEdgeFlags);
        // clear blocked directions from flags
        for (BooleanEncodedValue accessEnc : encodingManager.getAccessEncFromNodeFlags(nodeFlags)) {
            accessEnc.setBool(false, edgeFlags, false);
            accessEnc.setBool(true, edgeFlags, false);
        }
        // add edge
        barrierNodeIds.clear();
        barrierNodeIds.add(fromId);
        barrierNodeIds.add(toId);
        return addOSMWay(barrierNodeIds, edgeFlags, wayOsmId);
    }

    /**
     * Creates turn relations out of an unspecified OSM relation
     */
    List<OSMTurnRelation> createTurnRelations(ReaderRelation relation) {
        List<OSMTurnRelation> osmTurnRelations = new ArrayList<>();
        String vehicleTypeRestricted = "";
        List<String> vehicleTypesExcept = new ArrayList<>();
        if (relation.hasTag("except")) {
            String tagExcept = relation.getTag("except");
            if (!Helper.isEmpty(tagExcept)) {
                List<String> vehicleTypes = new ArrayList<>(Arrays.asList(tagExcept.split(";")));
                for (String vehicleType : vehicleTypes)
                    vehicleTypesExcept.add(vehicleType.trim());
            }
        }
        if (relation.hasTag("restriction")) {
            OSMTurnRelation osmTurnRelation = createTurnRelation(relation, relation.getTag("restriction"), vehicleTypeRestricted, vehicleTypesExcept);
            if (osmTurnRelation != null) {
                osmTurnRelations.add(osmTurnRelation);
            }
            return osmTurnRelations;
        }
        if (relation.hasTagWithKeyPrefix("restriction:")) {
            List<String> vehicleTypesRestricted = relation.getKeysWithPrefix("restriction:");
            for (String vehicleType : vehicleTypesRestricted) {
                String restrictionType = relation.getTag(vehicleType);
                vehicleTypeRestricted = vehicleType.replace("restriction:", "").trim();
                OSMTurnRelation osmTurnRelation = createTurnRelation(relation, restrictionType, vehicleTypeRestricted, vehicleTypesExcept);
                if (osmTurnRelation != null) {
                    osmTurnRelations.add(osmTurnRelation);
                }
            }
        }
        return osmTurnRelations;
    }

    OSMTurnRelation createTurnRelation(ReaderRelation relation, String restrictionType, String vehicleTypeRestricted, List<String> vehicleTypesExcept) {
        OSMTurnRelation.Type type = OSMTurnRelation.Type.getRestrictionType(restrictionType);
        if (type != OSMTurnRelation.Type.UNSUPPORTED) {
            long fromWayID = -1;
            long viaNodeID = -1;
            long toWayID = -1;

            for (ReaderRelation.Member member : relation.getMembers()) {
                if (ReaderElement.WAY == member.getType()) {
                    if ("from".equals(member.getRole())) {
                        fromWayID = member.getRef();
                    } else if ("to".equals(member.getRole())) {
                        toWayID = member.getRef();
                    }
                } else if (ReaderElement.NODE == member.getType() && "via".equals(member.getRole())) {
                    viaNodeID = member.getRef();
                }
            }
            if (fromWayID >= 0 && toWayID >= 0 && viaNodeID >= 0) {
                OSMTurnRelation osmTurnRelation = new OSMTurnRelation(fromWayID, viaNodeID, toWayID, type);
                osmTurnRelation.setVehicleTypeRestricted(vehicleTypeRestricted);
                osmTurnRelation.setVehicleTypesExcept(vehicleTypesExcept);
                return osmTurnRelation;
            }
        }
        return null;
    }

    /**
     * Filter method, override in subclass
     */
    boolean isInBounds(ReaderNode node) {
        return true;
    }

    /**
     * Maps OSM IDs (long) to internal node IDs (int)
     */
    protected LongIntMap getNodeMap() {
        return osmNodeIdToInternalNodeMap;
    }

    protected LongLongMap getNodeFlagsMap() {
        return osmNodeIdToNodeFlagsMap;
    }

    int getRelFlagsMapSize() {
        return osmWayIdToRouteWeightMap.size();
    }

    IntsRef getRelFlagsMap(long osmId) {
        long relFlagsAsLong = osmWayIdToRouteWeightMap.get(osmId);
        tempRelFlags.ints[0] = (int) relFlagsAsLong;
        tempRelFlags.ints[1] = (int) (relFlagsAsLong >> 32);
        return tempRelFlags;
    }

    void putRelFlagsMap(long osmId, IntsRef relFlags) {
        long relFlagsAsLong = ((long) relFlags.ints[1] << 32) | (relFlags.ints[0] & 0xFFFFFFFFL);
        osmWayIdToRouteWeightMap.put(osmId, relFlagsAsLong);
    }

    @Override
    public DataReader setWayPointMaxDistance(double maxDist) {
        doSimplify = maxDist > 0;
        simplifyAlgo.setMaxDistance(maxDist);
        return this;
    }

    @Override
    public DataReader setWayPointElevationMaxDistance(double elevationWayPointMaxDistance) {
        simplifyAlgo.setElevationMaxDistance(elevationWayPointMaxDistance);
        return this;
    }

    @Override
    public DataReader setSmoothElevation(boolean smoothElevation) {
        this.smoothElevation = smoothElevation;
        return this;
    }

    @Override
    public DataReader setLongEdgeSamplingDistance(double longEdgeSamplingDistance) {
        this.longEdgeSamplingDistance = longEdgeSamplingDistance;
        return this;
    }

    @Override
    public DataReader setWorkerThreads(int numOfWorkers) {
        this.workerThreads = numOfWorkers;
        return this;
    }

    @Override
    public DataReader setElevationProvider(ElevationProvider eleProvider) {
        if (eleProvider == null)
            throw new IllegalStateException("Use the NOOP elevation provider instead of null or don't call setElevationProvider");

        if (!nodeAccess.is3D() && ElevationProvider.NOOP != eleProvider)
            throw new IllegalStateException("Make sure you graph accepts 3D data");

        this.eleProvider = eleProvider;
        return this;
    }

    @Override
    public DataReader setFile(File osmFile) {
        this.osmFile = osmFile;
        return this;
    }

    private void printInfo(String str) {
        LOGGER.info("finished " + str + " processing." + " nodes: " + graph.getNodes()
                + ", osmIdMap.size:" + getNodeMap().getSize() + ", osmIdMap:" + getNodeMap().getMemoryUsage() + "MB"
                + ", nodeFlagsMap.size:" + getNodeFlagsMap().size() + ", relFlagsMap.size:" + getRelFlagsMapSize()
                + ", zeroCounter:" + zeroCounter
                + " " + Helper.getMemInfo());
    }

    @Override
    public Date getDataDate() {
        return osmDataDate;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
    
}