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

import com.graphhopper.GraphHopperConfig;
import com.graphhopper.json.geo.JsonFeatureCollection;
import com.graphhopper.reader.DataReader;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.storage.GraphHopperStorage;

import java.util.Map;

/**
 * Modified version of GraphHopper to optimize working with Postgis
 *
 * @author Phil
 * @author Robin Boldt
 */
public class GraphHopperPostgis extends GraphHopperOSM {
    
    private final Map<String, String> postgisParams;
    
    public GraphHopperPostgis(GraphHopperConfig configuration, JsonFeatureCollection landmarkSplittingFeatureCollection) {
        super.init(configuration);
        
        super.setDataReaderFile(configuration.getString("postgis.table", ""));
              
        this.postgisParams = Utils.postGisParamsFromConfig(configuration);
    }

    @Override
    protected DataReader createReader(GraphHopperStorage ghStorage) {
        OSMPostgisReader reader = new OSMPostgisReader(ghStorage, postgisParams);
        return initDataReader(reader);
    }   
        
//    @Override
//    protected DataReader importData() throws IOException {
//        DataReader reader = createReader(ghStorage);
//        logger.info("using " + ghStorage.toString() + ", memory:" + getMemInfo());
//        reader.readGraph();
//        return reader;
//    }

}
