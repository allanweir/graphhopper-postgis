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

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modified version of GraphHopper to optimize working with Postgis
 *
 * @author Phil
 * @author Robin Boldt
 */
public class GraphHopperPostgis extends GraphHopperOSM {
    
    private final Map<String, String> postgisParams = new HashMap<>();
    
    public GraphHopperPostgis(GraphHopperConfig configuration, JsonFeatureCollection landmarkSplittingFeatureCollection) {
        super.init(configuration);
        
        super.setDataReaderFile(configuration.getString("postgis.table", ""));
              
        Integer port = configuration.getInt("postgis.port", 0);
        postgisParams.put("dbtype", "postgis");
        postgisParams.put("host", configuration.getString("postgis.host", ""));
        postgisParams.put("port", port.toString());
        postgisParams.put("schema", configuration.getString("postgis.schema", ""));
        postgisParams.put("database", configuration.getString("postgis.database", ""));
        postgisParams.put("user", configuration.getString("postgis.user", ""));
        postgisParams.put("passwd", configuration.getString("postgis.password", ""));
        postgisParams.put("tags_to_copy", configuration.getString("postgis.tags_to_copy", ""));
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
