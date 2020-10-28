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
import java.util.HashMap;
import java.util.Map;

/**
 * @author Phil
 * @author Robin Boldt
 */
public class Utils {

    public static RuntimeException asUnchecked(Throwable e) {
        if (RuntimeException.class.isInstance(e)) {
            return (RuntimeException) e;
        }
        return new RuntimeException(e);
    }
    
    public static long[] LongsToPrimitive(Long[] list) {
        // this way you create array of long
        long[] arr2 = new long[list.length];
        int i = 0;
        for (Long e : list) {
            arr2[i++] = e; // autoboxing does the job here
        }
        return arr2;
    }
    
    public static Map<String, Object> postGisParamsFromConfig(GraphHopperConfig configuration) {
        Map<String, Object> postgisParams = new HashMap<>();
        Integer port = configuration.getInt("postgis.port", 0);
        postgisParams.put("dbtype", "postgis");
        postgisParams.put("host", configuration.getString("postgis.host", ""));
        postgisParams.put("port", port.toString());
        postgisParams.put("schema", configuration.getString("postgis.schema", ""));
        postgisParams.put("database", configuration.getString("postgis.database", ""));
        postgisParams.put("user", configuration.getString("postgis.user", ""));
        postgisParams.put("passwd", configuration.getString("postgis.password", ""));
        postgisParams.put("tags_to_copy", configuration.getString("postgis.tags_to_copy", ""));
        return postgisParams;
    }

}