/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.log;

import java.util.ArrayList;
import java.util.List;

/**
 * Supported Parameter for Beacon Log.
 */
public enum BeaconLogParams {
    USER("USER"),
    CLUSTER("CLUSTER"),
    POLICYNAME("POLICYNAME"),
    POLICYID("POLICYID"),
    INSTANCEID("INSTANCEID");

    private String name;

    private static final List<String> BEACON_LOG_PARAMS_LIST = new ArrayList<>();

    static {
        for (BeaconLogParams params : BeaconLogParams.values()) {
            BEACON_LOG_PARAMS_LIST.add(params.name());
        }
    }

    BeaconLogParams(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean checkParams(String name) {
        return BEACON_LOG_PARAMS_LIST.contains(name);
    }

    public static int size() {
        return BEACON_LOG_PARAMS_LIST.size();
    }

    public static String getParam(int i) {
        return BEACON_LOG_PARAMS_LIST.get(i);
    }

    public static void clearParams() {
        BEACON_LOG_PARAMS_LIST.clear();
    }
}
