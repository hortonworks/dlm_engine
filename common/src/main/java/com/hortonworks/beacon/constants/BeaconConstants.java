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

package com.hortonworks.beacon.constants;

/**
 * Beacon constants.
 */
public final class BeaconConstants {

    private BeaconConstants() {
        // Disable construction.
    }

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    public static final int MAX_YEAR = 9999;
    public static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    public static final int MAX_DAY = 31;
    public static final long SERVER_START_TIME = System.currentTimeMillis();
    public static final String COLON_SEPARATOR = ":";
    public static final String COMMA_SEPARATOR = ",";
    public static final String CLUSTER_NAME_SEPARATOR_REGEX = "\\$";
}
