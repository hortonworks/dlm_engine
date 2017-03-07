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

package com.hortonworks.beacon;

/**
 * Beacon unique id generator implementation.
 */
public final class BeaconIDGenerator {

    private static final String PADDING = "000000000";
    static final String SEPARATOR = "-";
    private static final long SERVER_START_TIME = System.currentTimeMillis();
    private static volatile int counter = 1;

    private BeaconIDGenerator() {
    }

    public static synchronized String getPolicyId(String dataCenter, String clusterName, int serverIndex) {
        StringBuilder policyId = new StringBuilder();
        policyId.append(dataCenter).append(SEPARATOR);
        policyId.append(clusterName).append(SEPARATOR);
        policyId.append(serverIndex).append(SEPARATOR);
        policyId.append(SERVER_START_TIME).append(SEPARATOR);
        policyId.append(PADDING.substring(String.valueOf(counter).length())).append(counter);
        counter++;
        return policyId.toString();
    }
}
