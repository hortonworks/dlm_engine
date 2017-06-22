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

import com.hortonworks.beacon.constants.BeaconConstants;

/**
 * Beacon unique id generator implementation.
 */
public final class BeaconIDGenerator {

    private static final String PADDING = "000000000";
    private static final String SEPARATOR = "/";
    private static volatile int counter = 1;

    private BeaconIDGenerator() {
    }

    /**
     * Enum for different parts of the policy ID.
     */
    public enum PolicyIdField {
        DATA_CENTER(1),
        CLUSTER(2),
        POLICY_NAME(3),
        SERVER_INDEX(4),
        SERVER_START_TIME(5),
        COUNTER(6);

        private int index;

        PolicyIdField(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public static synchronized String generatePolicyId(String clusterName,
                                                       String policyName, int serverIndex) {
        StringBuilder policyId = new StringBuilder(SEPARATOR);
        String[] pair = clusterName.split(BeaconConstants.CLUSTER_NAME_SEPARATOR_REGEX, 2);
        String dataCenter;
        if (pair.length == 2) {
            dataCenter = pair[0];
            clusterName = pair[1];
        } else {
            dataCenter = clusterName;
        }
        policyId.append(dataCenter).append(SEPARATOR);
        policyId.append(clusterName).append(SEPARATOR);
        policyId.append(policyName).append(SEPARATOR);
        policyId.append(serverIndex).append(SEPARATOR);
        policyId.append(BeaconConstants.SERVER_START_TIME).append(SEPARATOR);
        policyId.append(PADDING.substring(String.valueOf(counter).length())).append(counter);
        counter++;
        return policyId.toString();
    }

    public static String getPolicyIdField(String policyId, PolicyIdField policyIdField) {
        String[] idParts = policyId.split(SEPARATOR);
        return idParts[policyIdField.getIndex()];
    }
}
