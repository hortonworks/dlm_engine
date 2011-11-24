/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon;

import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.commons.lang3.StringUtils;

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
        SOURCE_DATA_CENTER(1),
        SOURCE_CLUSTER(2),
        TARGET_DATA_CENTER(3),
        TARGET_CLUSTER(4),
        POLICY_NAME(5),
        SERVER_INDEX(6),
        SERVER_START_TIME(7),
        COUNTER(8);

        private int index;

        PolicyIdField(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public static synchronized String generatePolicyId(String sourceCluster, String targetCluster,
                                                       String policyName, int serverIndex) {
        StringBuilder policyId = new StringBuilder(SEPARATOR);
        appendClusterInfo(sourceCluster, policyId);
        appendClusterInfo(targetCluster, policyId);
        policyId.append(policyName).append(SEPARATOR);
        policyId.append(serverIndex).append(SEPARATOR);
        policyId.append(BeaconConstants.SERVER_START_TIME).append(SEPARATOR);
        policyId.append(PADDING.substring(String.valueOf(counter).length())).append(counter);
        counter++;
        return policyId.toString();
    }

    private static void appendClusterInfo(String clusterName, StringBuilder policyId) {
        if (StringUtils.isBlank(clusterName)) {
            return;
        }
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
    }

    public static String getPolicyIdField(String policyId, PolicyIdField policyIdField) {
        String[] idParts = policyId.split(SEPARATOR);
        return idParts[policyIdField.getIndex()];
    }
}
