/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
