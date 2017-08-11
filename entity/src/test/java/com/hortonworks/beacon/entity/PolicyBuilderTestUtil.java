/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;

/**
 * Test util class to construct replication policy.
 */
public final class PolicyBuilderTestUtil {
    public static final String LOCAL_CLUSTER = "primaryCluster";

    private PolicyBuilderTestUtil() {
    }

    public static PropertiesIgnoreCase buildPolicyProps(String name, String sourceDataset,
                                                         String targetDataset, String targetCluster)
            throws BeaconException {
        return buildPolicyProps(name, sourceDataset, targetDataset, targetCluster, null, null);
    }

    public static PropertiesIgnoreCase buildPolicyProps(String name, String sourceDataset,
                                                        String targetDataset, String targetCluster,
                                                        String startTime, String endTime) {
        PropertiesIgnoreCase policyProps = new PropertiesIgnoreCase();
        policyProps.setProperty("name", name);
        policyProps.setProperty("type", "FS");
        if (StringUtils.isNotBlank(sourceDataset)) {
            policyProps.setProperty("sourceDataset", sourceDataset);
        }
        if (StringUtils.isNotBlank(targetDataset)) {
            policyProps.setProperty("targetDataset", targetDataset);
        }
        policyProps.setProperty("sourceCluster", LOCAL_CLUSTER);
        if (StringUtils.isNotBlank(targetCluster)) {
            policyProps.setProperty("targetCluster", targetCluster);
        }
        policyProps.setProperty("frequencyInSec", "120");
        if (StringUtils.isNotBlank(startTime)) {
            policyProps.setProperty("startTime", startTime);
        }
        if (StringUtils.isNotBlank(endTime)) {
            policyProps.setProperty("endTime", endTime);
        }
        return policyProps;
    }
}
