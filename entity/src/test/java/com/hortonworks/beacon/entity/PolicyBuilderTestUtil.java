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
package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.util.PropertiesIgnoreCase;
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
