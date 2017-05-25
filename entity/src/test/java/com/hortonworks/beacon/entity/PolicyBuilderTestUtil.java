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
