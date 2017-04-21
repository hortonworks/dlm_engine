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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

/**
 * Helper util class for Beacon ReplicationPolicy resource.
 */
public final class PolicyHelper {
    private PolicyHelper() {
    }

    public static String getRemoteBeaconEndpoint(final ReplicationPolicy policy) throws BeaconException {

        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException("No remote beacon endpoint for HCFS policy:" + policy.getName());
        }
        String remoteClusterName = getRemoteClusterName(policy);
        Cluster remoteCluster = EntityHelper.getEntity(EntityType.CLUSTER, remoteClusterName);
        return remoteCluster.getBeaconEndpoint();
    }

    public static String getRemoteClusterName(final ReplicationPolicy policy) throws BeaconException {
        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();
        return localClusterName.equalsIgnoreCase(policy.getSourceCluster())
                ? policy.getTargetCluster() : policy.getSourceCluster();
    }

    public static boolean isPolicyHCFS(final String sourceDataset, final String targetDataset) throws BeaconException {
        if (StringUtils.isNotBlank(sourceDataset)) {
            Path sourceDatasetPath = new Path(sourceDataset);
            if (FSUtils.isHCFS(sourceDatasetPath)) {
                return true;
            }
        }

        if (StringUtils.isNotBlank(targetDataset)) {
            Path targetDatasetPath = new Path(targetDataset);
            if (FSUtils.isHCFS(targetDatasetPath)) {
                return true;
            }
        }

        return false;
    }
}
