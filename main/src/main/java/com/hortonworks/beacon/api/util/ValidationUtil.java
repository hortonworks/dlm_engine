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


package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.store.ConfigurationStoreService;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

import javax.ws.rs.core.Response;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final String ERROR_MESSAGE_PART1 = "This operation is not allowed on source cluster: ";
    private static final String ERROR_MESSAGE_PART2 = ".Try it on target cluster: ";

    private ValidationUtil() {
    }

    public static void validateIfAPIRequestAllowed(String replicationPolicyName)
            throws BeaconException {

        ReplicationPolicy policy = ((ConfigurationStoreService) Services.get()
                .getService(ConfigurationStoreService.SERVICE_NAME)).getEntity(
                EntityType.REPLICATIONPOLICY, replicationPolicyName);
        if (policy == null) {
            throw BeaconWebException.newAPIException(replicationPolicyName
                    + " (" + EntityType.REPLICATIONPOLICY.name() + ") not " + "found", Response.Status.NOT_FOUND);
        }

        isRequestAllowed(policy);
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy)
            throws BeaconException {
        if (policy == null) {
            throw new BeaconException("Policy cannot be null");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();

        // If policy is HCFS then requests are allowed on source cluster
        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            String message = ERROR_MESSAGE_PART1 + sourceClusterName + ERROR_MESSAGE_PART2 + targetClusterName;
            throw BeaconWebException.newAPIException(message);
        }
    }

    public static void validatePolicy(final ReplicationPolicy policy) throws BeaconException {
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                break;
            default:
                throw new IllegalArgumentException("Invalid policy (Job) type :" + policy.getType());
        }
    }
}
