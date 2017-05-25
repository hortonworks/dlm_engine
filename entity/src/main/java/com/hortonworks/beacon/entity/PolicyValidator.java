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

import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.store.ConfigurationStoreService;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.fs.Path;

import java.util.Date;

/**
 * Validation helper function to validate Beacon ReplicationPolicy definition.
 */
public class PolicyValidator extends EntityValidator<ReplicationPolicy> {

    public PolicyValidator() {
        super(EntityType.REPLICATIONPOLICY);
    }

    @Override
    public void validate(ReplicationPolicy entity) throws BeaconException {
        validateScheduleDate(entity.getStartTime(), entity.getEndTime());
        if (PolicyHelper.isPolicyHCFS(entity.getSourceDataset(), entity.getTargetDataset())) {
            // Check which cluster is Non HCFS and validate it exists and no pairing required
            if (!FSUtils.isHCFS(new Path(entity.getSourceDataset()))) {
                validateEntityExists(EntityType.CLUSTER, entity.getSourceCluster());
            } else if (!FSUtils.isHCFS(new Path(entity.getTargetDataset()))) {
                validateEntityExists(EntityType.CLUSTER, entity.getTargetCluster());
            }
        } else {
            validateEntityExists(EntityType.CLUSTER, entity.getSourceCluster());
            validateEntityExists(EntityType.CLUSTER, entity.getTargetCluster());

            validateIfClustersPaired(EntityType.CLUSTER, entity.getSourceCluster(), entity.getTargetCluster());
        }
    }

    private static void validateEntityExists(EntityType type, String name) throws BeaconException {
        ConfigurationStoreService configStore = Services.get().getService(ConfigurationStoreService.SERVICE_NAME);
        if (configStore.getEntity(type, name) == null) {
            throw new ValidationException("Referenced " + type + " " + name + " is not registered. Source and target "
                    + "clusters in the policy should be paired before submitting or scheduling the policy");
        }
    }

    private static void validateIfClustersPaired(EntityType type, String sourceClluster,
                                                 String targetCluster) throws BeaconException {
        boolean paired = false;
        String[] peers = ClusterHelper.getPeers(sourceClluster);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(targetCluster)) {
                    paired = true;
                }
            }
        }

        peers = ClusterHelper.getPeers(targetCluster);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(sourceClluster)) {
                    paired = true;
                }
            }
        }

        if (!paired) {
            throw new ValidationException("Clusters " + sourceClluster + " and " + targetCluster + " are not paired. "
                    + "Pair the clusters before submitting or scheduling the policy");
        }
    }

    private static void validateScheduleDate(Date startTime, Date endTime) throws ValidationException {
        if (startTime != null && startTime.before(new Date())) {
            throw new ValidationException("Start time cannot be earlier than current time.");
        }
        if (startTime != null && endTime != null && endTime.before(startTime)) {
            throw new ValidationException("End time cannot be earlier than start time.");
        }
    }
}
