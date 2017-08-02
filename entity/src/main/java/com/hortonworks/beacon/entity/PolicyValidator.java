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
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterPersistenceHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;
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
                clusterExists(entity.getSourceCluster());
            } else if (!FSUtils.isHCFS(new Path(entity.getTargetDataset()))) {
                clusterExists(entity.getTargetCluster());
            }
        } else {
            clusterExists(entity.getSourceCluster());
            clusterExists(entity.getTargetCluster());
            ClusterHelper.validateIfClustersPaired(entity.getSourceCluster(), entity.getTargetCluster());
        }
    }

    private static void clusterExists(String name) throws BeaconException {
        ClusterPersistenceHelper.getActiveCluster(name);
    }

    private static void validateScheduleDate(Date startTime, Date endTime) throws ValidationException {
        if (startTime != null && startTime.before(new Date())) {
            throw new ValidationException(MessageCode.ENTI_000002.name(), "Start", "current");
        }
        if (startTime != null && endTime != null && endTime.before(startTime)) {
            throw new ValidationException(MessageCode.ENTI_000002.name(), "End", "start");
        }
        if (startTime == null && endTime != null && endTime.before(new Date())) {
            throw new ValidationException(MessageCode.ENTI_000002.name(), "End", "current");
        }
    }
}
