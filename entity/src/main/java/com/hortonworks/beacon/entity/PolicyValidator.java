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
