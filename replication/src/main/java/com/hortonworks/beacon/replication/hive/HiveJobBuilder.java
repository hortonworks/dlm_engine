/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.HiveActionType;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Hive Replication JobBuilder .
 */

public class HiveJobBuilder extends JobBuilder {

    public List<ReplicationJobDetails> buildJob(ReplicationPolicy policy) throws BeaconException {

        return Arrays.asList(exportReplicationJob(policy),
                importReplicationJob(policy));
    }

    private ReplicationJobDetails exportReplicationJob(ReplicationPolicy policy) throws BeaconException {
        Properties hiveDRProperties = HivePolicyHelper.buildHiveReplicationProperties(policy,
                HiveActionType.EXPORT.name());
        HivePolicyHelper.validateHiveReplicationProperties(hiveDRProperties);

        String name = hiveDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.NAME.getName());
        String type = hiveDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.TYPE.getName());

        return new ReplicationJobDetails(type, name, type, hiveDRProperties);
    }

    private ReplicationJobDetails importReplicationJob(ReplicationPolicy policy) throws BeaconException {
        Properties hiveDRProperties = HivePolicyHelper.buildHiveReplicationProperties(policy,
                HiveActionType.IMPORT.name());
        HivePolicyHelper.validateHiveReplicationProperties(hiveDRProperties);

        String name = hiveDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.NAME.getName());
        String type = hiveDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.TYPE.getName());

        return new ReplicationJobDetails(type, name, type, hiveDRProperties);
    }
}
