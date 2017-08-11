/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * FileSystem Replication JobBuilder.
 */

public class FSJobBuilder extends JobBuilder {

    public List<ReplicationJobDetails> buildJob(ReplicationPolicy policy) throws BeaconException {
        // Add any default plugin relates jobs first


        Properties fsDRProperties = FSPolicyHelper.buildFSReplicationProperties(policy);
        FSPolicyHelper.validateFSReplicationProperties(fsDRProperties);

        String name = fsDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.NAME.getName());
        String type = fsDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.TYPE.getName());
        return Arrays.asList(new ReplicationJobDetails(type, name, type, fsDRProperties));
    }
}
