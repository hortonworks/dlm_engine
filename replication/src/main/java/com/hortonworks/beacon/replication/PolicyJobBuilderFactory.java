/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.fs.FSJobBuilder;
import com.hortonworks.beacon.replication.hive.HiveJobBuilder;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Class to provide the Replication JobBuilder class.
 */
public final class PolicyJobBuilderFactory {

    private PolicyJobBuilderFactory() {}

    public static JobBuilder getJobBuilder(ReplicationPolicy policy) {
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                return new FSJobBuilder();
            case HIVE:
                return new HiveJobBuilder();
            default:
                throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_010011.name(), policy.getType()));
        }
    }
}
