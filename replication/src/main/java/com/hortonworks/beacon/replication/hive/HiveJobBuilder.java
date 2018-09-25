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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.HiveActionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Hive Replication JobBuilder .
 */

public class HiveJobBuilder extends JobBuilder {

    public List<ReplicationJobDetails> buildJob(ReplicationPolicy policy) throws BeaconException {
        List<ReplicationJobDetails> replicationJobDetailsList = new ArrayList<>();
        ReplicationJobDetails exportJobDetails = exportReplicationJob(policy);
        ReplicationJobDetails importJobDetails = importReplicationJob(policy);
        replicationJobDetailsList.add(exportJobDetails);
        replicationJobDetailsList.add(importJobDetails);
        boolean bootstrap = isBootstrapRun(policy);
        if (bootstrap) {
            replicationJobDetailsList.add(exportJobDetails);
            replicationJobDetailsList.add(importJobDetails);
        }
        return replicationJobDetailsList;
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

    private boolean isBootstrapRun(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        HiveServerClient hiveServerClient = null;
        try {
            hiveServerClient = HiveClientFactory.getHiveServerClient(cluster.getHsEndpoint(),
                    targetCluster);
            long replId = hiveServerClient.getReplicatedEventId(policy.getTargetDataset());
            return replId <= 0;
        } finally {
            HiveClientFactory.close(hiveServerClient);
        }
    }
}
