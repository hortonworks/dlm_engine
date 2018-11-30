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

package com.hortonworks.dlmengine.hive;

import com.hortonworks.beacon.ExecutionType;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.dlmengine.BeaconReplicationPolicy;
import com.hortonworks.dlmengine.fs.FSDataSet;
import com.hortonworks.dlmengine.fs.HCFSDataset;

/**
 * Hive replication - on-prem or cloud.
 */
public class HiveReplication extends BeaconReplicationPolicy<HiveDBDataSet, HiveDBDataSet> {
    public HiveReplication(ReplicationPolicy policyRequest) throws BeaconException {
        super(policyRequest,
                new HiveDBDataSet(policyRequest.getSourceDataset(), policyRequest.getSourceCluster(), policyRequest),
                new HiveDBDataSet(policyRequest.getTargetDataset(), policyRequest.getTargetCluster(), policyRequest));
    }

    @Override
    public ExecutionType getExecutionTypeEnum() {
        return ExecutionType.HIVE;
    }

    @Override
    protected Cluster getSchedulableCluster() {
        FSDataSet warehouse = getTargetDatasetV2().getWarehouseDataset();
        if (warehouse instanceof HCFSDataset) {
            return getSourceDatasetV2().getCluster();
        } else {
            return getTargetDatasetV2().getCluster();
        }
    }

    @Override
    public void validatePairing() {
        ClusterHelper.areClustersPaired(getSourceDatasetV2().getCluster(), getTargetCluster());
    }

    @Override
    protected void validateClusters() throws BeaconException {
        clusterExists(this.getSourceCluster());
        clusterExists(this.getTargetCluster());
    }

    /**
     * Validate if policy is compatible to be ran in the current setup.
     *
     * Restricted:
     *  HDP 3.0 to any target
     *  HDP 2.6.5 to HDP 3.0 Cloud
     * @throws BeaconException
     */
    @Override
    public void validateClusterCompatibility() throws BeaconException {
        try {
            BeaconClient sourceClient = getSourceDatasetV2().getCluster().getBeaconClient();
            BeaconClient targetClient = getTargetDatasetV2().getCluster().getBeaconClient();
            String sourceHDPVersion = sourceClient.getServiceStatus().getHdpVersion();
            String targetHDPVersion = targetClient.getServiceStatus().getHdpVersion();
            if (this.getTargetDatasetV2().isHCFSDataset() && targetHDPVersion.startsWith("3")) {
                throw new BeaconException("Hive Cloud Replication from on prem to HDP 3 cluster isn't supported yet!");
            } else if (sourceHDPVersion.startsWith("3")) {
                throw new BeaconException("Hive Replication from HDP 3 cluster isn't supported yet!");
            }
        } catch (BeaconClientException e) {
            throw new BeaconException(e);
        }
    }
}

