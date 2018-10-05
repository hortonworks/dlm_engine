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

package com.hortonworks.dlmengine.fs;

import com.hortonworks.beacon.ExecutionType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.dlmengine.BeaconReplicationPolicy;
import com.hortonworks.dlmengine.fs.hdfs.HDFSDataSet;

/**
 * HDFS on-prem to on-prem replication.
 */
public class HDFSReplication extends BeaconReplicationPolicy<HDFSDataSet, HDFSDataSet> {

    public HDFSReplication(ReplicationPolicy policyRequest) throws BeaconException {
        super(policyRequest,
                (HDFSDataSet) FSDataSet.create(policyRequest.getSourceDataset(), policyRequest.getSourceCluster()),
                (HDFSDataSet) FSDataSet.create(policyRequest.getTargetDataset(), policyRequest.getTargetCluster()));
    }

    @Override
    public ExecutionType getExecutionTypeEnum() throws BeaconException {
        return getSourceDatasetV2().isSnapshottable() ? ExecutionType.FS_SNAPSHOT : ExecutionType.FS;
    }

    @Override
    protected Cluster getSchedulableCluster() {
        return getTargetDatasetV2().getCluster();
    }

    @Override
    public void validatePairing() {
        ClusterHelper.areClustersPaired(getSourceDatasetV2().getCluster(), getTargetCluster());
    }
}

