/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;

/**
 * Sync policy status admin job.
 */
public class SyncStatusJob implements AdminJob {

    private static final Logger LOG = LoggerFactory.getLogger(SyncStatusJob.class);

    private String endPoint;
    private String policy;
    private String status;

    public SyncStatusJob(String endPoint, String policy, String status) {
        this.endPoint = endPoint;
        this.policy = policy;
        this.status = status;
    }

    @Override
    public String getName() {
        return policy;
    }

    @Override
    public String getGroup() {
        return AdminJob.POLICY_STATUS;
    }

    @Override
    public void perform() throws BeaconClientException {
        LOG.info("Sync status admin job is executing policy: [{}], status: [{}].", policy, status);
        BeaconWebClient beaconClient = new BeaconWebClient(endPoint);
        beaconClient.syncPolicyStatus(policy, status, true);
    }
}
