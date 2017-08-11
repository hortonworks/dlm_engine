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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

/**
 * Sync policy status admin job.
 */
public class SyncStatusJob implements AdminJob {

    private static final BeaconLog LOG = BeaconLog.getLog(SyncStatusJob.class);

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
    public boolean perform() {
        LOG.info(MessageCode.SCHD_000026.name(), policy, status);
        BeaconClient beaconClient = new BeaconClient(endPoint);
        APIResult apiResult = beaconClient.syncPolicyStatus(policy, status, true);
        return apiResult.getStatus() == APIResult.Status.SUCCEEDED;
    }
}
