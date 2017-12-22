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
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;

/**
 * Sync policy deletion event into remote beacon server.
 */
public class SyncPolicyDeleteJob implements AdminJob {

    private String endPoint;
    private String policy;


    public SyncPolicyDeleteJob(String endPoint, String policy) {
        this.endPoint = endPoint;
        this.policy = policy;
    }

    @Override
    public void perform() throws BeaconClientException {
        BeaconClient client = new BeaconWebClient(endPoint);
        client.deletePolicy(policy, true);
    }

    @Override
    public String getName() {
        return policy;
    }

    @Override
    public String getGroup() {
        return POLICY_DELETE;
    }
}
