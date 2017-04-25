/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.scheduler.internal;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.resource.APIResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return policy.concat("#").concat(status);
    }

    @Override
    public String getGroup() {
        return AdminJob.ADMIN_STATUS;
    }

    @Override
    public boolean perform() {
        LOG.info("Sync status admin job is executing policy: [{}], status: [{}].", policy, status);
        BeaconClient beaconClient = new BeaconClient(endPoint);
        APIResult apiResult = beaconClient.syncPolicyStatus(policy, status, true);
        return apiResult.getStatus() == APIResult.Status.SUCCEEDED;
    }
}
