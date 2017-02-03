/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyList;

public abstract class AbstractBeaconClient {
    public abstract APIResult submitCluster(String clusterName, String filePath);

    public abstract APIResult submitReplicationPolicy(String policyName, String filePath);

    public abstract APIResult scheduleReplicationPolicy(String policyName);

    public abstract APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath);

    public abstract ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                               Integer offset, Integer numResults);

    public abstract PolicyList getPolicyList(String fields, String orderBy, String sortOrder,
                                             Integer offset, Integer numResults);

    public abstract APIResult getClusterStatus(String clusterName);

    public abstract APIResult getPolicyStatus(String policyName);

    public abstract String getCluster(String clusterName);

    public abstract String getPolicy(String policyName);

    public abstract APIResult deleteCluster(String clusterName);

    public abstract APIResult deletePolicy(String policyName,
                                           boolean isInternalSyncDelete);

    public abstract APIResult suspendPolicy(String policyName);

    public abstract APIResult resumePolicy(String policyName);

    public abstract APIResult pairClusters(String remoteBeaconEndpoint,
                                           String remoteClusterName,
                                           boolean isInternalPairing);

    public abstract APIResult unpairClusters(String remoteBeaconEndpoint,
                                             String remoteClusterName,
                                             boolean isInternalunpairing);

    public abstract APIResult syncPolicy(String policyName, String policyDefinition);

    public abstract APIResult syncPolicyStatus(String policyName, String status,
                                               boolean isInternalStatusSync);
}
