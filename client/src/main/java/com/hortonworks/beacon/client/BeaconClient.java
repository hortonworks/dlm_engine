/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client;


import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.StatusResult;

/**
 * Abstract Client API to submit and manage Beacon resources.
 */
public interface BeaconClient {
    APIResult submitCluster(String clusterName, String filePath);

    APIResult submitReplicationPolicy(String policyName, String filePath);

    APIResult scheduleReplicationPolicy(String policyName);

    APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath);

    ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                               Integer offset, Integer numResults);

    PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder,
                             Integer offset, Integer numResults);

    StatusResult getClusterStatus(String clusterName);

    StatusResult getPolicyStatus(String policyName);

    String getCluster(String clusterName);

    String getPolicy(String policyName);

    APIResult deleteCluster(String clusterName);

    APIResult deletePolicy(String policyName,
            boolean isInternalSyncDelete);

    APIResult suspendPolicy(String policyName);

    APIResult resumePolicy(String policyName);

    APIResult pairClusters(String remoteClusterName,
            boolean isInternalPairing);

    APIResult unpairClusters(String remoteClusterName,
            boolean isInternalunpairing);

    APIResult syncPolicy(String policyName, String policyDefinition);

    APIResult syncPolicyStatus(String policyName, String status,
            boolean isInternalStatusSync);

    String getStatus();

    String getVersion();

    PolicyInstanceList listPolicyInstances(String policyName);

    APIResult abortPolicyInstance(String policyName);

    APIResult updateCluster(String clusterName, String updateDefinition);

    APIResult rerunPolicyInstance(String policyName);
}
