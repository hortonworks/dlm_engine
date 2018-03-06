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


import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;

/**
 * Abstract Client API to submit and manage Beacon resources.
 */
public interface BeaconClient {
    void submitCluster(String clusterName, String filePath) throws BeaconClientException;

    void submitAndScheduleReplicationPolicy(String policyName, String filePath) throws BeaconClientException;

    void dryrunPolicy(String policyName, String filePath) throws BeaconClientException;

    ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                               Integer offset, Integer numResults) throws BeaconClientException;

    PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder,
                             Integer offset, Integer numResults) throws BeaconClientException;

    Entity.EntityStatus getClusterStatus(String clusterName) throws BeaconClientException;

    Entity.EntityStatus getPolicyStatus(String policyName) throws BeaconClientException;

    Cluster getCluster(String clusterName) throws BeaconClientException;

    ReplicationPolicy getPolicy(String policyName) throws BeaconClientException;

    void deleteCluster(String clusterName) throws BeaconClientException;

    void deletePolicy(String policyName,
            boolean isInternalSyncDelete) throws BeaconClientException;

    void suspendPolicy(String policyName) throws BeaconClientException;

    void resumePolicy(String policyName) throws BeaconClientException;

    void pairClusters(String remoteClusterName,
            boolean isInternalPairing) throws BeaconClientException;

    void unpairClusters(String remoteClusterName,
            boolean isInternalunpairing) throws BeaconClientException;

    void syncPolicy(String policyName, String policyDefinition) throws BeaconClientException;

    void syncPolicyStatus(String policyName, String status,
            boolean isInternalStatusSync) throws BeaconClientException;

    ServerStatusResult getServiceStatus() throws BeaconClientException;

    ServerVersionResult getServiceVersion() throws BeaconClientException;

    PolicyInstanceList listPolicyInstances(String policyName) throws BeaconClientException;

    void abortPolicyInstance(String policyName) throws BeaconClientException;

    void updateCluster(String clusterName, String updateDefinition) throws BeaconClientException;

    void rerunPolicyInstance(String policyName) throws BeaconClientException;

    String submitCloudCred(CloudCred cloudCred) throws BeaconClientException;

    void updateCloudCred(String cloudCredId, CloudCred cloudCred) throws BeaconClientException;

    void deleteCloudCred(String cloudCredId) throws BeaconClientException;

    CloudCred getCloudCred(String cloudCredId) throws BeaconClientException;

    void validateCloudPath(String cloudCredId, String path) throws BeaconClientException;

    CloudCredList listCloudCred(String filterBy, String orderBy, String sortOrder,
                                Integer offset, Integer resultsPerPage) throws BeaconClientException;

    FileListResult listFiles(String path) throws BeaconClientException;

    FileListResult listFiles(String path, String cloudCredId) throws BeaconClientException;

    DBListResult listDBs() throws BeaconClientException;

    UserPrivilegesResult getUserPrivileges() throws BeaconClientException;
}
