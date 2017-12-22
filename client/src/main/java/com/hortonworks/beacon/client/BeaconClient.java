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


import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;

/**
 * Abstract Client API to submit and manage Beacon resources.
 */
public interface BeaconClient {
    void submitCluster(String clusterName, String filePath) throws BeaconClientException;

    void submitAndScheduleReplicationPolicy(String policyName, String filePath) throws BeaconClientException;

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

    String getServiceStatus() throws BeaconClientException;

    String getServiceVersion() throws BeaconClientException;

    PolicyInstanceList listPolicyInstances(String policyName) throws BeaconClientException;

    void abortPolicyInstance(String policyName) throws BeaconClientException;

    void updateCluster(String clusterName, String updateDefinition) throws BeaconClientException;

    void rerunPolicyInstance(String policyName) throws BeaconClientException;
}
