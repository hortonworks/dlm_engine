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

    void updateCluster(String clusterName, String filePath) throws BeaconClientException;

    void rerunPolicyInstance(String policyName) throws BeaconClientException;

    String getPolicyLogs(String policyName) throws BeaconClientException;

    String getPolicyLogsForId(String policId) throws BeaconClientException;

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
