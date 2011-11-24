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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
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
 * Local beacon client that calls resource's methods in the same JVM.
 */
public class LocalBeaconClient implements BeaconClient {
    @Override
    public void submitCluster(String clusterName, String filePath) throws BeaconClientException {

    }

    @Override
    public void submitAndScheduleReplicationPolicy(String policyName, String filePath) throws BeaconClientException {

    }

    @Override
    public void dryrunPolicy(String policyName, String filePath) throws BeaconClientException {

    }

    @Override
    public ClusterList getClusterList(String fields, String orderBy, String sortOrder, Integer offset,
                                      Integer numResults) throws BeaconClientException {
        return null;
    }

    @Override
    public PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder, Integer offset,
                                    Integer numResults) throws BeaconClientException {
        return null;
    }

    @Override
    public Entity.EntityStatus getClusterStatus(String clusterName) throws BeaconClientException {
        return null;
    }

    @Override
    public Entity.EntityStatus getPolicyStatus(String policyName) throws BeaconClientException {
        return null;
    }

    @Override
    public Cluster getCluster(String clusterName) throws BeaconClientException {
        return null;
    }

    @Override
    public ReplicationPolicy getPolicy(String policyName) throws BeaconClientException {
        return null;
    }

    @Override
    public void deleteCluster(String clusterName) throws BeaconClientException {

    }

    @Override
    public void deletePolicy(String policyName, boolean isInternalSyncDelete) throws BeaconClientException {

    }

    @Override
    public void suspendPolicy(String policyName) throws BeaconClientException {

    }

    @Override
    public void resumePolicy(String policyName) throws BeaconClientException {

    }

    @Override
    public void pairClusters(String remoteClusterName, boolean isInternalPairing) throws BeaconClientException {

    }

    @Override
    public void unpairClusters(String remoteClusterName, boolean isInternalunpairing) throws BeaconClientException {

    }

    @Override
    public void syncPolicy(String policyName, String policyDefinition) throws BeaconClientException {

    }

    @Override
    public void syncPolicyStatus(String policyName, String status, boolean isInternalStatusSync)
            throws BeaconClientException {

    }

    @Override
    public ServerStatusResult getServiceStatus() throws BeaconClientException {
        return null;
    }

    @Override
    public ServerVersionResult getServiceVersion() throws BeaconClientException {
        return null;
    }

    @Override
    public PolicyInstanceList listPolicyInstances(String policyName) throws BeaconClientException {
        return null;
    }

    @Override
    public void abortPolicyInstance(String policyName) throws BeaconClientException {

    }

    @Override
    public void updateCluster(String clusterName, String updateDefinition) throws BeaconClientException {

    }

    @Override
    public void rerunPolicyInstance(String policyName) throws BeaconClientException {

    }

    @Override
    public String getPolicyLogs(String policyName) throws BeaconClientException {
        return null;
    }

    @Override
    public String getPolicyLogsForId(String policId) throws BeaconClientException {
        return null;
    }

    @Override
    public String submitCloudCred(CloudCred cloudCred) throws BeaconClientException {
        return null;
    }

    @Override
    public void updateCloudCred(String cloudCredId, CloudCred cloudCred) throws BeaconClientException {

    }

    @Override
    public void deleteCloudCred(String cloudCredId) throws BeaconClientException {

    }

    @Override
    public CloudCred getCloudCred(String cloudCredId) throws BeaconClientException {
        return null;
    }

    @Override
    public void validateCloudPath(String cloudCredId, String path) throws BeaconClientException {

    }

    @Override
    public CloudCredList listCloudCred(String filterBy, String orderBy, String sortOrder, Integer offset,
                                       Integer resultsPerPage) throws BeaconClientException {
        return null;
    }

    @Override
    public FileListResult listFiles(String path) throws BeaconClientException {
        return null;
    }

    @Override
    public FileListResult listFiles(String path, String cloudCredId) throws BeaconClientException {
        return null;
    }

    @Override
    public DBListResult listDBs() throws BeaconClientException {
        return null;
    }

    @Override
    public UserPrivilegesResult getUserPrivileges() throws BeaconClientException {
        return null;
    }
}
