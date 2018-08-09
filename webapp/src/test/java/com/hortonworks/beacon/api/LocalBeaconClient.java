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

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.EventsResult;
import com.hortonworks.beacon.client.result.FileListResult;

/**
 * Local beacon client that calls resource's methods in the same JVM.
 */
public class LocalBeaconClient implements BeaconClient {


    private ClusterResource clusterResource = new ClusterResource();

    private PolicyResource policyResource = new PolicyResource();

    @Override
    public void submitCluster(final String clusterName, final PropertiesIgnoreCase properties)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return clusterResource.submit(clusterName, properties);
            }
        }.call();
    }

    abstract static class ClientResource<T> {
        abstract T api() throws BeaconWebException;
        T call() throws BeaconClientException {
            try {
                RequestContext.setInitialValue();
                return api();
            } catch (BeaconWebException e) {
                BeaconClientException clientException = new BeaconClientException(e);
                clientException.setStatus(e.getResponse().getStatus());
                throw clientException;
            } finally {
                RequestContext.get().clear();
            }
        }
    }

    @Override
    public ClusterList getClusterList(final String fields, final String orderBy,
                                      final String sortOrder, final Integer offset,
                                      final Integer numResults) throws BeaconClientException {
        return new ClientResource<ClusterList>() {
            @Override
            ClusterList api() throws BeaconWebException {
                return clusterResource.list(fields, orderBy, sortOrder, offset, numResults);
            }
        }.call();
    }

    @Override
    public StatusResult getClusterStatus(final String clusterName) throws BeaconClientException {
        return new ClientResource<StatusResult>(

        ) {
            @Override
            StatusResult api() throws BeaconWebException {
                return clusterResource.status(clusterName);
            }
        }.call();
    }

    @Override
    public Cluster getCluster(final String clusterName) throws BeaconClientException {
        return new ClientResource<Cluster>(
        ) {
            @Override
            Cluster api() throws BeaconWebException {
                return clusterResource.definition(clusterName);
            }
        }.call();
    }

    @Override
    public void deleteCluster(final String clusterName) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return clusterResource.delete(clusterName);
            }
        }.call();
    }

    @Override
    public void updateCluster(String clusterName, PropertiesIgnoreCase properties) throws BeaconClientException {

    }

    @Override
    public void submitAndScheduleReplicationPolicy(final String policyName, final PropertiesIgnoreCase properties)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.submitAndSchedule(policyName, "false", properties);
            }
        }.call();
    }

    @Override
    public void dryrunPolicy(String policyName, PropertiesIgnoreCase properties) throws BeaconClientException {

    }

    @Override
    public PolicyList getPolicyList(final String fields, final String orderBy, final String filterBy,
                                    final String sortOrder, final Integer offset,
                                    final Integer numResults) throws BeaconClientException {
        return new ClientResource<PolicyList>() {
            @Override
            PolicyList api() throws BeaconWebException {
                return policyResource.list(fields, orderBy, filterBy, sortOrder,
                        offset, numResults, 3);
            }
        }.call();
    }

    @Override
    public StatusResult getPolicyStatus(final String policyName) throws BeaconClientException {
        return new ClientResource<StatusResult>() {
            @Override
            StatusResult api() throws BeaconWebException {
                return policyResource.status(policyName);
            }
        }.call();
    }

    @Override
    public PolicyList getPolicy(final String policyName) throws BeaconClientException {
        return new ClientResource<PolicyList>() {
            @Override
            PolicyList api() throws BeaconWebException {
                return policyResource.definition(policyName, "false");
            }
        }.call();
    }

    @Override
    public void deletePolicy(final String policyName, final boolean isInternalSyncDelete) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.delete(policyName, isInternalSyncDelete);
            }
        }.call();
    }

    @Override
    public void updatePolicy(final String policyName, final PropertiesIgnoreCase properties)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.update(policyName, properties);
            }
        }.call();
    }

    @Override
    public void suspendPolicy(final String policyName) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.suspend(policyName);
            }
        }.call();
    }

    @Override
    public void resumePolicy(final String policyName) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.resume(policyName);
            }
        }.call();
    }

    @Override
    public void pairClusters(final String remoteClusterName, final boolean isInternalPairing)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return clusterResource.pair(remoteClusterName, isInternalPairing);
            }
        }.call();
    }

    @Override
    public void unpairClusters(final String remoteClusterName, final boolean isInternalunpairing)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return clusterResource.unPair(remoteClusterName, isInternalunpairing);
            }
        }.call();
    }

    @Override
    public void syncPolicy(final String policyName, final PropertiesIgnoreCase policyDefinition, final boolean update)
            throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.syncPolicy(policyName, update, policyDefinition);
            }
        }.call();
    }

    @Override
    public void syncPolicyStatus(final String policyName, final String status, boolean isInternalStatusSync)
            throws BeaconClientException {
    }

    @Override
    public PolicyInstanceList listPolicyInstances(final String policyName) throws BeaconClientException {
        return new ClientResource<PolicyInstanceList>() {
            @Override
            PolicyInstanceList api() throws BeaconWebException {
                return policyResource.listPolicyInstances(policyName, "", "startTime",
                        "DESC", 0, 10, "false");
            }
        }.call();
    }

    @Override
    public void abortPolicyInstance(final String policyName) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.abortPolicyInstance(policyName);
            }
        }.call();
    }

    @Override
    public void rerunPolicyInstance(final String policyName) throws BeaconClientException {
        new ClientResource<APIResult>() {
            @Override
            APIResult api() throws BeaconWebException {
                return policyResource.rerunPolicyInstance(policyName);
            }
        }.call();
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
    public ServerStatusResult getServiceStatus() throws BeaconClientException {
        return null;
    }

    @Override
    public ServerVersionResult getServiceVersion() throws BeaconClientException {
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

    @Override
    public EventsResult getAllEvents() throws BeaconClientException {
        return null;
    }
}
