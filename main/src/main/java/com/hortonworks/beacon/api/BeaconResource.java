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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.PolicyInstanceList;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.NoSuchElementException;

/**
 * Beacon resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconResource.class);

    @POST
    @Path("cluster/submit/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitCluster(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();

        try {
            requestProperties.load(request.getInputStream());
            return super.submit(ClusterBuilder.buildCluster(requestProperties, clusterName));
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/submit/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitReplicationPolicy(@PathParam("policy-name") String policyName,
                                             @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();

        try {
            LOG.info("Request for submit policy is received. policy-name: [{}]", policyName);
            requestProperties.load(request.getInputStream());
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            ValidationUtil.validatePolicy(replicationPolicy);
            ValidationUtil.validateIfAPIRequestAllowed(replicationPolicy);
            APIResult result = super.submitPolicy(replicationPolicy);
            // Sync the policy with remote cluster
            super.syncPolicyInRemote(replicationPolicy);
            LOG.info("Request for submit policy is processed successfully. policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/schedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult scheduleReplicationPolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info("Request for policy schedule is received. Policy-name: [{}]", policyName);
            ReplicationPolicy policy = PersistenceHelper.getPolicyForSchedule(policyName);
            super.schedule(policy);
            // Sync status in remote
            super.syncPolicyStatusInRemote(policy, Entity.EntityStatus.RUNNING.name());
            LOG.info("Request for policy schedule is processed successfully. Policy-name: [{}]", policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, policyName
                    + "(" + EntityType.REPLICATIONPOLICY.name() + ") scheduled successfully");
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/submitAndSchedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitAndScheduleReplicationPolicy(@PathParam("policy-name") String policyName,
                                                        @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        try {
            LOG.info("Request for policy submitAndSchedule is received. Policy-name: [{}]", policyName);
            requestProperties.load(request.getInputStream());
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            ValidationUtil.validatePolicy(replicationPolicy);
            ValidationUtil.validateIfAPIRequestAllowed(replicationPolicy);
            APIResult result = super.submitAndSchedule(replicationPolicy);
            // Sync the policy with remote cluster
            super.syncPolicyInRemote(replicationPolicy);
            // Sync status in remote
            super.syncPolicyStatusInRemote(replicationPolicy, Entity.EntityStatus.RUNNING.name());
            LOG.info("Request for policy submitAndSchedule is processed successfully. Policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("cluster/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public ClusterList getClusterList(@DefaultValue("") @QueryParam("fields") String fields,
                                      @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                      @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                      @DefaultValue("0") @QueryParam("offset") Integer offset,
                                      @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        return super.getClusterList(fields, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("policy/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyList getPolicyList(@DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("1") @QueryParam("offset") Integer offset,
                                    @DefaultValue("10") @QueryParam("numResults") Integer resultsPerPage) {
        LOG.info("Request for policy list is received. filterBy: [{}]", filterBy);
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        PolicyList policyList = super.getPolicyList(fields, orderBy, filterBy, sortOrder, offset, resultsPerPage);
        LOG.info("Request for policy list is processed successfully. filterBy: [{}]", filterBy);
        return policyList;
    }

    @GET
    @Path("cluster/status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getClusterStatus(@PathParam("cluster-name") String clusterName) {
        try {
            String status = super.getStatus(EntityType.CLUSTER.name(), clusterName);
            return new APIResult(APIResult.Status.SUCCEEDED, status);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("policy/status/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getPolicyStatus(@PathParam("policy-name") String policyName) {
        try {
            LOG.info("Request for policy status is received. policy-name: [{}]", policyName);
            String status = super.fetchPolicyStatus(policyName);
            LOG.info("Request for policy status is processed successfully. policy-name: [{}]", policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, status);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("policy/info/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getReplicationPolicyType(@PathParam("policy-name") String policyName) {
        try {
            String status = super.getReplicationPolicyType(EntityType.REPLICATIONPOLICY.name(), policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, "type=" + status);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("cluster/getEntity/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String getCluster(@PathParam("cluster-name") String clusterName) {
        return super.getEntityDefinition(EntityType.CLUSTER.name(), clusterName);
    }


    @GET
    @Path("policy/getEntity/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String getPolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info("Request for policy getEntity is received. policy-name: [{}]", policyName);
            String policyDefinition = super.getPolicyDefinition(policyName);
            LOG.info("Request for policy getEntity is processed successfully. policy-name: [{}]", policyName);
            return policyDefinition;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    @DELETE
    @Path("cluster/delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deleteCluster(@PathParam("cluster-name") String clusterName) {
        try {
            return super.deleteCluster(EntityType.CLUSTER.name(), clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    @DELETE
    @Path("policy/delete/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deletePolicy(@PathParam("policy-name") String policyName,
                                  @DefaultValue("false") @QueryParam("isInternalSyncDelete")
                                          boolean isInternalSyncDelete) {
        try {
            LOG.info("Request for policy delete is received. policy-name: [{}]", policyName);
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            if (!isInternalSyncDelete) {
                ValidationUtil.validateIfAPIRequestAllowed(policy);
            }
            APIResult result = super.deletePolicy(policy, isInternalSyncDelete);
            LOG.info("Request for policy delete is processed successfully. policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/suspend/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult suspendPolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info("Request for policy suspend is received. policy-name: [{}]", policyName);
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            ValidationUtil.validateIfAPIRequestAllowed(policy);
            APIResult result = super.suspend(policy);
            LOG.info("Request for policy suspend is processed successfully. policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/resume/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult resumePolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info("Request for policy resume is received. policy-name: [{}]", policyName);
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            ValidationUtil.validateIfAPIRequestAllowed(policy);
            APIResult result = super.resume(policy);
            LOG.info("Request for policy resume is processed successfully. policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("cluster/pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pairClusters(@QueryParam("remoteClusterName") String remoteClusterName,
                                  @DefaultValue("false") @QueryParam("isInternalPairing") boolean isInternalPairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteBeaconEndpoint and remoteClusterName cannot "
                    + "be null or empty");
        }

        try {
            return super.pairClusters(remoteClusterName, isInternalPairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("cluster/unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult unpairClusters(@QueryParam("remoteClusterName") String remoteClusterName,
                                    @DefaultValue("false") @QueryParam("isInternalUnpairing")
                                            boolean isInternalUnpairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteBeaconEndpoint and remoteClusterName cannot "
                    + "be null or empty");
        }

        try {
            return super.unpairClusters(remoteClusterName, isInternalUnpairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/sync/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult syncPolicy(@PathParam("policy-name") String policyName,
                                @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        try {
            requestProperties.load(request.getInputStream());
            String id = requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.name());
            LOG.info("Request for policy sync is received. policy-name: [{}], id: [{}]", policyName, id);
            if (StringUtils.isBlank(id)) {
                LOG.error("This should never happen. Policy id should be present during policy sync.");
                throw BeaconWebException.newAPIException("Policy id should be present during sync.",
                        Response.Status.INTERNAL_SERVER_ERROR);
            }
            APIResult result = super.syncPolicy(policyName, requestProperties, id);
            LOG.info("Request for policy sync is processed successfully. policy-name: [{}]", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("policy/syncStatus/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult syncPolicyStatus(@PathParam("policy-name") String policyName,
                                      @QueryParam("status") String status,
                                      @DefaultValue("false") @QueryParam("isInternalStatusSync")
                                              boolean isInternalStatusSync) {
        if (StringUtils.isBlank(status)) {
            throw BeaconWebException.newAPIException("Query param status cannot be null or empty");
        }
        try {
            LOG.info("Request for policy syncStatus is received. policy-name: [{}], status: [{}]", policyName, status);
            APIResult result = super.syncPolicyStatus(policyName, status, isInternalStatusSync);
            LOG.info("Request for policy syncStatus is processed successfully. policy-name: [{}], status: [{}]",
                    policyName, status);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("policy/instance/list/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listPolicyInstances(@PathParam("policy-name") String policyName,
                                                  @QueryParam("filterBy") String filters,
                                                  @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                                  @DefaultValue("ASC") @QueryParam("sortOrder") String sortBy,
                                                  @DefaultValue("1") @QueryParam("offset") Integer offset,
                                                  @DefaultValue("10") @QueryParam("numResults") Integer resultsPerPage){
        try {
            return super.listPolicyInstance(policyName, filters, orderBy, sortBy, offset, resultsPerPage);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("instance/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listInstances(@QueryParam("filterBy") String filters,
                                            @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                            @DefaultValue("ASC") @QueryParam("sortOrder") String sortBy,
                                            @DefaultValue("1") @QueryParam("offset") Integer offset,
                                            @DefaultValue("10") @QueryParam("numResults") Integer resultsPerPage) {
        if (StringUtils.isBlank(filters)) {
            throw BeaconWebException.newAPIException("Query param [filter] cannot be null or empty");
        }
        try {
            return super.listInstance(filters, orderBy, sortBy, offset, resultsPerPage);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}

