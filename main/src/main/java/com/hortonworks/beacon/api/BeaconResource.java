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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.JobInstanceList;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import org.apache.commons.lang3.StringUtils;

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
 * Root resource (exposed at "myresource" path)
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

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
            requestProperties.load(request.getInputStream());
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            ValidationUtil.validateIfAPIRequestAllowed(replicationPolicy, ValidationUtil.OperationType.WRITE);
            APIResult result = super.submit(replicationPolicy);
            // Sync the policy with remote cluster
            super.syncPolicyInRemote(policyName);
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
            ValidationUtil.validateIfAPIRequestAllowed(policyName, ValidationUtil.OperationType.WRITE);
            super.schedule(EntityType.REPLICATIONPOLICY.name(), policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, policyName + "(" + EntityType.REPLICATIONPOLICY.name() + ") scheduled successfully");
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
            requestProperties.load(request.getInputStream());
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            ValidationUtil.validateIfAPIRequestAllowed(replicationPolicy, ValidationUtil.OperationType.WRITE);
            APIResult result = super.submitAndSchedule(replicationPolicy);
            // Sync the policy with remote cluster
            super.syncPolicyInRemote(policyName);
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
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        return super.getPolicyList(fields, orderBy, filterBy, sortOrder, offset, resultsPerPage);
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
            String status = super.getStatus(EntityType.REPLICATIONPOLICY.name(), policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, status);
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
        return super.getEntityDefinition(EntityType.REPLICATIONPOLICY.name(), policyName);
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
                                  @DefaultValue("false") @QueryParam("isInternalSyncDelete") boolean isInternalSyncDelete) {
        try {
            if (!isInternalSyncDelete) {
                ValidationUtil.validateIfAPIRequestAllowed(policyName, ValidationUtil.OperationType.WRITE);
            }
            return super.deletePolicy(EntityType.REPLICATIONPOLICY.name(), policyName, isInternalSyncDelete);
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
            ValidationUtil.validateIfAPIRequestAllowed(policyName, ValidationUtil.OperationType.WRITE);
            return super.suspend(EntityType.REPLICATIONPOLICY.name(), policyName);
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
            ValidationUtil.validateIfAPIRequestAllowed(policyName, ValidationUtil.OperationType.WRITE);
            return super.resume(EntityType.REPLICATIONPOLICY.name(), policyName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("cluster/pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pairClusters(@QueryParam("remoteBeaconEndpoint") String remoteBeaconEndpoint,
                                  @QueryParam("remoteClusterName") String remoteClusterName,
                                  @DefaultValue("false") @QueryParam("isInternalPairing") boolean isInternalPairing) {
        if (StringUtils.isBlank(remoteBeaconEndpoint) || StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteBeaconEndpoint and remoteClusterName cannot " +
                    "be null or empty");
        }

        try {
            return super.pairClusters(remoteBeaconEndpoint, remoteClusterName, isInternalPairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("cluster/unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult unpairClusters(@QueryParam("remoteBeaconEndpoint") String remoteBeaconEndpoint,
                                    @QueryParam("remoteClusterName") String remoteClusterName,
                                    @DefaultValue("false") @QueryParam("isInternalUnpairing") boolean isInternalUnpairing) {
        if (StringUtils.isBlank(remoteBeaconEndpoint) || StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException("Query params remoteBeaconEndpoint and remoteClusterName cannot " +
                    "be null or empty");
        }

        try {
            return super.unpairClusters(remoteBeaconEndpoint, remoteClusterName, isInternalUnpairing);
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
            return super.syncPolicy(policyName, requestProperties);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("policy/instance/list/{entity-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public JobInstanceList listInstances(@PathParam("entity-name") String entityName,
                                         @DefaultValue("") @QueryParam("status") String status,
                                         @DefaultValue("") @QueryParam("startTime") String startTime,
                                         @DefaultValue("") @QueryParam("endTime") String endTime,
                                         @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                         @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                         @DefaultValue("0") @QueryParam("offset") Integer offset,
                                         @QueryParam("numResults") Integer resultsPerPage) {
        try {
            return super.listInstance(entityName, status, startTime, endTime, orderBy, sortOrder, offset, resultsPerPage);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}

