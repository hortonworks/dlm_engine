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
import com.hortonworks.beacon.api.result.APIResult;
import com.hortonworks.beacon.api.result.EntityList;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;

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
import java.util.Properties;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    @POST
    @Path("cluster/submit/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitCluster(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        Properties requestProperties = new Properties();

        try {
            requestProperties.load(request.getInputStream());
            return super.submit(ClusterBuilder.buildCluster(requestProperties));
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("policy/submit/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitReplicationPolicy(@PathParam("policy-name") String policyName,
                                             @Context HttpServletRequest request) {
        Properties requestProperties = new Properties();

        try {
            requestProperties.load(request.getInputStream());
            return super.submit(ReplicationPolicyBuilder.buildPolicy(requestProperties));
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("policy/schedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult scheduleReplicationPolicy(@PathParam("policy-name") String policyName) {
        try {
            super.schedule(EntityType.REPLICATIONPOLICY.name(), policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, policyName + "(" + EntityType.REPLICATIONPOLICY.name() + ") scheduled successfully");
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e);
        }
    }

    @POST
    @Path("policy/submitAndSchedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitAndScheduleReplicationPolicy(@PathParam("policy-name") String policyName,
                                             @Context HttpServletRequest request) {
        Properties requestProperties = new Properties();

        try {
            requestProperties.load(request.getInputStream());
            return super.submitAndSchedule(ReplicationPolicyBuilder.buildPolicy(requestProperties));
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("cluster/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EntityList getClusterList(@DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        return super.getEntityList(fields, orderBy, sortOrder, offset, resultsPerPage, EntityType.CLUSTER);
    }

    @GET
    @Path("policy/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EntityList getPolicyList(@DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        return super.getEntityList(fields, orderBy, sortOrder, offset, resultsPerPage, EntityType.REPLICATIONPOLICY);
    }

    @GET
    @Path("cluster/status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getClusterStatus(@PathParam("cluster-name") String clusterName) {
        try {
            return super.getStatus(EntityType.CLUSTER.name(), clusterName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("policy/status/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getPolicyStatus(@PathParam("policy-name") String policyName) {
        try {
            return super.getStatus(EntityType.REPLICATIONPOLICY.name(), policyName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("cluster/get/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String getCluster(@PathParam("cluster-name") String clusterName) {
        return super.getEntityDefinition(EntityType.CLUSTER.name(), clusterName);
    }


    @GET
    @Path("policy/get/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String getPolicy(@PathParam("policy-name") String policyName) {
        return super.getEntityDefinition(EntityType.REPLICATIONPOLICY.name(), policyName);
    }


    @DELETE
    @Path("cluster/delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deleteCluster(@PathParam("cluster-name") String clusterName) {
        try {
            return super.delete(EntityType.CLUSTER.name(), clusterName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }


    @DELETE
    @Path("policy/delete/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deletePolicy(@PathParam("policy-name") String policyName) {
        try {
            return super.delete(EntityType.REPLICATIONPOLICY.name(), policyName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("policy/suspend/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult suspendPolicy(@PathParam("policy-name") String policyName) {
        try {
            return super.suspend(EntityType.REPLICATIONPOLICY.name(), policyName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("policy/resume/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult resumePolicy(@PathParam("policy-name") String policyName) {
        try {
            return super.resume(EntityType.REPLICATIONPOLICY.name(), policyName);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("pair/{remotecluster-endpoint}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pairClusters(@PathParam("remotecluster-endpoint") String remoteClusterBeaconEndPoint,
                                             @Context HttpServletRequest request) {
        try {
            return super.pairCusters(remoteClusterBeaconEndPoint);
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable);
        }
    }

}

