/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import java.util.NoSuchElementException;

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

import org.apache.commons.lang3.StringUtils;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.DBListResult;
import com.hortonworks.beacon.api.result.EventsResult;
import com.hortonworks.beacon.api.result.FileListResult;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.service.Services;

/**
 * Beacon resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon")
public class BeaconResource extends AbstractResourceManager {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconResource.class);

    @POST
    @Path("cluster/submit/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitCluster(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        try {
            requestProperties.load(request.getInputStream());
            String localStr = requestProperties.getPropertyIgnoreCase(Cluster.ClusterFields.LOCAL.getName());
            APIResult result = super.submitCluster(ClusterBuilder.buildCluster(requestProperties, clusterName));
            if (APIResult.Status.SUCCEEDED == result.getStatus()
                    && Services.get().isRegistered(PluginManagerService.SERVICE_NAME)
                    && Boolean.parseBoolean(localStr)) {
                // Register all the plugins
                ((PluginManagerService) Services.get()
                        .getService(PluginManagerService.SERVICE_NAME)).registerPlugins();
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/submit/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitReplicationPolicy(@PathParam("policy-name") String policyName,
                                             @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();

        try {
            LOG.info(MessageCode.MAIN_000060.name(), policyName);
            requestProperties.load(request.getInputStream());
            BeaconLogUtils.setLogInfo(
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.USER.getName()),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.getName()));
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            String executionType = ReplicationUtils.getReplicationPolicyType(replicationPolicy);
            replicationPolicy.setExecutionType(executionType);
            ValidationUtil.validationOnSubmission(replicationPolicy);
            APIResult result = super.submitPolicy(replicationPolicy);
            // Sync the policy with remote cluster
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                super.syncPolicyInRemote(replicationPolicy);
                LOG.info(MessageCode.MAIN_000061.name(), policyName);
            }
            return result;
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/schedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult scheduleReplicationPolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "schedule", policyName);
            ReplicationPolicy policy = PersistenceHelper.getPolicyForSchedule(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    policy.getPolicyId());
            super.schedule(policy);
            // Sync status in remote
            super.syncPolicyStatusInRemote(policy, Entity.EntityStatus.RUNNING.name());
            LOG.info(MessageCode.MAIN_000063.name(), "schedule", policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000027.name(), policyName,
                    EntityType.REPLICATIONPOLICY.name());
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/submitAndSchedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitAndScheduleReplicationPolicy(@PathParam("policy-name") String policyName,
                                                        @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "submitAndSchedule", policyName);
            requestProperties.load(request.getInputStream());
            BeaconLogUtils.setLogInfo(
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.USER.getName()),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.getName()));
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            return super.submitAndSchedulePolicy(replicationPolicy);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("cluster/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public ClusterList getClusterList(@DefaultValue("") @QueryParam("fields") String fields,
                                      @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                                      @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                      @DefaultValue("0") @QueryParam("offset") Integer offset,
                                      @QueryParam("numResults") Integer resultsPerPage) {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        return super.getClusterList(fields, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("policy/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyList getPolicyList(@DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage,
                                    @DefaultValue("3") @QueryParam("instanceCount") int instanceCount) {
        LOG.info(MessageCode.MAIN_000082.name(), filterBy);
        instanceCount = instanceCount > getMaxInstanceCount() ? getMaxInstanceCount() : instanceCount;
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        PolicyList policyList = super.getPolicyList(fields, orderBy, filterBy, sortOrder,
                offset, resultsPerPage, instanceCount);
        LOG.info(MessageCode.MAIN_000064.name(), filterBy);
        return policyList;
    }

    @GET
    @Path("cluster/status/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public StatusResult getClusterStatus(@PathParam("cluster-name") String clusterName) {
        try {
            return super.getClusterStatus(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("policy/status/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public StatusResult getPolicyStatus(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "status", policyName);
            String status = super.fetchPolicyStatus(policyName);
            LOG.info(MessageCode.MAIN_000063.name(), "status", policyName);
            return new StatusResult(policyName, status);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("policy/info/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getReplicationPolicyType(@PathParam("policy-name") String policyName) {
        try {
            String replicationPolicyType = super.getReplicationType(policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000029.name(), replicationPolicyType);
        } catch (BeaconWebException e) {
            throw e;
        }
    }

    @GET
    @Path("cluster/getEntity/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public String getCluster(@PathParam("cluster-name") String clusterName) {
        return super.getClusterDefinition(clusterName);
    }


    @GET
    @Path("policy/getEntity/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyList getPolicy(@PathParam("policy-name") String policyName,
                                @DefaultValue("false") @QueryParam("archived") String archived) {
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            LOG.info(MessageCode.MAIN_000065.name(), policyName, isArchived);
            PolicyList policyList = super.getPolicyDefinition(policyName, isArchived);
            LOG.info(MessageCode.MAIN_000066.name(), policyName, isArchived);
            return policyList;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }


    @DELETE
    @Path("cluster/delete/{cluster-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deleteCluster(@PathParam("cluster-name") String clusterName,
                                   @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        try {
            return super.deleteCluster(clusterName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }


    @DELETE
    @Path("policy/delete/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult deletePolicy(@PathParam("policy-name") String policyName,
                                  @DefaultValue("false") @QueryParam("isInternalSyncDelete")
                                          boolean isInternalSyncDelete) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "delete", policyName);
            APIResult result = super.deletePolicy(policyName, isInternalSyncDelete);
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                LOG.info(MessageCode.MAIN_000063.name(), "delete", policyName);
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/suspend/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult suspendPolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "suspend", policyName);
            APIResult result = super.suspend(policyName);
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                LOG.info(MessageCode.MAIN_000063.name(), "suspend", policyName);
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/resume/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult resumePolicy(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "resume", policyName);
            APIResult result = super.resume(policyName);
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                LOG.info(MessageCode.MAIN_000063.name(), "resume", policyName);
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("cluster/pair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult pairClusters(@QueryParam("remoteClusterName") String remoteClusterName,
                                  @DefaultValue("false") @QueryParam("isInternalPairing") boolean isInternalPairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query params remoteClusterName");
        }

        try {
            return super.pairClusters(remoteClusterName, isInternalPairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("cluster/unpair")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult unpairClusters(@QueryParam("remoteClusterName") String remoteClusterName,
                                    @DefaultValue("false") @QueryParam("isInternalUnpairing")
                                            boolean isInternalUnpairing) {
        if (StringUtils.isBlank(remoteClusterName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query params remoteClusterName");
        }

        try {
            return super.unpairClusters(remoteClusterName, isInternalUnpairing);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
            BeaconLogUtils.setLogInfo(
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.USER.getName()),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.getName()));
            String id = requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.getName());
            String executionType = requestProperties.getPropertyIgnoreCase(
                    ReplicationPolicy.ReplicationPolicyFields.EXECUTIONTYPE.getName());
            LOG.info(MessageCode.MAIN_000067.name(), policyName, id);
            if (StringUtils.isBlank(id)) {
                LOG.error(MessageCode.MAIN_000068.name());
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000026.name(), Response.Status.BAD_REQUEST);
            }
            requestProperties.remove(ReplicationPolicy.ReplicationPolicyFields.ID.getName());
            requestProperties.remove(ReplicationPolicy.ReplicationPolicyFields.EXECUTIONTYPE.getName());
            APIResult result = super.syncPolicy(policyName, requestProperties, id, executionType);
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                LOG.info(MessageCode.MAIN_000063.name(), "sync", policyName);
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
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
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query param status");
        }
        try {
            LOG.info(MessageCode.MAIN_000069.name(), policyName, status);
            APIResult result = super.syncPolicyStatus(policyName, status, isInternalStatusSync);
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                LOG.info(MessageCode.MAIN_000070.name(), policyName, status);
            }
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("policy/instance/list/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listPolicyInstances(@PathParam("policy-name") String policyName,
                                                  @QueryParam("filterBy") String filters,
                                                  @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                                  @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                                  @DefaultValue("0") @QueryParam("offset") Integer offset,
                                                  @QueryParam("numResults") Integer resultsPerPage,
                                                  @DefaultValue("false") @QueryParam("archived") String archived) {
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            return super.listPolicyInstance(policyName, filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/instance/abort/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult abortPolicyInstance(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000071.name(), "abort", policyName);
            APIResult result = super.abortPolicyInstance(policyName);
            LOG.info(MessageCode.MAIN_000072.name(), "abort", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("policy/instance/rerun/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult rerunPolicyInstance(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000071.name(), "rerun", policyName);
            APIResult result = super.rerunPolicyInstance(policyName);
            LOG.info(MessageCode.MAIN_000072.name(), "rerun", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("file/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public FileListResult listFiles(@QueryParam("path") String path) {
        try {
            if (StringUtils.isBlank(path)) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000159.name());
            }
            LOG.info(MessageCode.MAIN_000161.name(), path, ClusterHelper.getLocalCluster().getName());
            return super.listFiles(ClusterHelper.getLocalCluster(), path);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("hive/listDBs")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveDBs() {
        try {
            LOG.info(MessageCode.MAIN_000162.name(), ClusterHelper.getLocalCluster().getName());
            return super.listHiveDBs(ClusterHelper.getLocalCluster());
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("hive/listTables")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public DBListResult listHiveTables(@QueryParam("db") String dbName) {
        try {
            if (StringUtils.isBlank(dbName)) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000160.name());
            }

            LOG.info(MessageCode.MAIN_000162.name(), ClusterHelper.getLocalCluster().getName());
            return super.listHiveTables(ClusterHelper.getLocalCluster(), dbName);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("instance/list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listInstances(@QueryParam("filterBy") String filters,
                                            @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                            @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                            @DefaultValue("0") @QueryParam("offset") Integer offset,
                                            @QueryParam("numResults") Integer resultsPerPage,
                                            @DefaultValue("false") @QueryParam("archived") String archived) {
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            return super.listInstance(filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events/policy/{policy_name}")
    @Produces({MediaType.APPLICATION_JSON})
    public EventsResult getEventsWithPolicyName(@PathParam("policy_name") String policyName,
                                                @QueryParam("start") String startDate,
                                                @QueryParam("end") String endDate,
                                                @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                                @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                                @DefaultValue("0") @QueryParam("offset") Integer offset,
                                                @QueryParam("numResults") Integer resultsPerPage) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Policy name");
        }

        try {
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return super.getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events/{event_name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getEventsWithName(@PathParam("event_name") String eventName,
                                          @QueryParam("start") String startStr,
                                          @QueryParam("end") String endStr,
                                          @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                          @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                          @DefaultValue("0") @QueryParam("offset") Integer offset,
                                          @QueryParam("numResults") Integer resultsPerPage) {
        if (StringUtils.isBlank(eventName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Event Type");
        }

        try {
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return super.getEventsWithName(eventName, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events/entity/{entity_type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getEntityTypeEvents(@PathParam("entity_type") String entityType,
                                            @QueryParam("start") String startStr,
                                            @QueryParam("end") String endStr,
                                            @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                            @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                            @DefaultValue("0") @QueryParam("offset") Integer offset,
                                            @QueryParam("numResults") Integer resultsPerPage) {
        if (StringUtils.isBlank(entityType)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Event Type");
        }

        try {
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return super.getEntityTypeEvents(entityType, startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events/instance")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getEventsForInstance(@QueryParam("instanceId") String instanceId) {

        if (StringUtils.isBlank(instanceId)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Instance Id");
        }

        try {
            return super.getEventsForInstance(instanceId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events/policy/{policy_name}/{action_id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getEventsWithPolicyActionId(@PathParam("policy_name") String policyName,
                                                    @PathParam("action_id") Integer actionId) {

        if (StringUtils.isBlank(policyName)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Policy name");
        }

        try {
            return super.getEventsWithPolicyActionId(policyName, actionId);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }


    @GET
    @Path("events/all")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getAllEventsInfo(@QueryParam("start") String startStr,
                                         @QueryParam("end") String endStr,
                                         @DefaultValue("eventTimeStamp") @QueryParam("orderBy") String orderBy,
                                         @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                         @DefaultValue("0") @QueryParam("offset") Integer offset,
                                         @QueryParam("numResults") Integer resultsPerPage) {

        try {
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
            offset = checkAndSetOffset(offset);
            return super.getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPerPage);
        }  catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("events")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public EventsResult getSupportedEventDetails() {

        try {
            return super.getSupportedEventDetails();
        }  catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("logs")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult getPolicyLogs(@QueryParam("filterBy") String filters,
                                   @QueryParam("start") String startStr,
                                   @QueryParam("end") String endStr,
                                   @DefaultValue("12") @QueryParam("frequency") Integer frequency,
                                   @DefaultValue("100") @QueryParam("numResults") Integer numLogs) {
        try {
            if (StringUtils.isBlank(filters)) {
                throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query param [filterBy]");
            }
            return super.getPolicyLogs(filters, startStr, endStr, frequency, numLogs);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("admin/version")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerVersionResult getServerVersion() {
        return super.getServerVersion();
    }

    @GET
    @Path("admin/status")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerStatusResult getServerStatus() {
        return super.getServerStatus();
    }
}

