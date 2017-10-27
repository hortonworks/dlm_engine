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

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventInfo;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogHelper;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.PolicyJobBuilderFactory;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.internal.AdminJobService;
import com.hortonworks.beacon.scheduler.internal.SyncPolicyDeleteJob;
import com.hortonworks.beacon.scheduler.internal.SyncStatusJob;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon policy resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon/policy")
public class PolicyResource extends AbstractResourceManager {

    private static final BeaconLog LOG = BeaconLog.getLog(PolicyResource.class);
    private static final List<String> COMPLETION_STATUS = JobStatus.getCompletionStatus();

    @POST
    @Path("submit/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submit(@PathParam("policy-name") String policyName, @Context HttpServletRequest request) {
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        try {
            LOG.info(MessageCode.MAIN_000060.name(), policyName);
            requestProperties.load(request.getInputStream());
            LOG.info(MessageCode.MAIN_000167.name(), requestProperties);
            BeaconLogUtils.setLogInfo(
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.USER.getName()),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    requestProperties.getPropertyIgnoreCase(ReplicationPolicy.ReplicationPolicyFields.ID.getName()));
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            String executionType = ReplicationUtils.getReplicationPolicyType(replicationPolicy);
            replicationPolicy.setExecutionType(executionType);
            ValidationUtil.validationOnSubmission(replicationPolicy);
            APIResult result = submitPolicy(replicationPolicy);
            // Sync the policy with remote cluster
            if (APIResult.Status.SUCCEEDED == result.getStatus()) {
                syncPolicyInRemote(replicationPolicy);
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
    @Path("schedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult schedule(@PathParam("policy-name") String policyName) {
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "schedule", policyName);
            ReplicationPolicy policy = PersistenceHelper.getPolicyForSchedule(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName,
                    policy.getPolicyId());
            schedule(policy);
            // Sync status in remote
            syncPolicyStatusInRemote(policy, Entity.EntityStatus.RUNNING.name());
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
    @Path("submitAndSchedule/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submitAndSchedule(@PathParam("policy-name") String policyName,
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
            LOG.info(MessageCode.MAIN_000167.name(), requestProperties);
            ReplicationPolicy replicationPolicy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            return submitAndSchedulePolicy(replicationPolicy);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("list")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyList list(@DefaultValue("") @QueryParam("fields") String fields,
                           @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                           @DefaultValue("") @QueryParam("filterBy") String filterBy,
                           @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                           @DefaultValue("0") @QueryParam("offset") Integer offset,
                           @QueryParam("numResults") Integer resultsPerPage,
                           @DefaultValue("3") @QueryParam("instanceCount") Integer instanceCount,
                           @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("fields", "orderBy", "filterBy", "sortOrder", "offset", "resultsPerPage",
                "instanceCount");
        List<String> values = Arrays.asList(fields, orderBy, filterBy, sortOrder,
                offset.toString(), resultsPerPage.toString(), instanceCount.toString());
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        instanceCount = instanceCount > getMaxInstanceCount() ? getMaxInstanceCount() : instanceCount;
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        PolicyList policyList = getPolicyList(fields, orderBy, filterBy, sortOrder,
                offset, resultsPerPage, instanceCount);
        LOG.info(MessageCode.MAIN_000064.name(), filterBy);
        return policyList;
    }



    @GET
    @Path("status/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public StatusResult status(@PathParam("policy-name") String policyName, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "status", policyName);
            String status = fetchPolicyStatus(policyName);
            LOG.info(MessageCode.MAIN_000063.name(), "status", policyName);
            return new StatusResult(policyName, status);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("info/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult replicationPolicyType(@PathParam("policy-name") String policyName,
                                           @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        List<String> keys = Collections.singletonList("policyName");
        List<String> values = Collections.singletonList(policyName);
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        String replicationPolicyType = getReplicationType(policyName);
        return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000029.name(), replicationPolicyType);
    }



    @GET
    @Path("getEntity/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyList definition(@PathParam("policy-name") String policyName,
                                 @DefaultValue("false") @QueryParam("archived") String archived,
                                 @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        List<String> keys = Arrays.asList("policyName", "archived");
        List<String> values = Arrays.asList(policyName, archived);
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            LOG.info(MessageCode.MAIN_000065.name(), policyName, isArchived);
            PolicyList policyList = getPolicyDefinition(policyName, isArchived);
            LOG.info(MessageCode.MAIN_000066.name(), policyName, isArchived);
            return policyList;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @DELETE
    @Path("delete/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult delete(@PathParam("policy-name") String policyName,
                                  @DefaultValue("false") @QueryParam("isInternalSyncDelete")
                                          boolean isInternalSyncDelete, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "delete", policyName);
            APIResult result = deletePolicy(policyName, isInternalSyncDelete);
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
    @Path("suspend/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult suspend(@PathParam("policy-name") String policyName, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "suspend", policyName);
            APIResult result = suspend(policyName);
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
    @Path("resume/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult resume(@PathParam("policy-name") String policyName, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000062.name(), "resume", policyName);
            APIResult result = resume(policyName);
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
    @Path("sync/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult syncPolicy(@PathParam("policy-name") String policyName,
                                @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        PropertiesIgnoreCase requestProperties = new PropertiesIgnoreCase();
        try {
            requestProperties.load(request.getInputStream());
            LOG.info(MessageCode.MAIN_000167.name(), requestProperties);
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
            APIResult result = syncPolicy(policyName, requestProperties, id, executionType);
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
    @Path("syncStatus/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult syncPolicyStatus(@PathParam("policy-name") String policyName,
                                      @QueryParam("status") String status,
                                      @DefaultValue("false") @QueryParam("isInternalStatusSync")
                                              boolean isInternalStatusSync, @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        List<String> keys = Arrays.asList("policyName", "status");
        List<String> values = Arrays.asList(policyName, status);
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        if (StringUtils.isBlank(status)) {
            throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query param status");
        }
        try {
            LOG.info(MessageCode.MAIN_000069.name(), policyName, status);
            APIResult result = syncPolicyStatus(policyName, status, isInternalStatusSync);
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
    @Path("instance/list/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public PolicyInstanceList listPolicyInstances(@PathParam("policy-name") String policyName,
                                                  @QueryParam("filterBy") String filters,
                                                  @DefaultValue("startTime") @QueryParam("orderBy") String orderBy,
                                                  @DefaultValue("DESC") @QueryParam("sortOrder") String sortBy,
                                                  @DefaultValue("0") @QueryParam("offset") Integer offset,
                                                  @QueryParam("numResults") Integer resultsPerPage,
                                                  @DefaultValue("false") @QueryParam("archived") String archived,
                                                  @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        List<String> keys = Arrays.asList("policyName", "filterBy", "orderBy", "sortBy", "offset",
                "numResults", "archived");
        List<String> values = Arrays.asList(policyName, filters, orderBy, sortBy, offset.toString(),
                resultsPerPage.toString(), archived);
        LOG.info(MessageCode.MAIN_000167.name(), concatKeyValue(keys, values));
        try {
            boolean isArchived = Boolean.parseBoolean(archived);
            return listPolicyInstance(policyName, filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }


    @POST
    @Path("instance/abort/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult abortPolicyInstance(@PathParam("policy-name") String policyName,
                                         @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000071.name(), "abort", policyName);
            APIResult result = abortPolicyInstance(policyName);
            LOG.info(MessageCode.MAIN_000072.name(), "abort", policyName);
            return result;
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("instance/rerun/{policy-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult rerunPolicyInstance(@PathParam("policy-name") String policyName,
                                         @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName(), policyName);
        try {
            LOG.info(MessageCode.MAIN_000071.name(), "rerun", policyName);
            APIResult result = rerunPolicyInstance(policyName);
            LOG.info(MessageCode.MAIN_000072.name(), "rerun", policyName);
            return result;
        } catch (BeaconWebException e) {
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
                                   @DefaultValue("100") @QueryParam("numResults") Integer numLogs,
                                   @Context HttpServletRequest request) {
        BeaconLogUtils.setLogInfo((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        List<String> keys = Arrays.asList("filterBy", "start", "end", "frequency", "numResults");
        List<String> values = Arrays.asList(filters, startStr, endStr, frequency.toString(), numLogs.toString());
        LOG.info(MessageCode.MAIN_000167.name(), super.concatKeyValue(keys, values));
        try {
            if (StringUtils.isBlank(filters)) {
                throw BeaconWebException.newAPIException(MessageCode.COMM_010008.name(), "Query param [filterBy]");
            }
            return getPolicyLogs(filters, startStr, endStr, frequency, numLogs);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable throwable) {
            throw BeaconWebException.newAPIException(throwable, Response.Status.BAD_REQUEST);
        }
    }

    private synchronized APIResult submitPolicy(ReplicationPolicy policy) throws BeaconWebException {
        List<Entity> tokenList = new ArrayList<>();
        BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);
        EntityManager entityManager = null;
        try {
            validate(policy);
            obtainEntityLocks(policy, "submit", tokenList);
            entityManager = store.getEntityManager();
            entityManager.getTransaction().begin();
            PersistenceHelper.retireCompletedPolicy(policy.getName(), entityManager);
            PersistenceHelper.persistPolicy(policy, entityManager);
            //Sync Event is true, if current cluster is equal to source cluster.
            boolean syncEvent = (policy.getSourceCluster()).equals(ClusterHelper.getLocalCluster().getName());
            BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.POLICY, PersistenceHelper.getPolicyBean(policy),
                    getEventInfo(policy, syncEvent));
            entityManager.getTransaction().commit();
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000001.name(), policy.getEntityType(),
                    policy.getName());
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            if (entityManager != null && entityManager.getTransaction().isActive()) {
                entityManager.getTransaction().rollback();
            }
            store.closeEntityManager(entityManager);
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    private APIResult syncPolicy(String policyName, PropertiesIgnoreCase requestProperties, String id,
                                 String executionType) {
        try {
            ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            policy.setPolicyId(id);
            policy.setExecutionType(executionType);
            submitPolicy(policy);
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000020.name(), policyName);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    protected synchronized void schedule(ReplicationPolicy policy) {
        /* TODO : For HCFS job can run on source or target */
        List<Entity> tokenList = new ArrayList<>();
        try {
            ValidationUtil.validateIfAPIRequestAllowed(policy);
            JobBuilder jobBuilder = PolicyJobBuilderFactory.getJobBuilder(policy);
            List<ReplicationJobDetails> policyJobs = jobBuilder.buildJob(policy);
            if (policyJobs == null || policyJobs.isEmpty()) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000006.name(), policy.getName());
            }
            // Now get plugin related jobs and add it to front of the job list
            List<ReplicationJobDetails> pluginJobs = new PluginJobBuilder().buildJob(policy);

            List<ReplicationJobDetails> jobs = new ArrayList<>();
            if (pluginJobs != null && !pluginJobs.isEmpty()) {
                jobs.addAll(pluginJobs);
            }
            jobs.addAll(policyJobs);

            // Update the policy jobs in policy table
            String jobList = getPolicyJobList(jobs);
            PersistenceHelper.updatePolicyJobs(policy.getPolicyId(), policy.getName(), jobList);

            BeaconScheduler scheduler = getScheduler();
            obtainEntityLocks(policy, "schedule", tokenList);
            scheduler.schedulePolicy(jobs, false, policy.getPolicyId(), policy.getStartTime(), policy.getEndTime(),
                    policy.getFrequencyInSec());
            PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), JobStatus.RUNNING.name());
            BeaconEvents.createEvents(Events.SCHEDULED, EventEntityType.POLICY,
                    PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, false));
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    private APIResult submitAndSchedulePolicy(ReplicationPolicy replicationPolicy) {
        try {
            String policyName = replicationPolicy.getName();
            String executionType = ReplicationUtils.getReplicationPolicyType(replicationPolicy);
            replicationPolicy.setExecutionType(executionType);
            ValidationUtil.validationOnSubmission(replicationPolicy);
            submitPolicy(replicationPolicy);
            // Sync the policy with remote cluster
            syncPolicyInRemote(replicationPolicy);
            schedule(replicationPolicy);
            // Sync status in remote
            syncPolicyStatusInRemote(replicationPolicy, Entity.EntityStatus.RUNNING.name());
            LOG.info(MessageCode.MAIN_000063.name(), "submitAndSchedule", policyName);
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000028.name(), policyName);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private APIResult suspend(String policyName) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, policy.getPolicyId());
            ValidationUtil.validateIfAPIRequestAllowed(policy);
            String policyStatus = policy.getStatus();
            if (policyStatus.equalsIgnoreCase(JobStatus.RUNNING.name())) {
                obtainEntityLocks(policy, "suspend", tokenList);
                BeaconScheduler scheduler = getScheduler();
                scheduler.suspendPolicy(policy.getPolicyId());
                PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), JobStatus.SUSPENDED.name());
                syncPolicyStatusInRemote(policy, JobStatus.SUSPENDED.name());
            } else {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000007.name(), policy.getName(),
                        policy.getType(), policyStatus);
            }

            BeaconEvents.createEvents(Events.SUSPENDED, EventEntityType.POLICY,
                    PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, false));
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000008.name(), policy.getName(),
                    policy.getType());
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policyName, tokenList);
        }
    }

    private APIResult resume(String policyName) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, policy.getPolicyId());
            ValidationUtil.validateIfAPIRequestAllowed(policy);
            ClusterHelper.validateIfClustersPaired(policy.getSourceCluster(), policy.getTargetCluster());
            String policyStatus = policy.getStatus();
            if (policyStatus.equalsIgnoreCase(Entity.EntityStatus.SUSPENDED.name())) {
                BeaconScheduler scheduler = getScheduler();
                obtainEntityLocks(policy, "resume", tokenList);
                scheduler.resumePolicy(policy.getPolicyId());
                String status = Entity.EntityStatus.RUNNING.name();
                PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), JobStatus.RUNNING.name());
                syncPolicyStatusInRemote(policy, status);
            } else {
                throw new IllegalStateException(
                        ResourceBundleService.getService()
                                .getString(MessageCode.MAIN_000009.name(), policy.getName(), policy.getType(),
                                        policyStatus));
            }
            BeaconEvents.createEvents(Events.RESUMED, EventEntityType.POLICY,
                    PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, false));
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000010.name(), policy.getName(),
                    policy.getType());
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policyName, tokenList);
        }
    }

    private PolicyList getPolicyList(String fieldStr, String orderBy, String filterBy,
                                     String sortOrder, Integer offset, Integer resultsPerPage, int instanceCount) {
        try {
            return PersistenceHelper.getFilteredPolicy(fieldStr, filterBy, orderBy, sortOrder,
                    offset, resultsPerPage, instanceCount);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private String fetchPolicyStatus(String name) {
        try {
            return PersistenceHelper.getPolicyStatus(name);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private String getReplicationType(String policyName) {
        String replicationPolicyType;
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            replicationPolicyType = getReplicationType(policy);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
        return replicationPolicyType;
    }

    private PolicyList getPolicyDefinition(String name, boolean isArchived) {
        try {
            return PersistenceHelper.getPolicyDefinitions(name, isArchived);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private APIResult deletePolicy(String policyName, boolean isInternalSyncDelete) throws BeaconException {
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, policy.getPolicyId());
            if (!isInternalSyncDelete) {
                ValidationUtil.validateIfAPIRequestAllowed(policy);
            }
            return deletePolicy(policy, isInternalSyncDelete);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        }
    }

    private APIResult deletePolicy(ReplicationPolicy policy, boolean isInternalSyncDelete) throws BeaconException {
        List<Entity> tokenList = new ArrayList<>();
        boolean syncEvent = false;
        BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);
        EntityManager entityManager = null;
        boolean schedulerJobDelete;
        try {
            String status = policy.getStatus();
            obtainEntityLocks(policy, "delete", tokenList);
            entityManager = store.getEntityManager();
            entityManager.getTransaction().begin();
            // This is not a sync call
            Date retirementTime = new Date();
            if (!isInternalSyncDelete) {
                // The status of the policy is not submitted.
                if (!JobStatus.SUBMITTED.name().equalsIgnoreCase(status)) {
                    List<PolicyInstanceBean> instances = PersistenceHelper.getPolicyInstance(policy.getPolicyId());
                    PersistenceHelper.markInstanceJobDeleted(instances, retirementTime, entityManager);
                    // For a failed running instance retry is scheduled, in mean time user issues the
                    // policy deletion operation, so move the instance to DELETED state from RUNNING.
                    PersistenceHelper.updateInstanceStatus(policy.getPolicyId(), entityManager);
                    PersistenceHelper.markPolicyInstanceDeleted(policy.getPolicyId(), retirementTime, entityManager);
                    PersistenceHelper.deletePolicy(policy.getName(), retirementTime, entityManager);
                    schedulerJobDelete = getScheduler().deletePolicy(policy.getPolicyId());
                } else {
                    // Status of the policy is submitted.
                    PersistenceHelper.deletePolicy(policy.getName(), retirementTime, entityManager);
                    schedulerJobDelete = true;
                }
            } else {
                // This is a sync call.
                syncEvent = (policy.getSourceCluster()).equals(ClusterHelper.getLocalCluster().getName());
                PersistenceHelper.deletePolicy(policy.getName(), retirementTime, entityManager);
                schedulerJobDelete = true;
            }
            // Check policy is deleted from scheduler and commit.
            if (schedulerJobDelete) {
                entityManager.getTransaction().commit();
                BeaconEvents.createEvents(Events.DELETED, EventEntityType.POLICY,
                        PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, syncEvent));
            } else {
                throw new BeaconException(MessageCode.MAIN_000011.name(), policy.getName(), policy.getType());
            }

            //Call syncDelete when scheduler job is deleted (true) and not a sync delete call.
            if (!isInternalSyncDelete) {
                try {
                    syncDeletePolicyToRemote(policy);
                } catch (Exception e) {
                    return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000155.name(), policy.getName());
                }
            }
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000012.name(), policy.getName(),
                    policy.getType());
        } catch (BeaconException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            if (entityManager != null && entityManager.getTransaction().isActive()) {
                entityManager.getTransaction().rollback();
            }
            releaseEntityLocks(policy.getName(), tokenList);
            store.closeEntityManager(entityManager);
        }
    }

    private PolicyInstanceList listPolicyInstance(String policyName, String filters, String orderBy, String sortBy,
                                Integer offset, Integer resultsPerPage, boolean isArchived) throws BeaconException {
        if (!isArchived) {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            ValidationUtil.validateIfAPIRequestAllowed(policy);
        }

        StringBuilder newFilters = new StringBuilder();
        if (StringUtils.isNotBlank(filters)) {
            String[] filtersArray = filters.split(BeaconConstants.COMMA_SEPARATOR);
            List<String> asList = Arrays.asList(filtersArray);
            for (String str : asList) {
                if (str.startsWith("name" + BeaconConstants.COLON_SEPARATOR)) {
                    continue;
                }
                newFilters.append(str).append(BeaconConstants.COMMA_SEPARATOR);
            }
        }
        newFilters.append("name" + BeaconConstants.COLON_SEPARATOR).append(policyName);
        filters = newFilters.toString();
        return listInstance(filters, orderBy, sortBy, offset, resultsPerPage, isArchived);
    }


    private APIResult abortPolicyInstance(String policyName) {
        try {
            ReplicationPolicy activePolicy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(activePolicy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, activePolicy.getPolicyId());
            String status = activePolicy.getStatus();
            if (JobStatus.SUBMITTED.name().equalsIgnoreCase(status)
                    || COMPLETION_STATUS.contains(status.toUpperCase())) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000023.name(), policyName, status);
            }
            BeaconScheduler scheduler = getScheduler();
            boolean abortStatus = scheduler.abortInstance(activePolicy.getPolicyId());
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000024.name(), abortStatus);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    private APIResult rerunPolicyInstance(String policyName) {
        try {
            ReplicationPolicy activePolicy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(activePolicy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, activePolicy.getPolicyId());
            String status = activePolicy.getStatus();
            // Policy should be in the RUNNING state.
            if (!JobStatus.RUNNING.name().equalsIgnoreCase(status)) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000023.name(), policyName, status);
            }
            PolicyInstanceBean latestInstance = PersistenceHelper.getInstanceForRerun(activePolicy.getPolicyId());
            status = latestInstance.getStatus();
            // Last should be FAILED/KILLED for rerun the last instance.
            if (status != null && (JobStatus.FAILED.name().equalsIgnoreCase(status)
                    || JobStatus.KILLED.name().equalsIgnoreCase(status))) {
                BeaconScheduler scheduler = getScheduler();
                boolean isRerun = scheduler.rerunPolicyInstance(activePolicy.getPolicyId(),
                        String.valueOf(latestInstance.getCurrentOffset()), latestInstance.getInstanceId());
                if (isRerun) {
                    PersistenceHelper.updateInstanceRerun(latestInstance.getInstanceId());
                    return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000150.name(),
                            latestInstance.getInstanceId());
                } else {
                    return new APIResult(APIResult.Status.FAILED, MessageCode.MAIN_000151.name(),
                            latestInstance.getInstanceId());
                }
            } else {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000149.name(),
                        latestInstance.getInstanceId(), status);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void syncDeletePolicyToRemote(ReplicationPolicy policy) throws BeaconException {
        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            // No policy sync delete needed for HCFS
            return;
        }

        String remoteEndPoint = PolicyHelper.getRemoteBeaconEndpoint(policy);
        String remoteClusterName = PolicyHelper.getRemoteClusterName(policy);
        try {
            BeaconClient remoteClient = new BeaconWebClient(remoteEndPoint);
            remoteClient.deletePolicy(policy.getName(), true);
            checkAndDeleteSyncStatus(policy.getName());
        } catch (BeaconClientException e) {
            LOG.error(MessageCode.MAIN_000025.name(), remoteClusterName, e.getMessage());
            scheduleSyncPolicyDelete(remoteEndPoint, policy.getName(), e);
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000002.name(), remoteClusterName, e);
            scheduleSyncPolicyDelete(remoteEndPoint, policy.getName(), e);
        }
    }

    private void syncPolicyStatusInRemote(ReplicationPolicy policy, String status) throws BeaconException {
        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            // No policy status sync needed for HCFS
            return;
        }
        String remoteBeaconEndpoint = PolicyHelper.getRemoteBeaconEndpoint(policy);
        try {
            //TODO Check is there any sync status job scheduled. removed them and update it.
            BeaconWebClient remoteClient = new BeaconWebClient(remoteBeaconEndpoint);
            remoteClient.syncPolicyStatus(policy.getName(), status, true);
            checkAndDeleteSyncStatus(policy.getName());
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000051.name(), policy.getName(), e);
            scheduleSyncStatus(policy, status, remoteBeaconEndpoint, e);
        }
    }

    private void syncPolicyInRemote(ReplicationPolicy policy) throws BeaconException {
        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            // No policy sync needed for HCFS
            return;
        }
        boolean exceptionThrown = true;
        String policyName = policy.getName();
        try {
            syncPolicyInRemote(policy, policyName,
                    PolicyHelper.getRemoteBeaconEndpoint(policy), PolicyHelper.getRemoteClusterName(policy));
            exceptionThrown = false;
        } finally {
            // Cleanup locally
            if (exceptionThrown) {
                deletePolicy(policy, true);
            }
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void syncPolicyInRemote(ReplicationPolicy policy, String policyName,
                                    String remoteBeaconEndpoint, String remoteClusterName) {
        try {
            BeaconClient remoteClient = new BeaconWebClient(remoteBeaconEndpoint);
            remoteClient.syncPolicy(policyName, policy.toString());
            BeaconEvents.createEvents(Events.SYNCED, EventEntityType.POLICY,
                    PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, false));

        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000025.name(),
                    Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName, e.getMessage());
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000055.name(), e, policy.getSourceCluster());
        }
    }

    private APIResult getPolicyLogs(String filters, String startStr, String endStr,
                                    int frequency, int numLogs) throws BeaconException {
        try {
            String logString = BeaconLogHelper.getPolicyLogs(filters, startStr, endStr, frequency, numLogs);
            return new APIResult(APIResult.Status.SUCCEEDED, logString);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private static String getPolicyJobList(final List<ReplicationJobDetails> jobs) {
        StringBuilder jobList = new StringBuilder();
        for (ReplicationJobDetails job : jobs) {
            if (jobList.length() > 0) {
                jobList.append(",");
            }
            jobList.append(job.getIdentifier());
        }
        return jobList.toString();
    }

    private void scheduleSyncPolicyDelete(String remoteEndPoint, String policyName, Exception e)
            throws BeaconException {
        AdminJobService service = getAdminJobService();
        if (service != null) {
            SyncPolicyDeleteJob deleteJob = new SyncPolicyDeleteJob(remoteEndPoint, policyName);
            int frequency = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncFrequency();
            int maxRetry = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncMaxRetry();
            service.checkAndSchedule(deleteJob, frequency, maxRetry);
            checkAndDeleteSyncStatus(policyName);
        } else {
            throw new BeaconException(e);
        }
    }

    private void checkAndDeleteSyncStatus(String policyName) throws BeaconException {
        AdminJobService adminJobService = getAdminJobService();
        if (adminJobService != null) {
            SyncStatusJob syncStatusJob = new SyncStatusJob(null, policyName, null);
            adminJobService.checkAndDelete(syncStatusJob);
        }
    }

    private void scheduleSyncStatus(ReplicationPolicy policy, String status, String remoteBeaconEndpoint, Exception e)
            throws BeaconException {
        AdminJobService adminJobService = getAdminJobService();
        if (adminJobService != null) {
            SyncStatusJob syncStatusJob = new SyncStatusJob(remoteBeaconEndpoint, policy.getName(), status);
            int frequency = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncFrequency();
            int maxRetry = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncMaxRetry();
            adminJobService.checkAndSchedule(syncStatusJob, frequency, maxRetry);
        } else {
            throw new BeaconException(e);
        }
    }

    private AdminJobService getAdminJobService() {
        AdminJobService adminJobService = null;
        try {
            adminJobService = Services.get().getService(AdminJobService.SERVICE_NAME);
        } catch (NoSuchElementException e) {
            //AdminJob Service might not be configured, so log the message and processed.
            LOG.error(e.getMessage());
        }
        return adminJobService;
    }

    private APIResult syncPolicyStatus(String policyName, String status,
                                       boolean isInternalStatusSync) throws BeaconException {
        List<Entity> tokenList = new ArrayList<>();
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            BeaconLogUtils.setLogInfo(
                    policy.getUser(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName(),
                    policyName, policy.getPolicyId());
            boolean isCompletionStatus = COMPLETION_STATUS.contains(status.toUpperCase());
            if (isCompletionStatus) {
                PersistenceHelper.updateCompletionStatus(policy.getPolicyId(), status.toUpperCase());
            } else {
                PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), status);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000021.name());
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policyName, tokenList);
        }
    }

    private String getReplicationType(final ReplicationPolicy policy) throws BeaconException {
        String replicationPolicyType;
        try {
            replicationPolicyType = ReplicationUtils.getReplicationPolicyType(policy);
        } catch (BeaconException e) {
            throw new BeaconException(MessageCode.MAIN_000003.name(), e);
        }
        return replicationPolicyType;
    }

    BeaconQuartzScheduler getScheduler() {
        return ((SchedulerInitService)Services.get().getService(SchedulerInitService.SERVICE_NAME)).getScheduler();
    }

    private EventInfo getEventInfo(ReplicationPolicy policy, boolean syncEvent) {
        EventInfo eventInfo = new EventInfo();
        eventInfo.updateEventsInfo(policy.getSourceCluster(), policy.getTargetCluster(),
                policy.getSourceDataset(), syncEvent);
        return eventInfo;
    }

}
