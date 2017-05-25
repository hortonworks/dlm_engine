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
import com.hortonworks.beacon.api.result.EventsResult;
import com.hortonworks.beacon.api.result.StatusResult;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.Entity.EntityStatus;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.lock.MemoryLocks;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.ClusterPersistenceHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogHelper;
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.PolicyJobBuilderFactory;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.internal.AdminJobService;
import com.hortonworks.beacon.scheduler.internal.SyncStatusJob;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.PolicyInstanceListExecutor;
import com.hortonworks.beacon.store.result.PolicyInstanceList;
import com.hortonworks.beacon.util.ClusterStatus;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A base class for managing Beacon resource operations.
 */
public abstract class AbstractResourceManager {
    private static final BeaconLog LOG = BeaconLog.getLog(AbstractResourceManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    private BeaconConfig config = BeaconConfig.getInstance();

    synchronized APIResult submitCluster(Cluster cluster) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            obtainEntityLocks(cluster, "submit", tokenList);
            ClusterPersistenceHelper.submitCluster(cluster);
            BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.CLUSTER);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + cluster.getEntityType() + ") "
                    + cluster.getName());
        } catch (BeaconStoreException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to persist cluster entity ", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(cluster.getName(), tokenList);
        }
    }

    synchronized APIResult submitPolicy(ReplicationPolicy policy) throws BeaconWebException {
        List<Entity> tokenList = new ArrayList<>();
        try {
            validate(policy);
            obtainEntityLocks(policy, "submit", tokenList);
            PersistenceHelper.persistPolicy(policy);

            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful ("
                    + policy.getEntityType() + ") " + policy.getName());
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
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
                LOG.error("No jobs to schedule for : [{}]", policy.getName());
                throw BeaconWebException.newAPIException("No jobs to schedule for: " + policy.getName());
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
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Entity schedule failed for name: [{}], error: {}", policy.getName(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    /**
     * Suspends a running entity.
     *
     * @param policy policy entity
     * @return APIResult
     */
    public APIResult suspend(ReplicationPolicy policy) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            String policyStatus = PersistenceHelper.getPolicyStatus(policy.getName());
            if (policyStatus.equalsIgnoreCase(JobStatus.RUNNING.name())) {
                obtainEntityLocks(policy, "suspend", tokenList);
                BeaconScheduler scheduler = getScheduler();
                scheduler.suspendPolicy(policy.getPolicyId());
                PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), JobStatus.SUSPENDED.name());
                syncPolicyStatusInRemote(policy, JobStatus.SUSPENDED.name());
            } else {
                throw BeaconWebException.newAPIException(policy.getName() + "(" + policy.getType()
                        + ") is cannot be suspended. Current " + "status: " + policyStatus);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, policy.getName()
                    + "(" + policy.getType() + ") suspended successfully");
        } catch (Throwable e) {
            LOG.error("Unable to suspend entity", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    /**
     * Resumes a suspended entity.
     *
     * @param policy policy entity
     * @return APIResult
     */
    public APIResult resume(ReplicationPolicy policy) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            String policyStatus = PersistenceHelper.getPolicyStatus(policy.getName());
            if (policyStatus.equalsIgnoreCase(EntityStatus.SUSPENDED.name())) {
                BeaconScheduler scheduler = getScheduler();
                obtainEntityLocks(policy, "resume", tokenList);
                scheduler.resumePolicy(policy.getPolicyId());
                String status = EntityStatus.RUNNING.name();
                PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), JobStatus.RUNNING.name());
                syncPolicyStatusInRemote(policy, status);
            } else {
                throw new IllegalStateException(policy.getName()
                        + "(" + policy.getType() + ") is cannot resumed. Current status: " + policyStatus);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, policy.getName()
                    + "(" + policy.getType() + ") resumed successfully");
        } catch (Exception e) {
            LOG.error("Unable to resume entity", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    public ClusterList getClusterList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                      Integer resultsPerPage) {
        try {
            return ClusterPersistenceHelper.getFilteredClusters(fieldStr, orderBy, sortOrder, offset, resultsPerPage);
        } catch (Exception e) {
            LOG.error("Failed to get cluster list", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public PolicyList getPolicyList(String fieldStr, String orderBy, String filterBy,
                                    String sortOrder, Integer offset, Integer resultsPerPage) {
        try {
            return PersistenceHelper.getFilteredPolicy(fieldStr, filterBy, orderBy, sortOrder, offset, resultsPerPage);
        } catch (Exception e) {
            LOG.error("Failed to get policy list", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    String fetchPolicyStatus(String name) {
        try {
            return PersistenceHelper.getPolicyStatus(name);
        } catch (Exception e) {
            LOG.error("Unable to get status for policy name: [{}]", name, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public StatusResult getClusterStatus(String clusterName) {
        return new StatusResult(clusterName, EntityStatus.SUBMITTED.name());
    }

    String getReplicationType(String policyName) {
        String replicationPolicyType;
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            replicationPolicyType = getReplicationType(policy);
        } catch (Throwable e) {
            LOG.error("Unable to get replication policy type for policy {} ({})", policyName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return replicationPolicyType;
    }


    String getPolicyDefinition(String name) {
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(name);
            ObjectMapper mapper = new ObjectMapper();
            mapper.setDateFormat(DateUtil.getDateFormat());
            return mapper.writeValueAsString(policy);
        } catch (Throwable e) {
            LOG.error("Unable to policy entity definition for name: [{}]", name, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Returns the cluster definition as an XML based on name.
     *
     * @param entityName entity name
     * @return String
     */
    String getClusterDefinition(String entityName) {
        try {
            Entity entity = ClusterPersistenceHelper.getActiveCluster(entityName);
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(entity);
        } catch (Throwable e) {
            LOG.error("Unable to get cluster definition for {}", entityName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public APIResult deletePolicy(ReplicationPolicy policy, boolean isInternalSyncDelete) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            String status = policy.getStatus();
            obtainEntityLocks(policy, "delete", tokenList);
            // This is not a sync call
            Date retirementTime = new Date();
            if (!isInternalSyncDelete) {
                // The status of the policy is not submitted.
                if (!JobStatus.SUBMITTED.name().equalsIgnoreCase(status)) {
                    BeaconScheduler scheduler = getScheduler();
                    boolean deleteJob = scheduler.deletePolicy(policy.getPolicyId());
                    if (deleteJob) {
                        List<PolicyInstanceBean> instances = PersistenceHelper.getPolicyInstance(policy.getPolicyId());
                        PersistenceHelper.markInstanceJobDeleted(instances, retirementTime);
                        PersistenceHelper.markPolicyInstanceDeleted(instances, retirementTime);
                        PersistenceHelper.deletePolicy(policy.getName(), retirementTime);
                        syncDeletePolicyToRemote(policy);
                    } else {
                        String msg = "Failed to delete policy from Beacon Scheduler name: "
                                + policy.getName() + ", type: " + policy.getType();
                        LOG.error(msg);
                        throw BeaconWebException.newAPIException(new RuntimeException(msg),
                                Response.Status.INTERNAL_SERVER_ERROR);
                    }
                } else {
                    // Status of the policy is submitted.
                    PersistenceHelper.deletePolicy(policy.getName(), retirementTime);
                    syncDeletePolicyToRemote(policy);
                }
            } else {
                // This is a sync call.
                PersistenceHelper.deletePolicy(policy.getName(), retirementTime);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
        return new APIResult(APIResult.Status.SUCCEEDED,
                policy.getName() + "(" + policy.getType() + ") removed successfully.");
    }

    public APIResult deleteCluster(String clusterName) {
        if (ClusterHelper.isLocalCluster(clusterName)) {
            throw BeaconWebException.newAPIException("Local cluster " + clusterName + " cannot be deleted.");
        }
        List<Entity> tokenList = new ArrayList<>();
        try {
            Cluster cluster = ClusterPersistenceHelper.getActiveCluster(clusterName);
            obtainEntityLocks(cluster, "delete", tokenList);
            ClusterPersistenceHelper.unpairAllPairedCluster(cluster);
            ClusterPersistenceHelper.deleteCluster(cluster);
            BeaconEvents.createEvents(Events.DELETED, EventEntityType.CLUSTER);
        } catch (NoSuchElementException e) { // already deleted
            return new APIResult(APIResult.Status.SUCCEEDED,
                    clusterName + " doesn't exist. Nothing to do");
        } catch (BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(clusterName, tokenList);
        }

        return new APIResult(APIResult.Status.SUCCEEDED,
                clusterName + " removed successfully ");
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
            BeaconClient remoteClient = new BeaconClient(remoteEndPoint);
            remoteClient.deletePolicy(policy.getName(), true);
        } catch (BeaconClientException e) {
            String message = "Remote cluster " + remoteClusterName + " returned error: " + e.getMessage();
            throw BeaconWebException.newAPIException(message, Response.Status.fromStatusCode(e.getStatus()), e);
        } catch (Exception e) {
            LOG.error("Exception while sync delete policy to remote: {}", e);
            throw e;
        }
    }

    public APIResult pairClusters(String remoteClusterName, boolean isInternalPairing) {
        String localClusterName = config.getEngine().getLocalClusterName();

        if (localClusterName.equalsIgnoreCase(remoteClusterName)) {
            String message = "remoteClusterName " + remoteClusterName + " cannot be same as "
                    + "localClusterName " + localClusterName + ". Cluster cannot be paired with itself";
            throw BeaconWebException.newAPIException(message);
        }

        Cluster localCluster;
        try {
            localCluster = ClusterPersistenceHelper.getActiveCluster(localClusterName);
            if (ClusterHelper.areClustersPaired(localCluster, remoteClusterName)) {
                String status = "Cluster " + localClusterName + " has already been paired with "
                        + remoteClusterName;
                return new APIResult(APIResult.Status.SUCCEEDED, status);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        Cluster remoteCluster;
        try {
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                String message = "For pairing both local " + localClusterName + " and remote cluster "
                        + remoteClusterName + " should be submitted.";
                throw BeaconWebException.newAPIException(message, Response.Status.NOT_FOUND);
            }
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to getEntity entity definition from config store for ({}): {}", (EntityType.CLUSTER),
                    remoteClusterName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        boolean exceptionThrown = true;

        try {
            ClusterPersistenceHelper.pairCluster(localCluster, remoteCluster);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (exceptionThrown) {
                revertPairing(localCluster, remoteCluster, ClusterStatus.PAIRED);
            }
        }

        /* Call pair remote only if pairing locally succeeds else we need to rollback pairing in remote
         */
        if (!isInternalPairing) {
            exceptionThrown = true;
            BeaconClient remoteClient = new BeaconClient(remoteCluster.getBeaconEndpoint());
            try {
                pairClustersInRemote(remoteClient, remoteClusterName, localClusterName,
                        localCluster.getBeaconEndpoint());
                exceptionThrown = false;
            } finally {
                if (exceptionThrown) {
                    revertPairing(localCluster, remoteCluster, ClusterStatus.PAIRED);
                }
            }
        }

        BeaconEvents.createEvents(Events.PAIRED, EventEntityType.CLUSTER);

        return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully paired");
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void pairClustersInRemote(BeaconClient remoteClient, String remoteClusterName,
                                      String localClusterName, String localBeaconEndpoint) {
        try {
            remoteClient.pairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            String message = "Remote cluster " + remoteClusterName + " returned error: " + e.getMessage();
            throw BeaconWebException.newAPIException(message, Response.Status.fromStatusCode(e.getStatus()), e);
        } catch (Exception e) {
            LOG.error("Exception while Pairing local cluster to remote: {}", e);
            throw e;
        }
    }

    private void revertPairing(Cluster localCluster, Cluster remoteCluster, ClusterStatus status) {
        try {
            ClusterPersistenceHelper.unPairOrPairCluster(localCluster, remoteCluster, status);
        } catch (BeaconException e) {
            // Ignore exceptions for cleanup
            LOG.error(e.getMessage(), e);
        }
    }

    public APIResult unpairClusters(String remoteClusterName, boolean isInternalUnpairing) {
        String localClusterName = config.getEngine().getLocalClusterName();
        Cluster localCluster;
        try {
            localCluster = ClusterPersistenceHelper.getActiveCluster(localClusterName);
            if (!ClusterHelper.areClustersPaired(localCluster, remoteClusterName)) {
                String status = "Cluster " + localClusterName + " is not yet paired with "
                        + remoteClusterName;
                return new APIResult(APIResult.Status.SUCCEEDED, status);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            LOG.error("Unable to unpair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        Cluster remoteCluster;
        try {
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                String message = "For unpairing both local " + localClusterName + " and remote cluster "
                        + remoteClusterName + " should have been submitted and paired.";
                throw BeaconWebException.newAPIException(message, Response.Status.NOT_FOUND);
            }
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config store for ({}): {}", (EntityType.CLUSTER),
                    remoteClusterName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        boolean exceptionThrown = true;

        // Check active policies between the paired clusters.
        checkActivePolicies(localClusterName, remoteClusterName);

        try {
            // Update local cluster with paired information so that it gets pushed to remote
            ClusterPersistenceHelper.unpairPairedCluster(localCluster, remoteCluster);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error("Unable to unpair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (exceptionThrown) {
                revertPairing(localCluster, remoteCluster, ClusterStatus.PAIRED);
            }
        }

        /* Call pair remote only if pairing locally succeeds else we need to rollback pairing in remote
         */
        if (!isInternalUnpairing) {
            exceptionThrown = true;
            BeaconClient remoteClient = new BeaconClient(remoteCluster.getBeaconEndpoint());
            try {
                unpairClustersInRemote(remoteClient, remoteClusterName, localClusterName,
                        localCluster.getBeaconEndpoint());
                exceptionThrown = false;
            } finally {
                if (exceptionThrown) {
                    revertPairing(localCluster, remoteCluster, ClusterStatus.PAIRED);
                }
            }
        }

        return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully unpaired");
    }

    private void checkActivePolicies(String localClusterName, String remoteClusterName) {
        boolean exists = PersistenceHelper.activePairedClusterPolicies(localClusterName, remoteClusterName);
        if (exists) {
            throw BeaconWebException.newAPIException("Active policies are present, unpair operation can not be done.",
                    Response.Status.BAD_REQUEST);
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void unpairClustersInRemote(BeaconClient remoteClient, String remoteClusterName,
                                        String localClusterName, String localBeaconEndpoint) {
        try {
            remoteClient.unpairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            String message = "Remote cluster " + remoteClusterName + " returned error: " + e.getMessage();
            throw BeaconWebException.newAPIException(message, Response.Status.fromStatusCode(e.getStatus()), e);
        } catch (Exception e) {
            LOG.error("Exception while unpairing local cluster to remote: {}", e);
            throw e;
        }
    }

    APIResult syncPolicy(String policyName, PropertiesIgnoreCase requestProperties, String id) {
        try {
            ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            policy.setPolicyId(id);
            submitPolicy(policy);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit and Sync policy successful (" + policyName + ") ");
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to sync the policy", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    void syncPolicyStatusInRemote(ReplicationPolicy policy, String status) throws BeaconException {
        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            // No policy status sync needed for HCFS
            return;
        }

        String remoteBeaconEndpoint = PolicyHelper.getRemoteBeaconEndpoint(policy);
        try {
            //TODO Check is there any sync status job scheduled. removed them and update it.
            BeaconClient remoteClient = new BeaconClient(remoteBeaconEndpoint);
            remoteClient.syncPolicyStatus(policy.getName(), status, true);
            checkAndDeleteSyncStatus(policy);
        } catch (BeaconClientException e) {
            LOG.error("Exception while sync status for policy: [{}].", policy.getName(), e);
            scheduleSyncStatus(policy, status, remoteBeaconEndpoint, e);
        } catch (Exception e) {
            LOG.error("Exception while sync status for policy: [{}] {}", policy.getName(), e);
            scheduleSyncStatus(policy, status, remoteBeaconEndpoint, e);
        }
    }

    private void checkAndDeleteSyncStatus(ReplicationPolicy policy) throws BeaconException {
        AdminJobService adminJobService = getAdminJobService();
        if (adminJobService != null) {
            SyncStatusJob syncStatusJob = new SyncStatusJob(null, policy.getName(), null);
            adminJobService.checkAndDelete(syncStatusJob);
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

    private void scheduleSyncStatus(ReplicationPolicy policy, String status, String remoteBeaconEndpoint, Exception e)
            throws BeaconException {
        AdminJobService adminJobService = getAdminJobService();
        if (adminJobService != null) {
            SyncStatusJob syncStatusJob = new SyncStatusJob(remoteBeaconEndpoint, policy.getName(), status);
            int frequency = BeaconConfig.getInstance().getScheduler().getSyncStatusFrequency();
            adminJobService.checkAndSchedule(syncStatusJob, frequency);
        } else {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    public APIResult syncPolicyStatus(String policyName, String status,
                                      boolean isInternalStatusSync) throws BeaconException {
        List<Entity> tokenList = new ArrayList<>();
        try {
            ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
            PersistenceHelper.updatePolicyStatus(policy.getName(), policy.getType(), status);
            return new APIResult(APIResult.Status.SUCCEEDED, "Update status succeeded");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Entity update status failed for " + policyName + ": " + " in remote cluster "
                    + config.getEngine().getLocalClusterName(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(policyName, tokenList);
        }
    }

    PolicyInstanceList listPolicyInstance(String policyName, String filters, String orderBy, String sortBy,
                                          Integer offset, Integer resultsPerPage) throws BeaconException {
        ReplicationPolicy policy = PersistenceHelper.getActivePolicy(policyName);
        ValidationUtil.validateIfAPIRequestAllowed(policy);

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
        return listInstance(filters, orderBy, sortBy, offset, resultsPerPage);
    }

    PolicyInstanceList listInstance(String filters, String orderBy, String sortBy, Integer offset,
                                    Integer resultsPerPage) throws BeaconException {
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = offset >= 0 ? offset : 0;
        PolicyInstanceListExecutor executor = new PolicyInstanceListExecutor();
        try {
            return executor.getFilteredJobInstance(filters, orderBy, sortBy, offset, resultsPerPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected Integer getDefaultResultsPerPage() {
        return config.getEngine().getResultsPerPage();
    }

    Integer getMaxResultsPerPage() {
        return config.getEngine().getMaxResultsPerPage();
    }

    private void obtainEntityLocks(Entity entity, String command, List<Entity> tokenList)
            throws BeaconException {
        //first obtain lock for the entity for which update is issued.
        if (memoryLocks.acquireLock(entity, command)) {
            tokenList.add(entity);
        } else {
            throw new BeaconException(command + " command is already issued for " + entity.toShortString());
        }

        /* TODO : now obtain locks for all dependent entities if any */
    }

    private void releaseEntityLocks(String entityName, List<Entity> tokenList) {
        if (tokenList != null && !tokenList.isEmpty()) {
            for (Entity entity : tokenList) {
                memoryLocks.releaseLock(entity);
            }
            LOG.info("All locks released on {}", entityName);
        } else {
            LOG.info("No locks to release on " + entityName);
        }

    }

    private void validate(Entity entity) throws BeaconException {
        EntityValidator validator = EntityValidatorFactory.getValidator(entity.getEntityType());
        validator.validate(entity);
    }

    private String getReplicationType(final ReplicationPolicy policy) throws BeaconException {
        String replicationPolicyType;
        try {
            replicationPolicyType = ReplicationUtils.getReplicationPolicyType(policy);
        } catch (BeaconException e) {
            throw new BeaconException("Exception while obtain replication type:", e);
        }
        return replicationPolicyType;
    }

    public void syncPolicyInRemote(ReplicationPolicy policy) throws BeaconException {
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
                deletePolicy(policy, false);
            }
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void syncPolicyInRemote(ReplicationPolicy policy, String policyName,
                                    String remoteBeaconEndpoint, String remoteClusterName) {
        try {
            BeaconClient remoteClient = new BeaconClient(remoteBeaconEndpoint);
            remoteClient.syncPolicy(policyName, policy.toString());
        } catch (BeaconClientException e) {
            String message = "Remote cluster " + remoteClusterName + " returned error: " + e.getMessage();
            throw BeaconWebException.newAPIException(message, Response.Status.fromStatusCode(e.getStatus()), e);
        } catch (Exception e) {
            LOG.error("Exception while sync policy to source cluster: [{}]", policy.getSourceCluster(), e);
            throw e;
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

    private BeaconQuartzScheduler getScheduler() {
        return ((SchedulerInitService)Services.get().getService(SchedulerInitService.SERVICE_NAME)).getScheduler();
    }

    protected EventsResult getEventsWithPolicyName(String policyName, String startDate, String endDate,
                                                   Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getEventsWithPolicyName(policyName, startDate, endDate, offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEventsWithName(String eventName, String eventEntityType, String startStr, String endStr,
                                             Integer offset, Integer resultsPage) throws BeaconException {
        try {
            Events event = BeaconEventsHelper.validateEventName(eventName);
            if (event == null) {
                throw new BeaconException("Event Name :" + eventName + "not supported ");
            }

            EventEntityType type = BeaconEventsHelper.validateEventEntityType(eventEntityType);
            LOG.info("Events id  : {} for event name : {}", event.getId(), eventName);
            return BeaconEventsHelper.getEventsWithName(event.getId(), (type == null ? null : type.getName()),
                    startStr, endStr, offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEntityTypeEvents(String entityType, String startStr, String endStr,
                                               Integer offset, Integer resultsPage) throws BeaconException {
        try {
            EventEntityType type = BeaconEventsHelper.validateEventEntityType(entityType);
            if (type != null) {
                LOG.info("Find events for the entity type : {}", type.getName());
                return BeaconEventsHelper.getEntityTypeEvents(type.getName(), startStr, endStr, offset, resultsPage);
            } else {
                throw new BeaconException("Entity type :" + entityType + "not supported ");
            }
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEventsForInstance(String instanceId) throws BeaconException {
        try {
            return BeaconEventsHelper.getInstanceEvents(instanceId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEventsWithPolicyActionId(String policyName, Integer actionId) throws BeaconException {
        try {
            return BeaconEventsHelper.getEventsWithPolicyActionId(policyName, actionId);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }


    protected EventsResult getAllEventsInfo(String startStr, String endStr,
                                        Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getAllEventsInfo(startStr, endStr, offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getSupportedEventDetails() throws BeaconException {
        try {
            return BeaconEventsHelper.getSupportedEventDetails();
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    public APIResult abortPolicyInstance(String policyName) {
        try {
            ReplicationPolicy activePolicy = PersistenceHelper.getActivePolicy(policyName);
            String status = activePolicy.getStatus();
            if (!JobStatus.RUNNING.name().equalsIgnoreCase(status)) {
                String message = "Policy [" + policyName + "] is not in [RUNNING] state. "
                        + "Current status [" + status + "]";
                throw BeaconWebException.newAPIException(message);
            }
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            boolean abortStatus = scheduler.abortInstance(activePolicy.getPolicyId());
            return new APIResult(APIResult.Status.SUCCEEDED, "policy instance abort status "
                    + "[" + abortStatus + "]");
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    protected APIResult getPolicyLogs(String filters, String startStr, String endStr,
                                      int frequency, int numLogs) throws BeaconException {
        try {
            String logString = BeaconLogHelper.getPolicyLogs(filters, startStr, endStr, frequency, numLogs);
            return new APIResult(APIResult.Status.SUCCEEDED, logString);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
