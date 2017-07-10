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
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
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
import com.hortonworks.beacon.events.EventInfo;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogHelper;
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
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
import com.hortonworks.beacon.util.ClusterStatus;
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
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000001.name(), cluster.getEntityType(),
                    cluster.getName());
        } catch (BeaconStoreException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000033.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
            //Sync Event is true, if current cluster is equal to source cluster.
            boolean syncEvent = (policy.getSourceCluster()).equals(config.getEngine().getLocalClusterName());
            BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.POLICY, PersistenceHelper.getPolicyBean(policy),
                    getEventInfo(policy, syncEvent));
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000001.name(), policy.getEntityType(),
                    policy.getName());
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000034.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
                LOG.error(MessageCode.MAIN_000035.name(), policy.getName());
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
            LOG.error(MessageCode.MAIN_000036.name(), policy.getName(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000007.name(), policy.getName(),
                        policy.getType(), policyStatus);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000008.name(), policy.getName(),
                    policy.getType());
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000037.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
                throw new IllegalStateException(
                        ResourceBundleService.getService()
                                .getString(MessageCode.MAIN_000009.name(), policy.getName(), policy.getType(),
                                        policyStatus));
            }
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000010.name(), policy.getName(),
                    policy.getType());
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000038.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
    }

    public ClusterList getClusterList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                      Integer resultsPerPage) {
        try {
            return ClusterPersistenceHelper.getFilteredClusters(fieldStr, orderBy, sortOrder, offset, resultsPerPage);
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000039.name(), "cluster", e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    public PolicyList getPolicyList(String fieldStr, String orderBy, String filterBy,
                                    String sortOrder, Integer offset, Integer resultsPerPage, int instanceCount) {
        try {
            return PersistenceHelper.getFilteredPolicy(fieldStr, filterBy, orderBy, sortOrder,
                    offset, resultsPerPage, instanceCount);
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000039.name(), "policy", e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }
    }

    String fetchPolicyStatus(String name) {
        try {
            return PersistenceHelper.getPolicyStatus(name);
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000040.name(), name, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
            LOG.error(MessageCode.MAIN_000041.name(), policyName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }

        return replicationPolicyType;
    }

    PolicyList getPolicyDefinition(String name, boolean isArchived) {
        try {
            return PersistenceHelper.getPolicyDefinitions(name, isArchived);
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000042.name(), name, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
            LOG.error(MessageCode.MAIN_000043.name(), entityName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
                        // For a failed running instance retry is scheduled, in mean time user issues the
                        // policy deletion operation, so move the instance to DELETED state from RUNNING.
                        PersistenceHelper.updateInstanceStatus(policy.getPolicyId());
                        PersistenceHelper.markPolicyInstanceDeleted(instances, retirementTime);
                        PersistenceHelper.deletePolicy(policy.getName(), retirementTime);
                        syncDeletePolicyToRemote(policy);
                    } else {
                        String msg = ((ResourceBundleService) Services.get()
                                .getService(ResourceBundleService.get().getName()))
                                        .getString(MessageCode.MAIN_000011.name(), policy.getName(), policy.getType());
                        LOG.error(msg);
                        throw BeaconWebException.newAPIException(new RuntimeException(msg),
                                Response.Status.BAD_REQUEST);
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
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policy.getName(), tokenList);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000012.name(), policy.getName(),
                policy.getType());
    }

    public APIResult deleteCluster(String clusterName) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            Cluster cluster = ClusterPersistenceHelper.getActiveCluster(clusterName);
            obtainEntityLocks(cluster, "delete", tokenList);
            ClusterPersistenceHelper.unpairAllPairedCluster(cluster);
            ClusterPersistenceHelper.deleteCluster(cluster);
            BeaconEvents.createEvents(Events.DELETED, EventEntityType.CLUSTER);
        } catch (BeaconException e) {
            LOG.error(MessageCode.MAIN_000044.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(clusterName, tokenList);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000012.name(), clusterName, "Cluster");
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
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000025.name(),
                    Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName, e.getMessage());
        } catch (Exception e) {
            throw new BeaconException(MessageCode.MAIN_000002.name(), e, remoteClusterName);
        }
    }

    public APIResult pairClusters(String remoteClusterName, boolean isInternalPairing) {
        String localClusterName = config.getEngine().getLocalClusterName();

        if (localClusterName.equalsIgnoreCase(remoteClusterName)) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000013.name(), remoteClusterName,
                    localClusterName);
        }

        Cluster localCluster;
        try {
            localCluster = ClusterPersistenceHelper.getActiveCluster(localClusterName);
            if (ClusterHelper.areClustersPaired(localCluster, remoteClusterName)) {
                return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000014.name(), localClusterName,
                        remoteClusterName);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            LOG.error(MessageCode.MAIN_000045.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }

        Cluster remoteCluster;
        try {
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000015.name(), Response.Status.NOT_FOUND,
                        localClusterName, remoteClusterName);
            }
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000046.name(), (EntityType.CLUSTER),
                    remoteClusterName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }

        boolean exceptionThrown = true;

        try {
            ClusterPersistenceHelper.pairCluster(localCluster, remoteCluster);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error(MessageCode.MAIN_000045.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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

        return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000016.name());
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void pairClustersInRemote(BeaconClient remoteClient, String remoteClusterName,
                                      String localClusterName, String localBeaconEndpoint) {
        try {
            remoteClient.pairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000025.name(),
                    Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName, e.getMessage());
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000047.name(), e);
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
                return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000017.name(), localClusterName,
                        remoteClusterName);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            LOG.error(MessageCode.MAIN_000048.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }

        Cluster remoteCluster;
        try {
            remoteCluster = ClusterPersistenceHelper.getActiveCluster(remoteClusterName);
            if (remoteCluster == null) {
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000018.name(), Response.Status.NOT_FOUND,
                        localClusterName, remoteClusterName);
            }
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000046.name(), (EntityType.CLUSTER),
                    remoteClusterName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        }

        boolean exceptionThrown = true;

        // Check active policies between the paired clusters.
        checkActivePolicies(localClusterName, remoteClusterName);

        try {
         // Update local cluster with paired information so that it gets pushed to remote
            ClusterPersistenceHelper.unpairPairedCluster(localCluster, remoteCluster);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error(MessageCode.MAIN_000048.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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

        return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000030.name());
    }

    private void checkActivePolicies(String localClusterName, String remoteClusterName) {
        boolean exists = PersistenceHelper.activePairedClusterPolicies(localClusterName, remoteClusterName);
        if (exists) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000019.name(), Response.Status.BAD_REQUEST);
        }
    }

    // TODO : In future when house keeping async is added ignore any errors as this will be retried async
    private void unpairClustersInRemote(BeaconClient remoteClient, String remoteClusterName,
                                        String localClusterName, String localBeaconEndpoint) {
        try {
            remoteClient.unpairClusters(localClusterName, true);
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000025.name(),
                    Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName, e.getMessage());
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000049.name(), e);
            throw e;
        }
    }

    APIResult syncPolicy(String policyName, PropertiesIgnoreCase requestProperties, String id) {
        try {
            ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(requestProperties, policyName);
            policy.setPolicyId(id);
            submitPolicy(policy);
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000020.name(), policyName);
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000050.name(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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
            LOG.error(MessageCode.MAIN_000051.name(), policy.getName(), e);
            scheduleSyncStatus(policy, status, remoteBeaconEndpoint, e);
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000051.name(), policy.getName(), e);
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
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000021.name());
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error(MessageCode.MAIN_000052.name(), policyName, config.getEngine().getLocalClusterName(), e);
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            releaseEntityLocks(policyName, tokenList);
        }
    }

    PolicyInstanceList listPolicyInstance(String policyName, String filters, String orderBy, String sortBy,
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

    PolicyInstanceList listInstance(String filters, String orderBy, String sortBy, Integer offset,
                                    Integer resultsPerPage, boolean isArchived) throws BeaconException {
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = offset >= 0 ? offset : 0;

        try {
            return PersistenceHelper.getFilteredJobInstance(filters, orderBy, sortBy,
                    offset, resultsPerPage, isArchived);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected Integer getDefaultResultsPerPage() {
        return config.getEngine().getResultsPerPage();
    }

    int getMaxInstanceCount() {
        return config.getEngine().getMaxInstanceCount();
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
            throw new BeaconException(MessageCode.MAIN_000004.name(), command, entity.toShortString());
        }

        /* TODO : now obtain locks for all dependent entities if any */
    }

    private void releaseEntityLocks(String entityName, List<Entity> tokenList) {
        if (tokenList != null && !tokenList.isEmpty()) {
            for (Entity entity : tokenList) {
                memoryLocks.releaseLock(entity);
            }
            LOG.info(MessageCode.MAIN_000053.name(), entityName);
        } else {
            LOG.info(MessageCode.MAIN_000054.name(), entityName);
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
            throw new BeaconException(MessageCode.MAIN_000003.name(), e);
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
            BeaconEvents.createEvents(Events.SYNCED, EventEntityType.POLICY,
                    PersistenceHelper.getPolicyBean(policy), getEventInfo(policy, false));
        } catch (BeaconClientException e) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000025.name(),
                    Response.Status.fromStatusCode(e.getStatus()), e, remoteClusterName, e.getMessage());
        } catch (Exception e) {
            LOG.error(MessageCode.MAIN_000055.name(), policy.getSourceCluster(), e);
            throw e;
        }
    }

    private EventInfo getEventInfo(ReplicationPolicy policy, boolean syncEvent) {
        EventInfo eventInfo = new EventInfo();
        eventInfo.updateEventsInfo(policy.getSourceCluster(), policy.getTargetCluster(),
                policy.getSourceDataset(), syncEvent);
        return eventInfo;
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
                                                   String orderBy, String sortBy,
                                                   Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getEventsWithPolicyName(policyName, startDate, endDate, orderBy, sortBy,
                    offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEventsWithName(String eventName, String startStr, String endStr,
                                             String orderBy, String sortBy, Integer offset, Integer resultsPage)
                                             throws BeaconException {
        try {
            Events event = BeaconEventsHelper.validateEventName(eventName);
            if (event == null) {
                throw new BeaconException(MessageCode.MAIN_000022.name(), eventName);
            }

            LOG.info(MessageCode.MAIN_000056.name(), event.getId(), eventName);
            return BeaconEventsHelper.getEventsWithName(event.getId(), startStr, endStr,
                    orderBy, sortBy,  offset, resultsPage);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    protected EventsResult getEntityTypeEvents(String entityType, String startStr, String endStr,
                                               String orderBy, String sortBy,
                                               Integer offset, Integer resultsPage) throws BeaconException {
        try {
            EventEntityType type = BeaconEventsHelper.validateEventEntityType(entityType);
            if (type != null) {
                LOG.info(MessageCode.MAIN_000057.name(), type.getName());
                return BeaconEventsHelper.getEntityTypeEvents(type.getName(), startStr, endStr,
                        orderBy, sortBy, offset, resultsPage);
            } else {
                throw new BeaconException(MessageCode.MAIN_000022.name(), entityType);
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


    protected EventsResult getAllEventsInfo(String startStr, String endStr, String orderBy, String sortBy,
                                        Integer offset, Integer resultsPage) throws BeaconException {
        try {
            return BeaconEventsHelper.getAllEventsInfo(startStr, endStr, orderBy, sortBy, offset, resultsPage);
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
                throw BeaconWebException.newAPIException(MessageCode.MAIN_000023.name(), policyName, status);
            }
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            boolean abortStatus = scheduler.abortInstance(activePolicy.getPolicyId());
            return new APIResult(APIResult.Status.SUCCEEDED, MessageCode.MAIN_000024.name(), abortStatus);
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
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

    ServerVersionResult getServerVersion() {
        ServerVersionResult result = new ServerVersionResult();
        result.setStatus("RUNNING");
        result.setVersion(PersistenceHelper.getServerVersion());
        return result;
    }

    ServerStatusResult getServerStatus() {
        ServerStatusResult result = new ServerStatusResult();
        result.setStatus("RUNNING");
        result.setVersion(PersistenceHelper.getServerVersion());
        result.setWireEncryption(BeaconConfig.getInstance().getEngine().getTlsEnabled());
        result.setSecurity("None");
        List<String> registeredPlugins = PluginManagerService.getRegisteredPlugins();
        if (registeredPlugins.isEmpty()) {
            result.setPlugins("None");
        } else {
            result.setPlugins(StringUtils.join(registeredPlugins, BeaconConstants.COMMA_SEPARATOR));
        }
        return result;
    }
}
