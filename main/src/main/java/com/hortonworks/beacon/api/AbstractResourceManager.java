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
import com.hortonworks.beacon.client.resource.ClusterList.ClusterElement;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.lock.MemoryLocks;
import com.hortonworks.beacon.entity.store.ConfigurationStoreService;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
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
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.PolicyInstanceListExecutor;
import com.hortonworks.beacon.store.result.PolicyInstanceList;
import com.hortonworks.beacon.store.result.PolicyInstanceList.InstanceElement;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A base class for managing Beacon resource operations.
 */
public abstract class AbstractResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractResourceManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    private ConfigurationStoreService configStore = Services.get().getService(ConfigurationStoreService.SERVICE_NAME);
    private BeaconConfig config = BeaconConfig.getInstance();

    protected synchronized APIResult submit(Entity entity) {
        try {
            submitInternal(entity);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + entity.getEntityType() + ") "
                    + entity.getName());
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
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

    private void submitInternal(Entity entity) throws BeaconException {
        EntityType entityType = entity.getEntityType();
        List<Entity> tokenList = new ArrayList<>();

        try {
            obtainEntityLocks(entity, "submit", tokenList);
        } finally {
            configStore.cleanupUpdateInit();
            releaseEntityLocks(entity.getName(), tokenList);
        }

        Entity existingEntity = configStore.getEntity(entityType, entity.getName());
        if (existingEntity != null) {
            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting."
            );
        }

        validate(entity);
        configStore.publish(entityType, entity);
        BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.CLUSTER);
        LOG.info("Submit successful: ({}): {}", entityType, entity.getName());
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

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toUpperCase().split(",")));

        try {
            // getEntity filtered entities
            List<Entity> entities = getFilteredEntities(EntityType.CLUSTER, "");

            String orderByField = null;
            if (StringUtils.isNotEmpty(orderBy)) {
                orderByField = ClusterList.ClusterFieldList.valueOf(orderBy.toUpperCase()).name().toUpperCase();
            }
            // sort entities and pagination
            List<Entity> entitiesReturn = sortEntitiesPagination(
                    entities, orderBy, sortOrder, offset, resultsPerPage, orderByField);

            // add total number of results
            ClusterList entityList = entitiesReturn.size() == 0
                    ? new ClusterList(new Entity[]{}, 0)
                    : new ClusterList(buildClusterElements(new HashSet<>(fields), entitiesReturn), entities.size());
            return entityList;
        } catch (Exception e) {
            LOG.error("Failed to getEntity entity list", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

    }

    public PolicyList getPolicyList(String fieldStr, String orderBy, String filterBy,
                                    String sortOrder, Integer offset, Integer resultsPerPage) {
        try {
            // getEntity filtered entities
            PolicyList entityList = PersistenceHelper.getFilteredPolicy(fieldStr, filterBy, orderBy, sortOrder, offset,
                    resultsPerPage);
            return entityList;
        } catch (Exception e) {
            LOG.error("Failed to getEntity entity list", e);
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

    public String getStatus(String type, String entityName) {

        Entity entity;
        try {
            entity = EntityHelper.getEntity(type, entityName);
            EntityStatus status = getStatus(entity);
            String statusString = status.name();
            LOG.info("Entity name: {}, type: {}, status: {}", entityName, type, statusString);
            return statusString;
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unable to getEntity status for entity {} ({})", entityName, type, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
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
     * Returns the entity definition as an XML based on name.
     *
     * @param type       entity type
     * @param entityName entity name
     * @return String
     */
    public String getEntityDefinition(String type, String entityName) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = configStore.getEntity(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }

            ObjectMapper mapper = new ObjectMapper();
            if (EntityType.REPLICATIONPOLICY == entityType) {
                mapper.setDateFormat(DateUtil.getDateFormat());
            }

            return mapper.writeValueAsString(entity);
        } catch (NoSuchElementException e) {
            LOG.error("Unable to getEntity, entity doesn't exist ({}): {}", type, entityName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Unable to getEntity entity definition from config store for ({}): {}", type, entityName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private void update(Entity entity) throws BeaconException {
        List<Entity> tokenList = new ArrayList<>();
        try {
            configStore.initiateUpdate(entity);
            obtainEntityLocks(entity, "update", tokenList);
            configStore.update(entity.getEntityType(), entity);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Update failed", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            configStore.cleanupUpdateInit();
            releaseEntityLocks(entity.getName(), tokenList);
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

    public APIResult deleteCluster(String type, String entity) {
        if (ClusterHelper.isLocalCluster(entity)) {
            throw BeaconWebException.newAPIException("Local cluster " + entity + " cannot be deleted.");
        }
        return delete(type, entity, false);
    }

    private APIResult delete(String type, String entity, boolean isInternalSyncDelete) {
        EntityType entityType = EntityType.getEnum(type);
        List<Entity> tokenList = new ArrayList<>();

        try {
            Entity entityObj = EntityHelper.getEntity(type, entity);
            canRemove(entityObj);
            obtainEntityLocks(entityObj, "delete", tokenList);
            if (EntityType.CLUSTER == entityType) {
                // If paired with other clusters unpair
                unPair(entity);
            }
            configStore.remove(entityType, entity);
            BeaconEvents.createEvents(Events.DELETED, EventEntityType.CLUSTER);
        } catch (NoSuchElementException e) { // already deleted
            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") doesn't exist. Nothing to do");
        } catch (IOException | BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(entity, tokenList);
        }

        return new APIResult(APIResult.Status.SUCCEEDED,
                entity + "(" + type + ") removed successfully ");
    }

    private void unPair(String clusterName) throws BeaconException {
        String[] peers = ClusterHelper.getPeers(clusterName);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                unPair(peer.trim(), clusterName);
            }
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
            localCluster = EntityHelper.getEntity(EntityType.CLUSTER, localClusterName);
            if (ClusterHelper.areClustersPaired(localClusterName, remoteClusterName)) {
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

        Cluster remoteClusterEntity;
        try {
            remoteClusterEntity = configStore.getEntity(EntityType.CLUSTER, remoteClusterName);
            if (remoteClusterEntity == null) {
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

        String localPairedWith = null;
        String remotePairedWith = null;
        boolean exceptionThrown = true;

        try {
            // Update local cluster with paired information so that it gets pushed to remote
            localPairedWith = localCluster.getPeers();
            ClusterHelper.updatePeers(localCluster, remoteClusterName);

            remotePairedWith = remoteClusterEntity.getPeers();
            ClusterHelper.updatePeers(remoteClusterEntity, localClusterName);

            update(localCluster);
            update(remoteClusterEntity);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (exceptionThrown) {
                revertPairingorUnpairingLocally(localCluster, remoteClusterEntity, localPairedWith, remotePairedWith);
            }
        }

        /* Call pair remote only if pairing locally succeeds else we need to rollback pairing in remote
         */
        if (!isInternalPairing) {
            exceptionThrown = true;
            BeaconClient remoteClient = new BeaconClient(remoteClusterEntity.getBeaconEndpoint());
            try {
                pairClustersInRemote(remoteClient, remoteClusterName, localClusterName,
                        localCluster.getBeaconEndpoint());
                exceptionThrown = false;
            } finally {
                if (exceptionThrown) {
                    revertPairingorUnpairingLocally(localCluster, remoteClusterEntity,
                            localPairedWith, remotePairedWith);
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

    private void revertPairingorUnpairingLocally(Cluster localCluster, Cluster remoteClusterEntity,
                                                 String localPairedWith, String remotePairedWith) {
        // Reset peers in config store
        ClusterHelper.resetPeers(localCluster, localPairedWith);
        ClusterHelper.resetPeers(remoteClusterEntity, remotePairedWith);

        try {
            update(localCluster);
            update(remoteClusterEntity);
        } catch (BeaconException e) {
            // Ignore exceptions for cleanup
            LOG.error("Exception while reverting pairing locally: {}", e.getMessage());
        }
    }

    public APIResult unpairClusters(String remoteClusterName,
                                    boolean isInternalUnpairing) {
        String localClusterName = config.getEngine().getLocalClusterName();
        Cluster localCluster;
        try {
            localCluster = EntityHelper.getEntity(EntityType.CLUSTER, localClusterName);
            if (!ClusterHelper.areClustersPaired(localClusterName, remoteClusterName)) {
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

        Cluster remoteClusterEntity;
        try {
            remoteClusterEntity = configStore.getEntity(EntityType.CLUSTER, remoteClusterName);
            if (remoteClusterEntity == null) {
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

        String localPairedWith = null;
        String remotePairedWith = null;
        boolean exceptionThrown = true;

        try {
            // Update local cluster with paired information so that it gets pushed to remote
            localPairedWith = localCluster.getPeers();
            unPair(localClusterName, remoteClusterName);

            remotePairedWith = remoteClusterEntity.getPeers();
            unPair(remoteClusterName, localClusterName);
            exceptionThrown = false;
        } catch (RuntimeException | BeaconException e) {
            LOG.error("Unable to unpair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (exceptionThrown) {
                revertPairingorUnpairingLocally(localCluster, remoteClusterEntity, localPairedWith, remotePairedWith);
            }
        }

        /* Call pair remote only if pairing locally succeeds else we need to rollback pairing in remote
         */
        if (!isInternalUnpairing) {
            exceptionThrown = true;
            BeaconClient remoteClient = new BeaconClient(remoteClusterEntity.getBeaconEndpoint());
            try {
                unpairClustersInRemote(remoteClient, remoteClusterName, localClusterName,
                        localCluster.getBeaconEndpoint());
                exceptionThrown = false;
            } finally {
                if (exceptionThrown) {
                    revertPairingorUnpairingLocally(localCluster, remoteClusterEntity,
                            localPairedWith, remotePairedWith);
                }
            }
        }

        return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully unpaired");
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

    private void unPair(String clusterName, String clusterTobeUnpaired) throws BeaconException {
        String[] peers = ClusterHelper.getPeers(clusterName);
        StringBuilder newPeers = new StringBuilder();
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(clusterTobeUnpaired)) {
                    continue;
                }
                if (StringUtils.isBlank(newPeers)) {
                    newPeers.append(peer);
                } else {
                    newPeers.append(BeaconConstants.COMMA_SEPARATOR).append(peer);
                }
            }
            Cluster cluster = EntityHelper.getEntity(EntityType.CLUSTER, clusterName);
            if (StringUtils.isBlank(newPeers)) {
                ClusterHelper.resetPeers(cluster, null);
            } else {
                ClusterHelper.resetPeers(cluster, newPeers.toString());
            }
            update(cluster);
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
            List<InstanceElement> elements = executor.getFilteredJobInstance(filters,
                    orderBy, sortBy, offset, resultsPerPage);
            return new PolicyInstanceList(elements);
        } catch (Exception e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    private List<Entity> getFilteredEntities(final EntityType entityType,
                                             String filterBy) throws BeaconException, IOException {
        Collection<String> entityNames = configStore.getEntities(entityType);
        if (entityNames.isEmpty()) {
            return Collections.emptyList();
        }

        List<Entity> entities = new ArrayList<Entity>();

        for (String entityName : entityNames) {
            Entity entity;
            try {
                entity = configStore.getEntity(entityType, entityName);
                if (entity == null) {
                    continue;
                }
            } catch (BeaconException e1) {
                LOG.error("Unable to getEntity list for entities for ({})",
                        entityType.getEntityClass().getSimpleName(), e1);
                throw BeaconWebException.newAPIException(e1, Response.Status.INTERNAL_SERVER_ERROR);
            }
            entities.add(entity);
        }

        return entities;
    }

    private List<Entity> sortEntitiesPagination(List<Entity> entities, String orderBy, String sortOrder,
                                                Integer offset, Integer resultsPerPage, String orderByField) {
        // sort entities
        entities = sortEntities(entities, orderBy, sortOrder, orderByField);

        // pagination
        int pageCount = getRequiredNumberOfResults(entities.size(), offset, resultsPerPage);
        List<Entity> entitiesReturn = new ArrayList<Entity>();
        if (pageCount > 0) {
            entitiesReturn.addAll(entities.subList(offset, (offset + pageCount)));
        }

        return entitiesReturn;
    }

    private int getRequiredNumberOfResults(int arraySize, int offset, int numresults) {
        /* Get a subset of elements based on offset and count. When returning subset of elements,
              elements[offset] is included. Size 10, offset 10, return empty list.
              Size 10, offset 5, count 3, return elements[5,6,7].
              Size 10, offset 5, count >= 5, return elements[5,6,7,8,9]
              return elements starting from elements[offset] until the end OR offset+numResults*/

        if (numresults < 1) {
            LOG.error("Value for param numResults should be > than 0  : {}", numresults);
            throw BeaconWebException.newAPIException("Value for param numResults should be > than 0  : " + numresults);
        }

        if (offset < 0) {
            offset = 0;
        }

        if (offset >= arraySize || arraySize == 0) {
            // No elements to return
            return 0;
        }

        int retLen = arraySize - offset;
        if (retLen > numresults) {
            retLen = numresults;
        }
        return retLen;
    }

    private List<Entity> sortEntities(List<Entity> entities, String orderBy, String sortOrder, String orderByField) {
        // Sort the ArrayList using orderBy param
        if (!entities.isEmpty() && StringUtils.isNotEmpty(orderBy)) {
            final String order = getValidSortOrder(sortOrder, orderBy);
            switch (orderByField) {

                case "NAME":
                    Collections.sort(entities, new Comparator<Entity>() {
                        @Override
                        public int compare(Entity e1, Entity e2) {
                            return (order.equalsIgnoreCase("asc")) ? e1.getName().compareTo(e2.getName())
                                    : e2.getName().compareTo(e1.getName());
                        }
                    });
                    break;

                default:
                    break;
            }
        } // else no sort

        return entities;
    }

    private String getValidSortOrder(String sortOrder, String orderBy) {
        if (StringUtils.isEmpty(sortOrder)) {
            return (orderBy.equalsIgnoreCase("starttime")
                    || orderBy.equalsIgnoreCase("endtime")) ? "desc" : "asc";
        }

        if (sortOrder.equalsIgnoreCase("asc") || sortOrder.equalsIgnoreCase("desc")) {
            return sortOrder;
        }

        String err = "Value for param sortOrder should be \"asc\" or \"desc\". It is  : " + sortOrder;
        LOG.error(err);
        throw BeaconWebException.newAPIException(err);
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

    private ClusterElement[] buildClusterElements(HashSet<String> fields, List<Entity> entities) {
        ClusterElement[] elements = new ClusterElement[entities.size()];
        int elementIndex = 0;
        for (Entity entity : entities) {
            elements[elementIndex++] = getClusterElement(entity, fields);
        }
        return elements;
    }

    private ClusterElement getClusterElement(Entity entity, HashSet<String> fields) {
        ClusterElement elem = new ClusterElement();
        elem.name = entity.getName();

        Cluster cluster = (Cluster) entity;

        if (fields.contains(ClusterList.ClusterFieldList.PEERS.name())) {
            elem.peer = getPeers(cluster.getPeers());
        }

        if (fields.contains(ClusterList.ClusterFieldList.TAGS.name())) {
            elem.tag = EntityHelper.getTags(entity);
        }

        return elem;
    }

    private List<String> getPeers(String peers) {
        List<String> peerList = new ArrayList<>();

        if (!StringUtils.isEmpty(peers)) {
            for (String peer : peers.split(",")) {
                peerList.add(peer.trim());
            }
        }
        return peerList;
    }


    private static EntityStatus getStatus(final Entity entity) {
        return EntityStatus.SUBMITTED;
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

    private static void canRemove(final Entity entity) throws BeaconException {
        /* TODO : Add logic to see if cluster or the entity is referenced by any other entities using quartz DB
        references */
//        throw new BeaconException(
//                entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
//                        + messages);
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
            if (jobList != null) {
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
}
