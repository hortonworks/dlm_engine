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
import com.hortonworks.beacon.client.resource.PolicyList.PolicyElement;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.JobBuilder;
import com.hortonworks.beacon.entity.PolicyJobBuilderFactory;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.lock.MemoryLocks;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.scheduler.BeaconQuartzScheduler;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.store.bean.PolicyInfoBean;
import com.hortonworks.beacon.store.executors.PolicyInfoExecutor;
import com.hortonworks.beacon.store.executors.PolicyInfoExecutor.PolicyInfoQuery;
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
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;


public abstract class AbstractResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractResourceManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    private ConfigurationStore configStore = ConfigurationStore.getInstance();
    private BeaconConfig config = BeaconConfig.getInstance();

    protected synchronized APIResult submit(Entity entity) {
        try {
            submitInternal(entity);
            boolean isSchedulable = entity.getEntityType().isSchedulable();
            if (isSchedulable) {
                ReplicationPolicy policy = (ReplicationPolicy) entity;
                updateStatus(policy.getName(), policy.getType(), EntityStatus.SUBMITTED.name(), isSchedulable);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + entity.getEntityType() + ") " +
                    entity.getName());
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private void submitInternal(Entity entity) throws BeaconException {
        EntityType entityType = entity.getEntityType();
        List<Entity> tokenList = new ArrayList<>();

        try {
            obtainEntityLocks(entity, "submit", tokenList);
        } finally {
            ConfigurationStore.getInstance().cleanupUpdateInit();
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
        LOG.info("Submit successful: ({}): {}", entityType, entity.getName());
    }


    protected synchronized void schedule(String type, String entityName) {
        /* TODO : Update the policy with start time in Quartz DB */
        Entity entityObj;
        List<Entity> tokenList = new ArrayList<>();
        try {
            checkSchedulableEntity(type);
            entityObj = EntityHelper.getEntity(type, entityName);
            EntityStatus status = getStatus(entityObj);
            if (status.equals(EntityStatus.SUBMITTED)) {
                ReplicationPolicy policy = (ReplicationPolicy) entityObj;
                obtainEntityLocks(entityObj, "schedule", tokenList);
                JobBuilder jobBuilder = PolicyJobBuilderFactory.getJobBuilder(policy);
                ReplicationJobDetails job = jobBuilder.buildJob(policy);
                BeaconScheduler scheduler = BeaconQuartzScheduler.get();
                scheduler.scheduleJob(job, false);
                updateStatus(policy.getName(), policy.getType(), EntityStatus.RUNNING.name(),
                        EntityType.getEnum(type).isSchedulable());
                LOG.info("scheduled policy type : {}", policy.getType());
            } else {
                throw BeaconWebException.newAPIException(entityName + "(" + type + ") is cannot be scheduled. Current " +
                        "status: " + status.name());
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Entity schedule failed for " + type + ": " + entityName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(entityName, tokenList);
        }
    }

    protected APIResult submitAndSchedule(Entity entity) {
        try {
            final String type = entity.getEntityType().name();
            checkSchedulableEntity(type);
            submit(entity);
            schedule(type, entity.getName());
            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity.getName() + "(" + type + ") scheduled successfully");
        } catch (Throwable e) {
            LOG.error("Unable to submit and schedule ", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Suspends a running entity.
     *
     * @param entityType entity type
     * @param entityName entity name
     * @return APIResult
     */
    public APIResult suspend(String entityType, String entityName) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            checkSchedulableEntity(entityType);
            Entity entityObj = EntityHelper.getEntity(entityType, entityName);
            obtainEntityLocks(entityObj, "suspend", tokenList);

            ReplicationPolicy policy = (ReplicationPolicy) entityObj;
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            String policyStatus = scheduler.getPolicyStatus(policy.getName(), policy.getType());
            if (policyStatus.equalsIgnoreCase(EntityStatus.RUNNING.name())) {
                scheduler.suspendJob(policy.getName(), policy.getType());
                updateStatus(policy.getName(), policy.getType(), EntityStatus.SUSPENDED.name(),
                        EntityType.getEnum(entityType).isSchedulable());
                LOG.info("Suspended successfully: ({}): {}", entityType, entityName);
            } else {
                throw BeaconWebException.newAPIException(entityName + "(" + entityType + ") is cannot be suspended. Current " +
                        "status: " + policyStatus);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entityName + "(" + entityType + ") suspended successfully");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Throwable e) {
            LOG.error("Unable to suspend entity", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(entityName, tokenList);
        }
    }

    /**
     * Resumes a suspended entity.
     *
     * @param entityType entity type
     * @param entityName entity name
     * @return APIResult
     */
    public APIResult resume(String entityType, String entityName) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            checkSchedulableEntity(entityType);
            Entity entityObj = EntityHelper.getEntity(entityType, entityName);
            obtainEntityLocks(entityObj, "resume", tokenList);

            ReplicationPolicy policy = (ReplicationPolicy) entityObj;
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            String policyStatus = scheduler.getPolicyStatus(policy.getName(), policy.getType());
            if (policyStatus.equalsIgnoreCase(EntityStatus.SUSPENDED.name())) {
                scheduler.resumeJob(policy.getName(), policy.getType());
                updateStatus(policy.getName(), policy.getType(), EntityStatus.RUNNING.name(),
                        EntityType.getEnum(entityType).isSchedulable());
                LOG.info("Resumed successfully: ({}): {}", entityType, entityName);
            } else {
                throw new IllegalStateException(entityName + "(" + entityType + ") is cannot resumed. Current status: " + policyStatus);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entityName + "(" + entityType + ") resumed successfully");
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Exception e) {
            LOG.error("Unable to resume entity", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(entityName, tokenList);
        }
    }

    public ClusterList getClusterList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                      Integer resultsPerPage) {

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toUpperCase().split(",")));

        try {
            // getEntity filtered entities
            List<Entity> entities = getFilteredEntities(EntityType.CLUSTER);

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

    public PolicyList getPolicyList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                    Integer resultsPerPage) {

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toUpperCase().split(",")));

        try {
            // getEntity filtered entities
            List<Entity> entities = getFilteredEntities(EntityType.REPLICATIONPOLICY);

            String orderByField = null;
            if (StringUtils.isNotEmpty(orderBy)) {
                orderByField = PolicyList.PolicyFieldList.valueOf(orderBy.toUpperCase()).name().toUpperCase();
            }
            // sort entities and pagination
            List<Entity> entitiesReturn = sortEntitiesPagination(
                    entities, orderBy, sortOrder, offset, resultsPerPage, orderByField);

            // add total number of results
            PolicyList entityList = entitiesReturn.size() == 0
                    ? new PolicyList(new Entity[]{}, 0)
                    : new PolicyList(buildPolicyElements(new HashSet<>(fields), entitiesReturn), entities.size());
            return entityList;
        } catch (Exception e) {
            LOG.error("Failed to getEntity entity list", e);
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
            ConfigurationStore.getInstance().cleanupUpdateInit();
            releaseEntityLocks(entity.getName(), tokenList);
        }

    }

    public APIResult delete(String type, String entity) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            EntityType entityType = EntityType.getEnum(type);
            try {
                Entity entityObj = EntityHelper.getEntity(type, entity);

                canRemove(entityObj);
                obtainEntityLocks(entityObj, "delete", tokenList);
                EntityStatus status = getStatus(entityObj);
                if (entityType.isSchedulable() && !status.equals(EntityStatus.SUBMITTED)) {
                    ReplicationPolicy policy = (ReplicationPolicy) entityObj;
                    BeaconScheduler scheduler = BeaconQuartzScheduler.get();
                    scheduler.deleteJob(policy.getName(), policy.getType());
                }
                deleteStatus(entity, entityType.isSchedulable());
                configStore.remove(entityType, entity);
            } catch (NoSuchElementException e) { // already deleted
                return new APIResult(APIResult.Status.SUCCEEDED,
                        entity + "(" + type + ") doesn't exist. Nothing to do");
            }

            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully ");
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or deletion failed", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            releaseEntityLocks(entity, tokenList);
        }
    }

    public APIResult pairCusters(String remoteBeaconEndpoint, String remoteClusterName) {
        // What happens when beacon endpoint changes - need a way to update in properties

        String localClusterName = config.getEngine().getLocalClusterName();
        Cluster localCluster;
        try {
            localCluster = EntityHelper.getEntity(EntityType.CLUSTER, localClusterName);
            if (ClusterHelper.areClustersPaired(localClusterName, remoteClusterName)) {
                String status = "Cluster " + localClusterName + " has already been paired with " +
                        remoteClusterName;
                return new APIResult(APIResult.Status.SUCCEEDED, status);
            }
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (BeaconException e) {
            LOG.error("Unable to pair the clusters", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        BeaconClient remoteClient = new BeaconClient(remoteBeaconEndpoint);
        boolean remoteClusterSynced = false;

        Cluster remoteClusterEntity;
        try {
            remoteClusterEntity = configStore.getEntity(EntityType.CLUSTER, remoteClusterName);
            if (remoteClusterEntity != null) {
                remoteClusterSynced = true;
            }
        } catch (Throwable e) {
            LOG.error("Unable to getEntity entity definition from config store for ({}): {}", (EntityType.CLUSTER),
                    remoteClusterName, e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }


        String remoteClusterDefinition;
        if (!remoteClusterSynced) {
            boolean exceptionThrown = true;
            try {
                /* Cannot ignore this error as remote cluster definition is required for pair and any subsequent
                   action to succeed. */
                remoteClusterDefinition = getRemoteClusterDefinition(remoteClient, remoteClusterName);
                remoteClusterEntity = ClusterBuilder.constructCluster(remoteClusterDefinition);
                submitInternal(remoteClusterEntity);
                exceptionThrown = false;
            } catch (RuntimeException | BeaconException e) {
                String message = "Unable to get the remote cluster: " + remoteClusterName;
                LOG.error(message, e);
                throw BeaconWebException.newAPIException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
            } finally {
                if (exceptionThrown) {
                    cleanupAfterPairClusterFailure(remoteClusterSynced,
                            remoteClusterName, localClusterName, remoteClient);
                }
            }
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
                // Reset peers in config store
                localCluster.setPeers(localPairedWith);
                remoteClusterEntity.setPeers(remotePairedWith);
                cleanupAfterPairClusterFailure(remoteClusterSynced,
                        remoteClusterName, localClusterName, remoteClient);
            }
        }


        syncLocalClusterToRemote(remoteClient, localClusterName, localCluster);
        return new APIResult(APIResult.Status.SUCCEEDED, "Clusters successfully paired");
    }

    // Can't be persisted to local variable as its remote call and can fail
    private boolean isLocalClusterSyncedToRemote(BeaconClient remoteClient, String clusterName) {
        boolean localClusterSynced = false;
        try {
            remoteClient.getCluster(clusterName);
            localClusterSynced = true;
        } catch (BeaconClientException e) {
            if (Response.Status.NOT_FOUND.getStatusCode() != e.getStatus()) {
                throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
        return localClusterSynced;
    }

    private String getRemoteClusterDefinition(BeaconClient remoteClient, String remoteClusterName) {
        return remoteClient.getCluster(remoteClusterName);
    }

    // TODO: Ignore any errors as this will be retries async
    private void syncLocalClusterToRemote(BeaconClient remoteClient,
                                          String localClusterName, Cluster localCluster) {
        try {
            boolean localClusterSynced = isLocalClusterSyncedToRemote(remoteClient, localClusterName);
            if (localClusterSynced) {
                // delete to send the updated paired information
                remoteClient.deleteCluster(localClusterName);
            }

            // Send local cluster definition to remote cluster
            remoteClient.syncCluster(localClusterName, localCluster.toString());
        } catch (Exception e) {
            // Don't rethrow the error as sync is tried async housekeeping service later
            LOG.error("Exception while syncing local cluster to remote: {}", e);
        }
    }


    private void cleanupAfterPairClusterFailure(boolean remoteClusterSynced, String remoteClusterName, String localClusterName,
                                                BeaconClient remoteClient) {
        try {
            // Do cleanup
            if (!remoteClusterSynced) {
                delete(EntityType.CLUSTER.name(), remoteClusterName);
            }

            // Cannot save this in local variable as its remote call and can fail which doesn't ensure correctness
            boolean localClusterSynced = isLocalClusterSyncedToRemote(remoteClient, localClusterName);
            if (!localClusterSynced) {
                remoteClient.deleteCluster(localClusterName);
            }
        } catch (Exception e) {
            // Don't rethrow the cleanup exception
            LOG.error("Exception during cleanup: {}", e);
        }
    }

    public APIResult syncCuster(String clusterName, Properties requestProperties) {
        try {
            submitInternal(ClusterBuilder.buildCluster(requestProperties));
            String localClusterName = config.getEngine().getLocalClusterName();
            Cluster localCluster = EntityHelper.getEntity(EntityType.CLUSTER, localClusterName);
            ClusterHelper.updatePeers(localCluster, clusterName);
            update(localCluster);
            return new APIResult(APIResult.Status.SUCCEEDED, "Sync cluster successful (" + clusterName + ") ");
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to sync the cluster", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public APIResult syncPolicy(String policyName, Properties requestProperties) {
        try {
            submitInternal(ReplicationPolicyBuilder.buildPolicy(requestProperties));
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit and Sync policy successful (" + policyName + ") ");
        } catch (ValidationException | EntityAlreadyExistsException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            LOG.error("Unable to sync the policy", e);
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public JobInstanceList listInstance(String entityName, String status, String startTime, String endTime,
                                        String orderBy, String sortOrder, Integer offset, Integer resultsPerPage)
            throws BeaconException {
        ConfigurationStore store = ConfigurationStore.getInstance();
        ReplicationPolicy policy = store.getEntity(EntityType.REPLICATIONPOLICY, entityName);
        if (policy != null) {
            // TODO process status and other query parameters
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            List<JobInstanceBean> instances = scheduler.listJob(entityName, policy.getType());
            return new JobInstanceList(instances);
        } else {
            throw new NoSuchElementException(entityName + " policy not found.");
        }
    }

    private List<Entity> getFilteredEntities(final EntityType entityType)
            throws BeaconException, IOException {
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
                LOG.error("Unable to getEntity list for entities for ({})", entityType.getEntityClass().getSimpleName(), e1);
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

    private void obtainEntityLocks(Entity entity, String command, List<Entity> tokenList)
            throws BeaconException {
        //first obtain lock for the entity for which update is issued.
        if (memoryLocks.acquireLock(entity, command)) {
            tokenList.add(entity);
        } else {
            throw new BeaconException(command + " command is already issued for " + entity.toShortString());
        }

        /* TODO: */
        //now obtain locks for all dependent entities if any.

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
        elem.dataCenter = cluster.getDataCenter();

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

    private PolicyElement[] buildPolicyElements(HashSet<String> fields, List<Entity> entities) {
        PolicyElement[] elements = new PolicyElement[entities.size()];
        int elementIndex = 0;
        for (Entity entity : entities) {
            elements[elementIndex++] = getPolicyElement(entity, fields);
        }
        return elements;
    }

    private PolicyElement getPolicyElement(Entity entity, HashSet<String> fields) {
        PolicyElement elem = new PolicyElement();
        elem.name = entity.getName();

        ReplicationPolicy policy = (ReplicationPolicy) entity;
        elem.type = policy.getType();
        if (fields.contains(PolicyList.PolicyFieldList.STATUS.name())) {
            elem.status = getStatus(entity).name();
        }
        if (fields.contains(PolicyList.PolicyFieldList.FREQUENCY.name())) {
            elem.frequency = policy.getFrequencyInSec();
        }
        if (fields.contains(PolicyList.PolicyFieldList.STARTTIME.name())) {
            elem.startTime = DateUtil.formatDate(policy.getStartTime());
        }
        if (fields.contains(PolicyList.PolicyFieldList.ENDTIME.name())) {
            elem.endTime = DateUtil.formatDate(policy.getEndTime());
        }
        if (fields.contains(PolicyList.PolicyFieldList.TAGS.name())) {
            elem.tag = EntityHelper.getTags(entity);
        }
        if (fields.contains(PolicyList.PolicyFieldList.CLUSTERS.name())) {
            elem.sourceCluster = new ArrayList<>(Arrays.asList(policy.getSourceCluster()));
            elem.targetCluster = new ArrayList<>(Arrays.asList(policy.getTargetCluster()));
        }
        return elem;
    }

    private static EntityStatus getStatus(final Entity entity) {
        EntityStatus status = EntityStatus.SUBMITTED;
        EntityType type = entity.getEntityType();
        String statusString;
        if (type.isSchedulable()) {
            ReplicationPolicy policy = (ReplicationPolicy) entity;
            BeaconScheduler scheduler = BeaconQuartzScheduler.get();
            statusString = scheduler.getPolicyStatus(policy.getName(), policy.getType());
            status = EntityStatus.valueOf(statusString);
        }
        return status;
    }

    private static void canRemove(final Entity entity) throws BeaconException {
        /* TODO : Add logic to see if cluster or the entity is referenced by any other entities using quartz DB
        references */
//        throw new BeaconException(
//                entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
//                        + messages);
    }

    private void checkSchedulableEntity(String type) throws BeaconException {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new BeaconException(
                    "Entity type (" + type + ") " + " cannot be Scheduled/Suspended/Resumed");
        }
    }

    // Catch all exceptions as sync to remote will be reattempted by async housekeeping service
    public void syncPolicyInRemote(String policyName) {
        String localClusterName = config.getEngine().getLocalClusterName();
        String remoteClusterName;
        ReplicationPolicy policy;
        String remoteBeaconEndpoint;

        try {
            policy = EntityHelper.getEntity(EntityType.REPLICATIONPOLICY, policyName);
            remoteClusterName = policy.getSourceCluster().equalsIgnoreCase(localClusterName)
                    ? policy.getTargetCluster() : policy.getSourceCluster();
            Cluster remoteCluster = EntityHelper.getEntity(EntityType.CLUSTER, remoteClusterName);
            remoteBeaconEndpoint = remoteCluster.getBeaconEndpoint();

            BeaconClient remoteClient = new BeaconClient(remoteBeaconEndpoint);
            remoteClient.syncPolicy(policyName, policy.toString());

        } catch (NoSuchElementException e) {
            LOG.error("Policy not found: {}", e);
        } catch (Exception e) {
            LOG.error("Error when syncing remote policy: {}", e);
        }
    }

    private void updateStatus(String name, String type, String status, boolean isSchedulable) {
        if (!isSchedulable) {
            return;
        }
        type = ReplicationType.valueOf(type).getName();
        PolicyInfoBean bean = new PolicyInfoBean();
        bean.setName(name);
        bean.setType(type);
        bean.setStatus(status);
        bean.setLastModified(System.currentTimeMillis());
        PolicyInfoExecutor executor = new PolicyInfoExecutor(bean);
        if (EntityStatus.SUBMITTED.name().equalsIgnoreCase(status)) {
            executor.execute();
        } else {
            executor.executeUpdate(PolicyInfoQuery.UPDATE_STATUS);
        }
    }

    private void deleteStatus(String name, boolean isSchedulable) {
        if (!isSchedulable) {
            return;
        }
        PolicyInfoBean policyInfoBean = new PolicyInfoBean();
        policyInfoBean.setName(name);
        PolicyInfoExecutor policyInfoExecutor = new PolicyInfoExecutor(policyInfoBean);
        policyInfoExecutor.executeUpdate(PolicyInfoQuery.DELETE_RECORD);
    }
}
