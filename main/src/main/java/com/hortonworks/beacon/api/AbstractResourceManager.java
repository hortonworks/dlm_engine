package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.result.APIResult;
import com.hortonworks.beacon.api.result.APIResult.Status;
import com.hortonworks.beacon.api.result.EntityList;
import com.hortonworks.beacon.api.result.EntityList.EntityElement;
import com.hortonworks.beacon.entity.Entity;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.entity.EntityValidator;
import com.hortonworks.beacon.entity.EntityValidatorFactory;
import com.hortonworks.beacon.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.EntityNotRegisteredException;
import com.hortonworks.beacon.entity.lock.MemoryLocks;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.entity.util.DateUtil;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.config.BeaconConfig;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;


public abstract class AbstractResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractResourceManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    private ConfigurationStore configStore = ConfigurationStore.get();

    /**
     * Enumeration of all possible status of an entity.
     */
    public enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING, COMPLETED
    }

    protected synchronized APIResult submit(Entity entity) {
        EntityType entityType = entity.getEntityType();
        List<Entity> tokenList = new ArrayList<>();

        try {
            try {
                obtainEntityLocks(entity, "submit", tokenList);
            } finally {
                ConfigurationStore.get().cleanupUpdateInit();
                releaseEntityLocks(entity.getName(), tokenList);
            }

            Entity existingEntity = configStore.get(entityType, entity.getName());
            if (existingEntity != null) {
                throw new EntityAlreadyExistsException(
                        entity.toShortString() + " already registered with configuration store. "
                                + "Can't be submitted again. Try removing before submitting."
                );
            }

            validate(entity);
            configStore.publish(entityType, entity);
            LOG.info("Submit successful: ({}): {}", entityType, entity.getName());
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + entity.getEntityType() + ") " +
                    entity.getName());
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw BeaconWebException.newAPIException(e);
        }
    }


    protected synchronized void schedule(String type, String entityName) throws BeaconException {
        checkSchedulableEntity(type);

        Entity entity = EntityHelper.getEntity(type, entityName);
        ReplicationPolicy policy = (ReplicationPolicy) entity;
        if (policy.getStartTime() == null) {
            // Update start time as now
            Date now = new Date();
            DateFormat isoFormat = DateUtil.getDateFormat();
            String strDate = isoFormat.format(now);
            Date date = null;
            try {
                date = isoFormat.parse(strDate);
            } catch (ParseException e) {
                throw new BeaconException(e);
            }
            policy.setStartTime(date);
            // Update in config store
            update(entity);
        }
        /* TODO : Update the policy with start time in Quartz DB */


        Entity entityObj = null;
        try {
            entityObj = EntityHelper.getEntity(type, entityName);

            //first acquire lock on entity before scheduling
            if (!memoryLocks.acquireLock(entityObj, "schedule")) {
                throw BeaconWebException.newAPIException("Looks like an schedule/update command is already"
                        + " running for " + entityObj.toShortString());
            }
            LOG.info("Memory lock obtained for {} by {}", entityObj.toShortString(), Thread.currentThread().getName());
            /* TODO : Scheduled using quartz */
        } catch (Throwable e) {
            LOG.error("Entity schedule failed for " + type + ": " + entityName, e);
            throw BeaconWebException.newAPIException(e);
        } finally {
            if (entityObj != null) {
                memoryLocks.releaseLock(entityObj);
                LOG.info("Memory lock released for {}", entityObj.toShortString());
            }
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
            throw BeaconWebException.newAPIException(e);
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
        try {
            checkSchedulableEntity(entityType);
            Entity entityObj = EntityHelper.getEntity(entityType, entityName);

            /* TODO if active in quartz supend all its instances */
            boolean active = true;
            if (active) {

            } else {
                throw BeaconWebException.newAPIException(entityName + "(" + entityType + ") is not scheduled");
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entityName + "(" + entityType + ") suspended successfully");
        } catch (Throwable e) {
            LOG.error("Unable to suspend entity", e);
            throw BeaconWebException.newAPIException(e);
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
        try {
            checkSchedulableEntity(entityType);
            Entity entityObj = EntityHelper.getEntity(entityType, entityName);

            /* TODO if suspended in quartz resume all its instances */
            boolean active = false;
            if (active) {

            } else {
                throw new IllegalStateException(entityName + "(" + entityType + ") is not scheduled");
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entityName + "(" + entityType + ") resumed successfully");
        } catch (Exception e) {
            LOG.error("Unable to resume entity", e);
            throw BeaconWebException.newAPIException(e);
        }
    }

    public EntityList getEntityList(String fieldStr, String orderBy, String sortOrder, Integer offset,
                                    Integer resultsPerPage, EntityType enityType) {

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toUpperCase().split(",")));

        try {
            // get filtered entities
            List<Entity> entities = getFilteredEntities(enityType);

            // sort entities and pagination
            List<Entity> entitiesReturn = sortEntitiesPagination(
                    entities, orderBy, sortOrder, offset, resultsPerPage);

            // add total number of results
            EntityList entityList = entitiesReturn.size() == 0
                    ? new EntityList(new Entity[]{}, 0)
                    : new EntityList(buildEntityElements(new HashSet<>(fields), entitiesReturn), entities.size());
            return entityList;
        } catch (Exception e) {
            LOG.error("Failed to get entity list", e);
            throw BeaconWebException.newAPIException(e);
        }

    }

    public APIResult getStatus(String type, String entityName) {

        Entity entity;
        try {
            entity = EntityHelper.getEntity(type, entityName);
            EntityStatus status = getStatus(entity);
            String statusString = status.name();

            return new APIResult(Status.SUCCEEDED, statusString);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unable to get status for entity {} ({})", entityName, type, e);
            throw BeaconWebException.newAPIException(e);
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
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }

            ObjectMapper mapper = new ObjectMapper();
            if (EntityType.REPLICATIONPOLICY == entityType) {
                mapper.setDateFormat(DateUtil.getDateFormat());
            }

            return mapper.writeValueAsString(entity);
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config store for ({}): {}", type, entityName, e);
            throw BeaconWebException.newAPIException(e);
        }
    }

    private void update(Entity entity) throws BeaconException {
        if (EntityType.CLUSTER == entity.getEntityType()) {
            throw new BeaconException("Update operation not support for " + EntityType.CLUSTER);
        }
        List<Entity> tokenList = new ArrayList<>();
        try {
            configStore.initiateUpdate(entity);
            obtainEntityLocks(entity, "update", tokenList);
            configStore.update(entity.getEntityType(), entity);
        } catch (Throwable e) {
            LOG.error("Update failed", e);
            throw BeaconWebException.newAPIException(e);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
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
                if (entityType.isSchedulable()) {
                    /*TODO : Remove from quartz DB */
                }
                configStore.remove(entityType, entity);
            } catch (EntityNotRegisteredException e) { // already deleted
                return new APIResult(APIResult.Status.SUCCEEDED,
                        entity + "(" + type + ") doesn't exist. Nothing to do");
            }

            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully ");
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or deletion failed", e);
            throw BeaconWebException.newAPIException(e);
        } finally {
            releaseEntityLocks(entity, tokenList);
        }
    }

    public APIResult pairCusters(final String remoteBeaconEndpoint) {
        /* TODO : Logic */
        return new APIResult(APIResult.Status.SUCCEEDED,
                "Clusters successfully paired");
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
                entity = configStore.get(entityType, entityName);
                if (entity == null) {
                    continue;
                }
            } catch (BeaconException e1) {
                LOG.error("Unable to get list for entities for ({})", entityType.getEntityClass().getSimpleName(), e1);
                throw BeaconWebException.newAPIException(e1);
            }
            entities.add(entity);
        }

        return entities;
    }

    private List<Entity> sortEntitiesPagination(List<Entity> entities, String orderBy, String sortOrder,
                                                Integer offset, Integer resultsPerPage) {
        // sort entities
        entities = sortEntities(entities, orderBy, sortOrder);

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

    private List<Entity> sortEntities(List<Entity> entities, String orderBy, String sortOrder) {
        // Sort the ArrayList using orderBy param
        if (!entities.isEmpty() && StringUtils.isNotEmpty(orderBy)) {
            EntityList.EntityFieldList orderByField = EntityList.EntityFieldList.valueOf(orderBy.toUpperCase());
            final String order = getValidSortOrder(sortOrder, orderBy);
            switch (orderByField) {

                case NAME:
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


    protected static Integer getDefaultResultsPerPage() {
        Integer result = 10;
        final String key = "webservices.default.results.per.page";
        String value = BeaconConfig.get().getProperty(key, result.toString());
        try {
            result = Integer.valueOf(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid value:{} for key:{} in config", value, key);
        }
        return result;
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

    private EntityElement[] buildEntityElements(HashSet<String> fields, List<Entity> entities) {
        EntityElement[] elements = new EntityElement[entities.size()];
        int elementIndex = 0;
        for (Entity entity : entities) {
            elements[elementIndex++] = getEntityElement(entity, fields);
        }
        return elements;
    }

    private EntityElement getEntityElement(Entity entity, HashSet<String> fields) {
        EntityElement elem = new EntityElement();
        elem.type = entity.getEntityType().toString();
        elem.name = entity.getName();
        if (fields.contains(EntityList.EntityFieldList.STATUS.name())) {
            elem.status = getStatus(entity).name();
        }
        if (fields.contains(EntityList.EntityFieldList.TAGS.name())) {
            elem.tag = EntityHelper.getTags(entity);
        }
        if (fields.contains(EntityList.EntityFieldList.CLUSTERS.name())) {
            elem.cluster = new ArrayList<String>(getClustersDefined(entity));
        }
        return elem;
    }

    private static EntityStatus getStatus(final Entity entity) {
        EntityStatus status = null;
        if (EntityType.CLUSTER == entity.getEntityType()) {
            status = EntityStatus.SUBMITTED;
        } else {
                /* TODO : get status from quartz */
//                elem.status = getStatusString(entity);
            status = EntityStatus.RUNNING;
        }
        return status;
    }


    private static Set<String> getClustersDefined(Entity entity) {
        Set<String> clusters = new HashSet<String>();
        switch (entity.getEntityType()) {
            case CLUSTER:
                clusters.add(entity.getName());
                break;

            case REPLICATIONPOLICY:
                ReplicationPolicy policy = (ReplicationPolicy) entity;
                clusters.add(policy.getSourceCluster());
                clusters.add(policy.getTargetCluster());
                break;
            default:
        }
        return clusters;
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


}
