package com.hortonworks.beacon.main;

import com.hortonworks.beacon.entity.Entity;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.entity.lock.MemoryLocks;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.exceptions.EntityAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sramesh on 9/30/16.
 */
public abstract class AbstractEntityManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntityManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    private ConfigurationStore configStore = ConfigurationStore.get();

    protected synchronized void submitInternal(Entity entity) throws IOException, BeaconException {
        EntityType entityType = entity.getEntityType();
        List<Entity> tokenList = new ArrayList<>();


        try {
            obtainEntityLocks(entity, "submit", tokenList);
        }finally {
            ConfigurationStore.get().cleanupUpdateInit();
            releaseEntityLocks(entity.getName(), tokenList);
        }
        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting.");
        }

//        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: ({}): {}", entityType, entity.getName());
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

}
