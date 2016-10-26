package com.hortonworks.beacon.entity.lock;

/**
 * Created by sramesh on 9/30/16.
 */

import com.hortonworks.beacon.client.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory resource locking that provides lock capabilities.
 */
public final class MemoryLocks {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLocks.class);
    private static ConcurrentHashMap<String, Boolean> locks = new ConcurrentHashMap<String, Boolean>();

    private static MemoryLocks instance = new MemoryLocks();

    private MemoryLocks() {
    }

    public static MemoryLocks getInstance() {
        return instance;
    }

    /**
     * Obtain a lock for an entity.
     *
     * @param entity entity object.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     */
    public boolean acquireLock(Entity entity, String command) {
        boolean lockObtained = false;
        String entityName = getLockKey(entity);

        Boolean putResponse = locks.putIfAbsent(entityName, true);
        if (putResponse == null || !putResponse) {
            LOG.info("Lock acquired for {} on {} by {}",
                    command, entity.toShortString(), Thread.currentThread().getName());
            lockObtained = true;
        }
        return lockObtained;
    }

    /**
     * Release the lock for an entity.
     *
     * @param entity entity object.
     */
    public void releaseLock(Entity entity) {
        String entityName = getLockKey(entity);

        locks.remove(entityName);
        LOG.info("Successfully released lock on {} by {}",
                entity.toShortString(), Thread.currentThread().getName());
    }

    private String getLockKey(Entity entity) {
        return entity.getEntityType().toString() + "." + entity.getName();
    }


}