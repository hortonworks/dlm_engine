/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.lock;

import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory resource locking that provides lock capabilities.
 */
public final class MemoryLocks {
    private static final BeaconLog LOG = BeaconLog.getLog(MemoryLocks.class);
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
            LOG.info(MessageCode.ENTI_000008.name(), command, entity.toShortString(),
                    Thread.currentThread().getName());
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
        LOG.info(MessageCode.ENTI_000009.name(), entity.toShortString(),
                Thread.currentThread().getName());
    }

    private String getLockKey(Entity entity) {
        return entity.getEntityType().toString() + "." + entity.getName();
    }

}
