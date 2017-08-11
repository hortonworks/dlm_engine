/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

import java.util.Hashtable;
import java.util.Map;

/**
 * It maintains map of currently running instances in the scheduler.
 */
public final class SchedulerCache {

    private static final BeaconLog LOG = BeaconLog.getLog(SchedulerCache.class);
    private Map<String, InstanceSchedulerDetail> cache;

    private static final SchedulerCache INSTANCE = new SchedulerCache();

    private SchedulerCache() {
        cache = new Hashtable<>();
    }

    public static SchedulerCache get() {
        return INSTANCE;
    }

    public synchronized boolean exists(String key) {
        boolean exists = cache.containsKey(key);
        LOG.info(MessageCode.SCHD_000011.name(), key, exists);
        return exists;
    }

    public synchronized void insert(String key, InstanceSchedulerDetail value) {
        LOG.info(MessageCode.SCHD_000012.name(), key, value);
        cache.put(key, value);
    }

    public synchronized InstanceSchedulerDetail remove(String key) {
        LOG.info(MessageCode.SCHD_000013.name(), key);
        return cache.remove(key);
    }

    public synchronized Boolean registerInterrupt(String key) {
        LOG.info(MessageCode.SCHD_000014.name(), key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        if (schedulerDetail != null) {
            schedulerDetail.setInterrupt(true);
            return true;
        }
        return false;
    }

    public synchronized boolean getInterrupt(String key) {
        LOG.info(MessageCode.SCHD_000015.name(), key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        return schedulerDetail != null && schedulerDetail.isInterrupt();
    }
}
