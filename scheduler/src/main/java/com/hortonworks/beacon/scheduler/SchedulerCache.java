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

import com.hortonworks.beacon.exceptions.BeaconException;
import java.util.Hashtable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It maintains map of currently running instances in the scheduler.
 */
public final class SchedulerCache {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerCache.class);
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
        LOG.debug("Key [{}] exists [{}] in the cache.", key, exists);
        return exists;
    }

    public synchronized void insert(String key, InstanceSchedulerDetail value) {
        LOG.info("Inserting new entry into cache for key: [{}], value: [{}].", key, value);
        cache.put(key, value);
    }

    public synchronized InstanceSchedulerDetail remove(String key) {
        LOG.info("Removing entry from cache for key: [{}].", key);
        return cache.remove(key);
    }

    public synchronized Boolean registerInterrupt(String key) {
        LOG.debug("Registering interruption for key: [{}].", key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        if (schedulerDetail != null) {
            schedulerDetail.setInterrupt(true);
            return true;
        }
        return false;
    }

    public synchronized boolean getInterrupt(String key) {
        LOG.debug("Querying interrupt flag for key: [{}].", key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        return schedulerDetail != null && schedulerDetail.isInterrupt();
    }

    public synchronized void updateInstanceSchedulerDetail(String key, String instanceId) throws BeaconException {
        if (exists(key)) {
            InstanceSchedulerDetail detail = cache.get(key);
            detail.setInstanceId(instanceId);
        } else {
            throw new BeaconException("Policy {} is not present into scheduler cache. Instance Id: {}", key,
                instanceId);
        }
    }

    public synchronized InstanceSchedulerDetail getInstanceSchedulerDetail(String key) {
        if (exists(key)) {
            return new InstanceSchedulerDetail(cache.get(key));
        } else {
            return null;
        }
    }
}
