/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;

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
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        boolean interrupted = false;
        if (schedulerDetail != null) {
            schedulerDetail.setInterrupt(true);
            interrupted = true;
        }
        LOG.debug("Registering interruption for key: {}, interrupted? {}", key, interrupted);
        return interrupted;
    }

    public synchronized boolean getInterrupt(String key) {
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        boolean interrupt = schedulerDetail != null && schedulerDetail.isInterrupt();
        LOG.debug("Querying interrupt flag for key: {}, interrupt? {}", key, interrupt);
        return interrupt;
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
