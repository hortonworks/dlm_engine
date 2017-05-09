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

package com.hortonworks.beacon.scheduler;

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
        LOG.info("key [{}] exists [{}] in the cache.", key, exists);
        return exists;
    }

    public synchronized void insert(String key, InstanceSchedulerDetail value) {
        LOG.info("inserting new entry into cache for key: [{}], value: [{}].", key, value);
        cache.put(key, value);
    }

    public synchronized InstanceSchedulerDetail remove(String key) {
        LOG.info("removing entry from cache for key: [{}].", key);
        return cache.remove(key);
    }

    public synchronized Boolean registerInterrupt(String key) {
        LOG.info("registering interruption for key: [{}].", key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        if (schedulerDetail != null) {
            schedulerDetail.setInterrupt(true);
            return true;
        }
        return false;
    }

    public synchronized boolean getInterrupt(String key) {
        LOG.info("querying interrupt flag for key: [{}].", key);
        InstanceSchedulerDetail schedulerDetail = cache.get(key);
        return schedulerDetail != null && schedulerDetail.isInterrupt();
    }
}
