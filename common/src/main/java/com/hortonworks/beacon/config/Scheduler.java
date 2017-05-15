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

package com.hortonworks.beacon.config;

/**
 * Configuration parameter related to beacon scheduler.
 * syncStatusFrequency: status sync frequency is in minutes.
 */
public class Scheduler {
    private String quartzPrefix;
    private String quartzThreadPool;
    private int retiredPolicyOlderThan;
    private int cleanupFrequency;
    private int housekeepingThreads;
    private int syncStatusFrequency;
    private int minReplicationFrequency;

    public void copy(Scheduler o) {
        setQuartzPrefix(o.getQuartzPrefix());
        setQuartzThreadPool(o.getQuartzThreadPool());
        setRetiredPolicyOlderThan(o.getRetiredPolicyOlderThan());
        setCleanupFrequency(o.getCleanupFrequency());
        setHousekeepingThreads(o.getHousekeepingThreads());
        setSyncStatusFrequency(o.getSyncStatusFrequency());
        setMinReplicationFrequency(o.getMinReplicationFrequency());
    }

    public String getQuartzPrefix() {
        return quartzPrefix;
    }

    public void setQuartzPrefix(String quartzPrefix) {
        this.quartzPrefix = quartzPrefix;
    }

    public String getQuartzThreadPool() {
        return quartzThreadPool;
    }

    public void setQuartzThreadPool(String quartzThreadPool) {
        this.quartzThreadPool = quartzThreadPool;
    }

    public int getRetiredPolicyOlderThan() {
        return retiredPolicyOlderThan;
    }

    public void setRetiredPolicyOlderThan(int retiredPolicyOlderThan) {
        this.retiredPolicyOlderThan = retiredPolicyOlderThan;
    }

    public int getCleanupFrequency() {
        return cleanupFrequency;
    }

    public void setCleanupFrequency(int cleanupFrequency) {
        this.cleanupFrequency = cleanupFrequency;
    }

    public int getHousekeepingThreads() {
        return housekeepingThreads;
    }

    public void setHousekeepingThreads(int housekeepingThreads) {
        this.housekeepingThreads = housekeepingThreads;
    }

    public int getSyncStatusFrequency() {
        return syncStatusFrequency;
    }

    public void setSyncStatusFrequency(int syncStatusFrequency) {
        this.syncStatusFrequency = syncStatusFrequency;
    }

    public int getMinReplicationFrequency() {
        return minReplicationFrequency;
    }

    public void setMinReplicationFrequency(int minReplicationFrequency) {
        this.minReplicationFrequency = minReplicationFrequency;
    }
}
