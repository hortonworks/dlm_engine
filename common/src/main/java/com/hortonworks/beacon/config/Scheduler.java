/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
    private int replicationMetricsInterval;

    public void copy(Scheduler o) {
        setQuartzPrefix(o.getQuartzPrefix());
        setQuartzThreadPool(o.getQuartzThreadPool());
        setRetiredPolicyOlderThan(o.getRetiredPolicyOlderThan());
        setCleanupFrequency(o.getCleanupFrequency());
        setHousekeepingThreads(o.getHousekeepingThreads());
        setSyncStatusFrequency(o.getSyncStatusFrequency());
        setMinReplicationFrequency(o.getMinReplicationFrequency());
        setReplicationMetricsInterval(o.getReplicationMetricsInterval());
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

    public int getReplicationMetricsInterval() {
        return replicationMetricsInterval;
    }

    public void setReplicationMetricsInterval(int replicationMetricsInterval) {
        this.replicationMetricsInterval = replicationMetricsInterval;
    }
}
