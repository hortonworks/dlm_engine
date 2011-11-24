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

package com.hortonworks.beacon.config;

/**
 * Configuration parameter related to beacon scheduler.
 * housekeepingSyncFrequency: status sync frequency is in minutes.
 */
public class Scheduler {
    private String quartzPrefix;
    private String quartzThreadPool;
    private int retiredPolicyOlderThan;
    private int cleanupFrequency;
    private int housekeepingThreads;
    private int housekeepingSyncFrequency;
    private int housekeepingSyncMaxRetry;
    private int minReplicationFrequency;
    private int replicationMetricsInterval;
    private int policyCheckFrequency;

    public void copy(Scheduler o) {
        setQuartzPrefix(o.getQuartzPrefix());
        setQuartzThreadPool(o.getQuartzThreadPool());
        setRetiredPolicyOlderThan(o.getRetiredPolicyOlderThan());
        setCleanupFrequency(o.getCleanupFrequency());
        setHousekeepingThreads(o.getHousekeepingThreads());
        setHousekeepingSyncFrequency(o.getHousekeepingSyncFrequency());
        setMinReplicationFrequency(o.getMinReplicationFrequency());
        setReplicationMetricsInterval(o.getReplicationMetricsInterval());
        setHousekeepingSyncMaxRetry(o.getHousekeepingSyncMaxRetry());
        setPolicyCheckFrequency(o.getPolicyCheckFrequency());
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

    public int getHousekeepingSyncFrequency() {
        return housekeepingSyncFrequency;
    }

    public void setHousekeepingSyncFrequency(int housekeepingSyncFrequency) {
        this.housekeepingSyncFrequency = housekeepingSyncFrequency;
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

    public int getHousekeepingSyncMaxRetry() {
        return housekeepingSyncMaxRetry;
    }

    public void setHousekeepingSyncMaxRetry(int housekeepingSyncMaxRetry) {
        this.housekeepingSyncMaxRetry = housekeepingSyncMaxRetry;
    }

    public int getPolicyCheckFrequency() {
        return policyCheckFrequency;
    }

    public void setPolicyCheckFrequency(int policyCheckFrequency) {
        this.policyCheckFrequency = policyCheckFrequency;
    }
}
