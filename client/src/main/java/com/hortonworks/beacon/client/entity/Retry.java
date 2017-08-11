/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.entity;

/**
 * Retry definition for ReplicationPolicy.
 */
public class Retry {
    private int attempts;
    private long delay;
    public static final int RETRY_ATTEMPTS = 3;
    // 30 second
    public static final long RETRY_DELAY = 30;

    public Retry() {
    }

    public Retry(int attempts, long delay) {
        this.attempts = attempts;
        this.delay = delay;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    @Override
    public String toString() {
        return "Retry{"
                + "attempts=" + attempts
                + ", delay=" + delay
                + '}';
    }

}
