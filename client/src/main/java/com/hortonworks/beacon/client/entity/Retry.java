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
