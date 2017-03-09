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

package com.hortonworks.beacon.common.job;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Job related details exchanged between two jobs.
 */
public class JobContext {
    private String jobInstanceId;
    private int offset;
    private AtomicBoolean shouldInterrupt;
    private Map<String, String> jobContextMap;

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public AtomicBoolean shouldInterrupt() {
        return shouldInterrupt;
    }

    public void setShouldInterrupt(AtomicBoolean shouldInterrupt) {
        this.shouldInterrupt = shouldInterrupt;
    }

    public Map<String, String> getJobContextMap() {
        return jobContextMap;
    }

    public void setJobContextMap(Map<String, String> jobContextMap) {
        this.jobContextMap = jobContextMap;
    }

    @Override
    public String toString() {
        return "JobContext{"
                + "jobInstanceId='" + jobInstanceId + '\''
                + ", offset=" + offset
                + ", shouldInterrupt=" + shouldInterrupt
                + ", jobContextMap=" + jobContextMap
                + '}';
    }
}
