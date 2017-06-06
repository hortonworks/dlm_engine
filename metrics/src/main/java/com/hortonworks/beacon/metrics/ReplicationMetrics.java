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

package com.hortonworks.beacon.metrics;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Replication metrics.
 */
public class ReplicationMetrics {

    private String jobId;
    private long numMapTasks;
    private long bytesCopied;
    private long filesCopied;
    private long timeTaken;

    public ReplicationMetrics() {
    }

    public String getJobId() {
        return jobId;
    }

    private void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getNumMapTasks() {
        return numMapTasks;
    }

    public void setNumMapTasks(long numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    public long getBytesCopied() {
        return bytesCopied;
    }

    private void setBytesCopied(long bytesCopied) {
        this.bytesCopied = bytesCopied;
    }

    public long getFilesCopied() {
        return filesCopied;
    }

    public void setFilesCopied(long filesCopied) {
        this.filesCopied = filesCopied;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    private void setTimeTaken(long timeTaken) {
        this.timeTaken = timeTaken;
    }

    public ReplicationMetrics getReplicationMetrics(String jsonString) {
        Gson gson = new Gson();
        return gson.fromJson(jsonString, ReplicationMetrics.class);
    }

    public String toJsonString() {
        return new Gson().toJson(this);
    }

    private void updateReplicationMetricsDetails(String jobid, long nummaptasks, long bytescopied,
                                                 long copyfiles, long timetaken) {
        this.setJobId(jobid);
        this.setNumMapTasks(nummaptasks);
        this.setBytesCopied(bytescopied);
        this.setFilesCopied(copyfiles);
        this.setTimeTaken(timetaken);
    }

    public void updateReplicationMetricsDetails(String jobid, Map<String, Long> metrics) {
        long numMapTasksVal = metrics.get(ReplicationJobMetrics.NUMMAPTASKS.getName()) != null
                ? metrics.get(ReplicationJobMetrics.NUMMAPTASKS.getName()) : 0L;
        long bytesCopiedVal = metrics.get(ReplicationJobMetrics.BYTESCOPIED.getName()) != null
                ? metrics.get(ReplicationJobMetrics.BYTESCOPIED.getName()) : 0L;
        long filesCopiedVal = metrics.get(ReplicationJobMetrics.COPY.getName()) != null
                ? metrics.get(ReplicationJobMetrics.COPY.getName()) : 0L;
        long timeTakenVal = metrics.get(ReplicationJobMetrics.TIMETAKEN.getName()) != null
                ? metrics.get(ReplicationJobMetrics.TIMETAKEN.getName()) : 0L;
        updateReplicationMetricsDetails(jobid, numMapTasksVal, bytesCopiedVal, filesCopiedVal, timeTakenVal);
    }

    @Override
    public String toString() {
        return "ReplicationMetrics{"
                + "jobId='" + jobId + '\''
                + ", numMapTasks='" + numMapTasks + '\''
                + ", bytesCopied='" + bytesCopied + '\''
                + ", filesCopied='" + filesCopied + '\''
                + ", timeTaken=" + timeTaken
                + '}';
    }
}
