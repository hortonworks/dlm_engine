/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Replication metrics.
 */
public class ReplicationMetrics {

    private String jobId;
    private JobType jobType;
    private long totalMapTasks;
    private long completedMapTasks;
    private long bytesCopied;
    private long filesCopied;
    private long timeTaken;

    /**
     * Enum for replication job type.
     */
    public enum JobType {
        MAIN,
        RECOVERY,
    }

    public ReplicationMetrics() {
    }

    public String getJobId() {
        return jobId;
    }

    private void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public long getTotalMapTasks() {
        return totalMapTasks;
    }

    public void setTotalMapTasks(long totalMapTasks) {
        this.totalMapTasks = totalMapTasks;
    }

    public long getCompletedMapTasks() {
        return completedMapTasks;
    }

    public void setCompletedMapTasks(long completedMapTasks) {
        this.completedMapTasks = completedMapTasks;
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

    public String toJsonString() {
        return new Gson().toJson(this);
    }

    private void updateReplicationMetricsDetails(String jobid, JobType type, long totalmaptasks,
                                                 long completedmaptasks, long bytescopied,
                                                 long copyfiles, long timetaken) {
        this.setJobId(jobid);
        this.setJobType(type);
        this.setTotalMapTasks(totalmaptasks);
        this.setCompletedMapTasks(completedmaptasks);
        this.setBytesCopied(bytescopied);
        this.setFilesCopied(copyfiles);
        this.setTimeTaken(timetaken);
    }

    public void updateReplicationMetricsDetails(String jobid, JobType type, Map<String, Long> metrics) {
        long totalMapTasksVal = metrics.get(ReplicationJobMetrics.TOTALMAPTASKS.getName()) != null
                ? metrics.get(ReplicationJobMetrics.TOTALMAPTASKS.getName()) : 0L;
        long completedMapTasksVal = metrics.get(ReplicationJobMetrics.COMPLETEDMAPTASKS.getName()) != null
                ? metrics.get(ReplicationJobMetrics.COMPLETEDMAPTASKS.getName()) : 0L;
        long bytesCopiedVal = metrics.get(ReplicationJobMetrics.BYTESCOPIED.getName()) != null
                ? metrics.get(ReplicationJobMetrics.BYTESCOPIED.getName()) : 0L;
        long filesCopiedVal = metrics.get(ReplicationJobMetrics.COPY.getName()) != null
                ? metrics.get(ReplicationJobMetrics.COPY.getName()) : 0L;
        long timeTakenVal = metrics.get(ReplicationJobMetrics.TIMETAKEN.getName()) != null
                ? metrics.get(ReplicationJobMetrics.TIMETAKEN.getName()) : 0L;
        updateReplicationMetricsDetails(jobid, type, totalMapTasksVal, completedMapTasksVal,
                bytesCopiedVal, filesCopiedVal, timeTakenVal);
    }

    @Override
    public String toString() {
        return "ReplicationMetrics{"
                + "jobId='" + jobId + '\''
                + "jobType='" + jobType + '\''
                + ", totalMapTasks='" + totalMapTasks + '\''
                + ", completedMapTasks='" + completedMapTasks + '\''
                + ", bytesCopied='" + bytesCopied + '\''
                + ", filesCopied='" + filesCopied + '\''
                + ", timeTaken=" + timeTaken
                + '}';
    }
}
