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

import java.util.Map;

import com.google.gson.Gson;

/**
 * Replication metrics.
 */
public class ReplicationMetrics {

    private String jobId;
    private JobType jobType;
    private Progress progress;

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

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    Progress getProgress() {
        return progress;
    }

    public void setProgress(Progress progress) {
        this.progress = progress;
    }

    public String toJsonString() {
        return new Gson().toJson(this);
    }

    private void updateReplicationMetricsDetails(String jobid, JobType type, Progress progressObj) {
        this.setJobId(jobid);
        this.setJobType(type);
        this.setProgress(progressObj);
    }

    private void updateReplicationMetricsDetails(Progress progressObj) {
        this.setProgress(progressObj);
    }

    public void updateReplicationMetricsDetails(String jobid, JobType type, Map<String, Long> metrics,
                                                ProgressUnit unit) {
        updateReplicationMetricsDetails(jobid, type, setFSReplicationProgress(metrics, unit));
    }

    public void updateReplicationMetricsDetails(Map<String, Long> metrics, ProgressUnit unit) {
        long total = metrics.get(ReplicationJobMetrics.TOTAL.getName()) != null
                ? metrics.get(ReplicationJobMetrics.TOTAL.getName()) : 0L;
        long completed = metrics.get(ReplicationJobMetrics.COMPLETED.getName()) != null
                ? metrics.get(ReplicationJobMetrics.COMPLETED.getName()) : 0L;
        updateReplicationMetricsDetails(new Progress(total, completed, unit.getName()));
    }

    private Progress setFSReplicationProgress(Map<String, Long> metrics, ProgressUnit unit) {
        Progress fsProgress = new Progress();
        fsProgress.setTotal(metrics.get(ReplicationJobMetrics.TOTAL.getName()));
        fsProgress.setCompleted(metrics.get(ReplicationJobMetrics.COMPLETED.getName()));
        fsProgress.setFailed(metrics.get(ReplicationJobMetrics.FAILED.getName()));
        fsProgress.setKilled(metrics.get(ReplicationJobMetrics.KILLED.getName()));
        fsProgress.setBytesCopied(metrics.get(ReplicationJobMetrics.BYTESCOPIED.getName()));
        fsProgress.setFilesCopied(metrics.get(ReplicationJobMetrics.COPY.getName()));
        fsProgress.setTimeTaken(metrics.get(ReplicationJobMetrics.TIMETAKEN.getName()));
        fsProgress.setUnit(unit.getName());
        fsProgress.setDirectoriesCopied(metrics.get(ReplicationJobMetrics.DIR_COPY.getName()));

        return fsProgress;
    }

    @Override
    public String toString() {
        return "ReplicationMetrics{"
                + "jobId='" + jobId + '\''
                + "jobType='" + jobType + '\''
                + "progress='" +progress.toString()
                + '}';
    }
}
