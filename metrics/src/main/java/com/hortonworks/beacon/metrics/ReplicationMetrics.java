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
