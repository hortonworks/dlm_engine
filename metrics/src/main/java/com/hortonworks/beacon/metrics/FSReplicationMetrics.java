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

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Obtain and store Filesystem Replication counters from Distcp job.
 */
public class FSReplicationMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(FSReplicationMetrics.class);
    private static final String COUNTER_GROUP = "org.apache.hadoop.tools.mapred.CopyMapper$Counter";
    private static final String JOB_COUNTER_GROUP = "org.apache.hadoop.mapreduce.JobCounter";
    private static final String TOTAL_LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
    private static final String NUM_KILLED_MAPS = "NUM_KILLED_MAPS";
    private static final String NUM_FAILED_MAPS = "NUM_FAILED_MAPS";
    private Progress progress = new Progress();

    public void obtainJobMetrics(Job job, boolean isJobComplete) {
        try {
            long timeTaken;
            if (isJobComplete) {
                timeTaken = job.getFinishTime() - job.getStartTime();
            } else {
                timeTaken = System.currentTimeMillis() - job.getStartTime();
            }
            progress.setTimeTaken(timeTaken);
            if (job.isComplete()) {
                progress.setJobProgress(100);
            } else {
                float jobProgress = job.getStatus().getMapProgress() * 100;
                progress.setJobProgress(Math.round(jobProgress * 100.0f)/100.0f);
            }
            progress.setUnit(ProgressUnit.MAPTASKS.getName());
            populateReplicationCountersMap(job);
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception occurred while obtaining job counters/progress", e);
        }
    }

    private void populateReplicationCountersMap(Job job) throws IOException, InterruptedException {
        addTotalMapTasks(job);
        addReplicationCounters(job);
        addCompletedMapTasks(job);
    }

    private void addTotalMapTasks(Job job) throws IOException {
        CounterGroup counterGroup = job.getCounters().getGroup(JOB_COUNTER_GROUP);
        if (counterGroup!=null) {
            progress.setTotal(counterGroup.findCounter(TOTAL_LAUNCHED_MAPS).getValue());
            progress.setFailed(counterGroup.findCounter(NUM_FAILED_MAPS).getValue());
            progress.setKilled(counterGroup.findCounter(NUM_KILLED_MAPS).getValue());
        } else {
            progress.setTotal(0);
            progress.setFailed(0);
            progress.setKilled(0);
        }
    }

    private void addReplicationCounters(Job job) throws IOException {
        CounterGroup counterGroup = job.getCounters().getGroup(COUNTER_GROUP);
        for (Counter counter : counterGroup) {
            if (counter.getName().equals(ReplicationJobMetrics.BYTESCOPIED.name())) {
                progress.setBytesCopied(counter.getValue());
            } else if (counter.getName().equals(ReplicationJobMetrics.COPY.name())) {
                progress.setFilesCopied(counter.getValue());
            } else if (counter.getName().equals(ReplicationJobMetrics.DIR_COPY.name())) {
                progress.setDirectoriesCopied(counter.getValue());
            }
        }
    }
    private void addCompletedMapTasks(Job job) throws IOException, InterruptedException {
        long completedTasks=0;
        if (job.isComplete()) {
            completedTasks = progress.getTotal();
        } else {
            for (TaskReport task : job.getTaskReports(TaskType.MAP)) {
                if (task.getCurrentStatus() == TIPStatus.COMPLETE
                        && task.getState().equals("SUCCEEDED")) {
                    completedTasks++;
                }
            }
        }
        progress.setCompleted(completedTasks);
    }

    public Progress getProgress() {
        return progress;
    }
}
