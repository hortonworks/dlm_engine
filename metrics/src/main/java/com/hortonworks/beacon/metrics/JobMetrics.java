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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Job Counters abstract class to be extended by supported job type.
 */
public abstract class JobMetrics {
    private static final BeaconLog LOG = BeaconLog.getLog(JobMetrics.class);
    private static final String COUNTER_GROUP = "org.apache.hadoop.tools.mapred.CopyMapper$Counter";
    private static final String JOB_COUNTER_GROUP = "org.apache.hadoop.mapreduce.JobCounter";
    private static final String TOTAL_LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";
    private Map<String, Long> countersMap = new HashMap<>();

    public void obtainJobMetrics(Job job, boolean isJobComplete) throws IOException {
        try {
            long timeTaken;
            if (isJobComplete) {
                timeTaken = job.getFinishTime() - job.getStartTime();
            } else {
                timeTaken = System.currentTimeMillis() - job.getStartTime();
            }
            countersMap.put(ReplicationJobMetrics.TIMETAKEN.getName(), timeTaken);
            collectJobMetrics(job);
        } catch (Exception e) {
            LOG.error(MessageCode.METR_000001.name(), e);
        }
    }

    void populateReplicationCountersMap(Job job) throws IOException, InterruptedException {
        addTotalMapTasks(job);
        addReplicationCounters(job);
        addCompletedMapTasks(job);
    }

    private void addTotalMapTasks(Job job) throws IOException {
        CounterGroup counterGroup = job.getCounters().getGroup(JOB_COUNTER_GROUP);
        if (counterGroup!=null) {
            countersMap.put(ReplicationJobMetrics.TOTALMAPTASKS.getName(),
                    counterGroup.findCounter(TOTAL_LAUNCHED_MAPS).getValue());
        } else {
            countersMap.put(ReplicationJobMetrics.TOTALMAPTASKS.getName(), 0L);
        }
    }

    private void addReplicationCounters(Job job) throws IOException {
        CounterGroup counterGroup = job.getCounters().getGroup(COUNTER_GROUP);
        for (ReplicationJobMetrics counterKey : ReplicationJobMetrics.values()) {
            for (Counter counter : counterGroup) {
                if (counter.getName().equals(counterKey.name())) {
                    String counterName = counter.getName();
                    long counterValue = counter.getValue();
                    countersMap.put(counterName, counterValue);
                }
            }
        }
    }
    private void addCompletedMapTasks(Job job) throws IOException, InterruptedException {
        long completedTasks=0;
        for (TaskReport task : job.getTaskReports(TaskType.MAP)) {
            if (task.getCurrentStatus() == TIPStatus.COMPLETE
                    && task.getState().equals("SUCCEEDED")) {
                completedTasks++;
            }
        }

        countersMap.put(ReplicationJobMetrics.COMPLETEDMAPTASKS.getName(), completedTasks);
    }

    public Map<String, Long> getCountersMap() {
        return countersMap;
    }

    protected abstract void collectJobMetrics(Job job) throws IOException, InterruptedException;
}
