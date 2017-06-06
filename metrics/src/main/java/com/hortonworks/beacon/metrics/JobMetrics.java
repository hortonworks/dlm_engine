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

import com.hortonworks.beacon.log.BeaconLog;
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
            LOG.error("Exception occurred while obtaining job counters: {}", e);
        }
    }

    void populateReplicationCountersMap(Job job) throws IOException, InterruptedException {
        addReplicationCounters(job);
        addCompletedMapTasks(job);
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

        countersMap.put(ReplicationJobMetrics.NUMMAPTASKS.getName(), completedTasks);
    }

    public Map<String, Long> getCountersMap() {
        return countersMap;
    }

    protected abstract void collectJobMetrics(Job job) throws IOException, InterruptedException;
}
