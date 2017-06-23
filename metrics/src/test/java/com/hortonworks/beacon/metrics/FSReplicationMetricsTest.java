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

import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for FS Replication Counters.
 */
public class FSReplicationMetricsTest {
    private static final String JOBID = "job_local_0001";
    private static final String[] COUNTERS = new String[]{"NUMMAPTASKS:3", "TIMETAKEN:5000",
        "BYTESCOPIED:1000", "COPY:1", };
    private static final String[] COUNTERS_2 = new String[]{
        "NUMMAPTASKS:5", "TIMETAKEN:4000", "BYTESCOPIED:4000", "COPY:2", };
    private Map<String, Long> countersMap = new HashMap<>();
    private Map<String, Long> countersMap2 = new HashMap<>();

    @BeforeClass
    public void setUp() throws Exception {
        for (String counters : COUNTERS) {
            countersMap.put(counters.split(":")[0], Long.parseLong(counters.split(":")[1]));
        }

        for (String counters : COUNTERS_2) {
            countersMap2.put(counters.split(":")[0], Long.parseLong(counters.split(":")[1]));
        }
    }

    @Test
    public void testObtainJobCounters() throws Exception {
        for (String countersKey : countersMap.keySet()) {
            Assert.assertEquals(countersKey, ReplicationJobMetrics.getCountersKey(countersKey).getName());
        }
    }

    @Test
    public void testReplicationMetrics() {
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        replicationMetrics.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.MAIN, countersMap);

        ReplicationMetrics metrics = ReplicationMetricsUtils.getReplicationMetrics(replicationMetrics.toJsonString());
        Assert.assertEquals(metrics.getNumMapTasks(), 3);
        Assert.assertEquals(metrics.getBytesCopied(), 1000L);
        Assert.assertEquals(metrics.getFilesCopied(), 1);
        Assert.assertEquals(metrics.getTimeTaken(), 5000);
        Assert.assertEquals(metrics.getJobId(), JOBID);
    }

    @Test
    public void testListOfReplicationMetrics() {
        ReplicationMetrics replicationMetrics1 = new ReplicationMetrics();
        replicationMetrics1.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.MAIN, countersMap);

        ReplicationMetrics replicationMetrics2 = new ReplicationMetrics();
        replicationMetrics2.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.RECOVERY, countersMap2);

        List<ReplicationMetrics> metricList = new ArrayList<>();
        metricList.add(replicationMetrics1);
        metricList.add(replicationMetrics2);


        List<ReplicationMetrics> metricResultList = ReplicationMetricsUtils.getListOfReplicationMetrics(
                ReplicationMetricsUtils.toJsonString(metricList));

        for (int i = 0; i < metricResultList.size(); ++i) {
            Assert.assertEquals(metricResultList.get(i).getNumMapTasks(), metricList.get(i).getNumMapTasks());
            Assert.assertEquals(metricResultList.get(i).getBytesCopied(), metricList.get(i).getBytesCopied());
            Assert.assertEquals(metricResultList.get(i).getFilesCopied(), metricList.get(i).getFilesCopied());
            Assert.assertEquals(metricResultList.get(i).getTimeTaken(), metricList.get(i).getTimeTaken());
            Assert.assertEquals(metricResultList.get(i).getJobId(), metricList.get(i).getJobId());
        }

    }
}
