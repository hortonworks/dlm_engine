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

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import com.hortonworks.beacon.service.ServiceManager;

/**
 * Test for FS Replication metrics.
 */
public class FSReplicationMetricsTest {
    private static final String JOBID = "job_local_0001";
    private static final String[] COUNTERS = new String[]{ "TOTAL:5", "COMPLETED:3", "FAILED:1", "KILLED:1",
        "TIMETAKEN:5000", "BYTESCOPIED:1000", "COPY:1", "DIR_COPY:2", };
    private static final String[] COUNTERS_2 = new String[]{ "TOTAL:5", "COMPLETED:5", "FAILED:0", "KILLED:0",
                                                             "TIMETAKEN:4000", "BYTESCOPIED:4000", "COPY:2", };
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
        ServiceManager.getInstance().initialize(null, null);
    }

    @Test
    public void testObtainJobCounters() throws Exception {
        for (String countersKey : countersMap.keySet()) {
            assertEquals(countersKey, ReplicationJobMetrics.getCountersKey(countersKey).getName());
        }
    }

    @Test
    public void testReplicationMetrics() {
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        replicationMetrics.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.MAIN,
                countersMap, ProgressUnit.MAPTASKS);
        ReplicationMetrics metrics = ReplicationMetricsUtils.getReplicationMetrics(replicationMetrics.toJsonString());
        assertEquals(metrics.getProgress().getTotal(), 5L);
        assertEquals(metrics.getProgress().getCompleted(), 3);
        assertEquals(metrics.getProgress().getFailed(), 1);
        assertEquals(metrics.getProgress().getKilled(), 1);
        assertEquals(metrics.getProgress().getBytesCopied(), 1000L);
        assertEquals(metrics.getProgress().getFilesCopied(), 1);
        assertEquals(metrics.getProgress().getTimeTaken(), 5000);
        assertEquals(metrics.getProgress().getDirectoriesCopied(), 2);
        assertEquals(metrics.getJobId(), JOBID);
    }

    @Test
    public void testListOfReplicationMetrics() {
        ReplicationMetrics replicationMetrics1 = new ReplicationMetrics();
        replicationMetrics1.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.MAIN,
                countersMap, ProgressUnit.MAPTASKS);

        ReplicationMetrics replicationMetrics2 = new ReplicationMetrics();
        replicationMetrics2.updateReplicationMetricsDetails(JOBID, ReplicationMetrics.JobType.RECOVERY,
                countersMap2, ProgressUnit.MAPTASKS);

        List<ReplicationMetrics> metricList = new ArrayList<>();
        metricList.add(replicationMetrics1);
        metricList.add(replicationMetrics2);


        List<ReplicationMetrics> metricResultList = ReplicationMetricsUtils.getListOfReplicationMetrics(
                ReplicationMetricsUtils.toJsonString(metricList));

        for (int i = 0; i < metricResultList.size(); ++i) {
            assertEquals(metricResultList.get(i).getProgress().getTotal(),
                    metricList.get(i).getProgress().getTotal());
            assertEquals(metricResultList.get(i).getProgress().getCompleted(),
                    metricList.get(i).getProgress().getCompleted());
            assertEquals(metricResultList.get(i).getProgress().getFailed(),
                    metricList.get(i).getProgress().getFailed());
            assertEquals(metricResultList.get(i).getProgress().getKilled(),
                    metricList.get(i).getProgress().getKilled());
            assertEquals(metricResultList.get(i).getProgress().getBytesCopied(),
                    (metricList.get(i).getProgress()).getBytesCopied());
            assertEquals(metricResultList.get(i).getProgress().getFilesCopied(),
                    (metricList.get(i).getProgress()).getFilesCopied());
            assertEquals(metricResultList.get(i).getProgress().getTimeTaken(),
                    (metricList.get(i).getProgress()).getTimeTaken());
            assertEquals(metricResultList.get(i).getJobId(), metricList.get(i).getJobId());
        }

    }
}
