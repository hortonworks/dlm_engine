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
