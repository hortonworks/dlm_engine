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

import com.hortonworks.beacon.XTestCase;
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
public class FSReplicationMetricsTest extends XTestCase {
    private static final String JOBID = "job_local_0001";
    private static final String[] COUNTERS = new String[]{ "TOTALMAPTASKS:3", "COMPLETEDMAPTASKS:3",
                                                           "TIMETAKEN:5000", "BYTESCOPIED:1000", "COPY:1", };
    private static final String[] COUNTERS_2 = new String[]{ "TOTALMAPTASKS:5", "COMPLETEDMAPTASKS:5",
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
        initializeServices(null);
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
        Assert.assertEquals(metrics.getTotalMapTasks(), 3L);
        Assert.assertEquals(metrics.getCompletedMapTasks(), 3);
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
            Assert.assertEquals(metricResultList.get(i).getTotalMapTasks(),
                    metricList.get(i).getTotalMapTasks());
            Assert.assertEquals(metricResultList.get(i).getCompletedMapTasks(),
                    metricList.get(i).getCompletedMapTasks());
            Assert.assertEquals(metricResultList.get(i).getBytesCopied(), metricList.get(i).getBytesCopied());
            Assert.assertEquals(metricResultList.get(i).getFilesCopied(), metricList.get(i).getFilesCopied());
            Assert.assertEquals(metricResultList.get(i).getTimeTaken(), metricList.get(i).getTimeTaken());
            Assert.assertEquals(metricResultList.get(i).getJobId(), metricList.get(i).getJobId());
        }

    }
}
