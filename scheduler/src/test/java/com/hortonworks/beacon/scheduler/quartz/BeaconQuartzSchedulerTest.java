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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.ReplicationType;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * BeaconQuartzScheduler Test class.
 */
public class BeaconQuartzSchedulerTest {

    public static final String JOB_IDENTIFIER = "job-identifier";
    public static final String NAME = "test-job";
    private BeaconQuartzScheduler scheduler = BeaconQuartzScheduler.get();
    private static final String POLICY_ID = "dataCenter-Cluster-0-1488946092144-000000001";

    @BeforeClass
    public void setUp() throws Exception {
        scheduler.startScheduler();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        scheduler.clear();
    }

    @Test
    public void testScheduleJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
    }

    @Test
    public void testDeleteJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, POLICY_ID,  null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        boolean deleteJob = scheduler.deleteJob(POLICY_ID);
        Assert.assertEquals(deleteJob, true);
    }

    @Test
    public void testSuspendJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        scheduler.suspendJob(POLICY_ID);
    }

    @Test
    public void testResumeJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        scheduler.suspendJob(POLICY_ID);
        scheduler.resumeJob(POLICY_ID);
    }

    private List<ReplicationJobDetails> getReplicationJob() {
        List<ReplicationJobDetails> jobDetailsList = new ArrayList<>();
        ReplicationJobDetails detail = new ReplicationJobDetails(JOB_IDENTIFIER, NAME, ReplicationType.TEST.getName(),
                null);
        jobDetailsList.add(detail);
        return jobDetailsList;
    }
}
