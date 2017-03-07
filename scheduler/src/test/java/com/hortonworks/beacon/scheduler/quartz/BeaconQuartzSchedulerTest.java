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

    private BeaconQuartzScheduler scheduler = BeaconQuartzScheduler.get();

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
        String jobName = scheduler.scheduleJob(job, false, null, null, 60);
        Assert.assertEquals(jobName, job.get(0).getName());
    }

    @Test
    public void testDeleteJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, null, null, 60);
        Assert.assertEquals(jobName, job.get(0).getName());
        boolean deleteJob = scheduler.deleteJob(job.get(0).getName(), job.get(0).getType().toUpperCase());
        Assert.assertEquals(deleteJob, true);
    }

    @Test
    public void testSuspendJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, null, null, 60);
        Assert.assertEquals(jobName, job.get(0).getName());
        scheduler.suspendJob(job.get(0).getName(), job.get(0).getType().toUpperCase());
    }

    @Test
    public void testResumeJob() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false, null, null, 60);
        Assert.assertEquals(jobName, job.get(0).getName());
        scheduler.suspendJob(job.get(0).getName(), job.get(0).getType().toUpperCase());
        scheduler.resumeJob(job.get(0).getName(), job.get(0).getType().toUpperCase());
    }

    private List<ReplicationJobDetails> getReplicationJob() {
        List<ReplicationJobDetails> jobDetailsList = new ArrayList<>();
        ReplicationJobDetails detail = new ReplicationJobDetails("job-type", "test-job",
                ReplicationType.TEST.getName(), null);
        jobDetailsList.add(detail);
        return jobDetailsList;
    }
}
