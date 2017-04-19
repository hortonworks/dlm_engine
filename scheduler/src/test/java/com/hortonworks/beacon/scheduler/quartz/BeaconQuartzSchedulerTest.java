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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.BeaconSchedulerService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.util.ReplicationType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * BeaconQuartzScheduler Test class.
 */
public class BeaconQuartzSchedulerTest {

    private static final String JOB_IDENTIFIER = "job-identifier";
    private static final String NAME = "test-job";
    private BeaconQuartzScheduler scheduler;
    private static final String POLICY_ID = "dataCenter-Cluster-0-1488946092144-000000001";
    private ServiceManager serviceManager = ServiceManager.getInstance();

    @BeforeClass
    public void setUp() throws Exception {
        String[] services = new String[]{BeaconSchedulerService.SERVICE_NAME};
        serviceManager.initialize(Arrays.asList(services));
        scheduler = ((BeaconSchedulerService)Services.get().getService(BeaconSchedulerService.SERVICE_NAME))
                .getScheduler();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        scheduler.clear();
    }

    @AfterClass
    public void cleanup() throws BeaconException {
        serviceManager.destroy();
    }

    @Test
    public void testSchedulePolicy() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.schedulePolicy(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
    }

    @Test
    public void testDeletePolicy() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.schedulePolicy(job, false, POLICY_ID,  null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        boolean deleteJob = scheduler.deletePolicy(POLICY_ID);
        Assert.assertEquals(deleteJob, true);
    }

    @Test
    public void testSuspendPolicy() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.schedulePolicy(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        scheduler.suspendPolicy(POLICY_ID);
    }

    @Test
    public void testResumePolicy() throws Exception {
        List<ReplicationJobDetails> job = getReplicationJob();
        String jobName = scheduler.schedulePolicy(job, false, POLICY_ID, null, null, 60);
        Assert.assertEquals(jobName, POLICY_ID);
        scheduler.suspendPolicy(POLICY_ID);
        scheduler.resumePolicy(POLICY_ID);
    }

    private List<ReplicationJobDetails> getReplicationJob() {
        List<ReplicationJobDetails> jobDetailsList = new ArrayList<>();
        ReplicationJobDetails detail = new ReplicationJobDetails(JOB_IDENTIFIER, NAME, ReplicationType.TEST.getName(),
                null);
        jobDetailsList.add(detail);
        return jobDetailsList;
    }
}
