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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
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
        serviceManager.initialize(Arrays.asList(BeaconQuartzScheduler.class.getName()), null);
        scheduler = Services.get().getService(BeaconQuartzScheduler.class);
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
