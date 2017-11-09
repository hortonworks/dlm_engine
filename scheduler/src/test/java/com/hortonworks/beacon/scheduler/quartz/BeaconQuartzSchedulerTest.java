/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.SchedulerStartService;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
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
        String[] services = new String[]{SchedulerInitService.SERVICE_NAME};
        String[] dependentService = new String[]{SchedulerStartService.SERVICE_NAME};
        serviceManager.initialize(Arrays.asList(services), Arrays.asList(dependentService));
        scheduler = ((SchedulerInitService)Services.get().getService(SchedulerInitService.SERVICE_NAME))
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
