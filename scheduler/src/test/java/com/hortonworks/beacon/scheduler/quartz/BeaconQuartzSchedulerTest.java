package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.ReplicationType;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BeaconQuartzSchedulerTest {

    BeaconQuartzScheduler scheduler = BeaconQuartzScheduler.get();

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
        ReplicationJobDetails job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false);
        Assert.assertEquals(jobName, job.getName());
    }

    @Test
    public void testDeleteJob() throws Exception {
        ReplicationJobDetails job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false);
        Assert.assertEquals(jobName, job.getName());
        boolean deleteJob = scheduler.deleteJob(job.getName(), job.getType().toUpperCase());
        Assert.assertEquals(deleteJob, true);
    }

    @Test
    public void testAddJob() throws Exception {
        ReplicationJobDetails job = getReplicationJob();
        String jobName = scheduler.addJob(job, false);
        Assert.assertEquals(jobName, job.getName());
    }

    @Test
    public void testSuspendJob() throws Exception {
        ReplicationJobDetails job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false);
        Assert.assertEquals(jobName, job.getName());
        scheduler.suspendJob(job.getName(), job.getType().toUpperCase());
    }

    @Test
    public void testResumeJob() throws Exception {
        ReplicationJobDetails job = getReplicationJob();
        String jobName = scheduler.scheduleJob(job, false);
        Assert.assertEquals(jobName, job.getName());
        scheduler.suspendJob(job.getName(), job.getType().toUpperCase());
        scheduler.resumeJob(job.getName(), job.getType().toUpperCase());
    }

    private ReplicationJobDetails getReplicationJob() {
        ReplicationJobDetails detail = new ReplicationJobDetails();
        detail.setName("test-job");
        detail.setType(ReplicationType.TEST.getName());
        detail.setFrequency(60);
        return detail;
    }
}