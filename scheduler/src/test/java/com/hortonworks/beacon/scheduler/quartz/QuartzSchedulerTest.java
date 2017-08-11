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

import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.internal.AdminJob;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.scheduler.SchedulerStartService;
import com.hortonworks.beacon.service.ServiceManager;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * QuartzScheduler Test class.
 */
public class QuartzSchedulerTest {

    private QuartzScheduler scheduler = QuartzScheduler.get();
    private static final  String NAME = "test-job";
    private static final String GROUP = AdminJob.POLICY_STATUS;
    private static final int FREQUENCY = 5;
    private JobDetail jobDetail = createJobDetail(NAME, GROUP, getJobDataMap());
    private Trigger trigger = createTrigger(NAME, GROUP, FREQUENCY);
    private ServiceManager serviceManager = ServiceManager.getInstance();


    @BeforeClass
    public void setUp() throws Exception {
        String[] services = new String[]{SchedulerInitService.SERVICE_NAME, ResourceBundleService.SERVICE_NAME};
        String[] dependentService = new String[]{SchedulerStartService.SERVICE_NAME};
        serviceManager.initialize(Arrays.asList(services), Arrays.asList(dependentService));
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
    public void testScheduleJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        JobDetail response = scheduler.getJobDetail(NAME, GROUP);
        Assert.assertEquals(response.getKey().getName(), NAME);
        Assert.assertEquals(response.getKey().getGroup(), GROUP);
    }

    @Test
    public void testDeleteJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        JobDetail response = scheduler.getJobDetail(NAME, GROUP);
        Assert.assertEquals(response.getKey().getName(), NAME);
        Assert.assertEquals(response.getKey().getGroup(), GROUP);
        boolean deleteJob = scheduler.deleteJob(NAME, GROUP);
        Assert.assertTrue(deleteJob);
        response = scheduler.getJobDetail(NAME, GROUP);
        Assert.assertNull(response);
    }

    @Test
    public void testSuspendJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.suspendJob(NAME, GROUP);
        JobDetail response = scheduler.getJobDetail(NAME, GROUP);
        Assert.assertEquals(response.getKey().getName(), NAME);
        Assert.assertEquals(response.getKey().getGroup(), GROUP);
    }

    @Test
    public void testResumeJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.resumeJob(NAME, GROUP);
        JobDetail response = scheduler.getJobDetail(NAME, GROUP);
        Assert.assertEquals(response.getKey().getName(), NAME);
        Assert.assertEquals(response.getKey().getGroup(), GROUP);
    }

    private Trigger createTrigger(String name, String group, int frequency) {
        return TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(1)
                .withMisfireHandlingInstructionNowWithRemainingCount()
                .withIntervalInSeconds(frequency)
                ).build();
    }

    private JobDetail createJobDetail(String name, String group, JobDataMap jobDataMap) {
        return JobBuilder.newJob(QuartzTestJob.class)
                .withIdentity(name, group)
                .storeDurably(true)
                .setJobData(jobDataMap)
                .build();
    }

    private JobDataMap getJobDataMap() {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(QuartzDataMapEnum.NO_OF_JOBS.getValue(), 1);
        return jobDataMap;
    }
}
