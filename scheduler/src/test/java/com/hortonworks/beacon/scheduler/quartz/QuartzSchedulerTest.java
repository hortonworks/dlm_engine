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
import com.hortonworks.beacon.scheduler.internal.AdminJob;
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
    private static final String GROUP = AdminJob.ADMIN_JOBS;
    private static final int FREQUENCY = 5;
    private JobDetail jobDetail = createJobDetail(NAME, GROUP, getJobDataMap());
    private Trigger trigger = createTrigger(NAME, GROUP, FREQUENCY);
    private ServiceManager serviceManager = ServiceManager.getInstance();


    @BeforeClass
    public void setUp() throws Exception {
        serviceManager.initialize(Arrays.asList(BeaconQuartzScheduler.class.getName()), null);
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
