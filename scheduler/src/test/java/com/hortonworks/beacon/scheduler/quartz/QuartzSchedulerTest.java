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
import com.hortonworks.beacon.scheduler.BeaconSchedulerService;
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
    private static final String GROUP = BeaconQuartzScheduler.START_NODE_GROUP;
    private static final int FREQUENCY = 5;
    private JobDetail jobDetail = createJobDetail(NAME, GROUP, getJobDataMap());
    private Trigger trigger = createTrigger(NAME, GROUP, FREQUENCY);
    private ServiceManager serviceManager = ServiceManager.getInstance();


    @BeforeClass
    public void setUp() throws Exception {
        String[] services = new String[]{BeaconSchedulerService.SERVICE_NAME};
        serviceManager.initialize(Arrays.asList(services));
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
        scheduler.deleteJob(NAME, GROUP);
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
