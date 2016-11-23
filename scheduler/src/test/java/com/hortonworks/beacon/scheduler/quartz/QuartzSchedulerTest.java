package com.hortonworks.beacon.scheduler.quartz;

import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class QuartzSchedulerTest {

    private QuartzScheduler scheduler = QuartzScheduler.get();
    private String name = "test-job";
    private String group = "test-group";
    private JobDetail jobDetail = createJobDetail(name, group, getJobDataMap());
    private Trigger trigger = createTrigger(name, group, 5);


    @BeforeClass
    public void setUp() throws Exception {
        scheduler.startScheduler(new QuartzJobListener("test-quartz-job-listener"),
                new QuartzTriggerListener("test-quartz-trigger-listener"),
                new QuartzSchedulerListener());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        scheduler.clear();
    }

    @Test
    public void testScheduleJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        JobDetail response = scheduler.getJobDetail(name, group);
        Assert.assertEquals(response.getKey().getName(), name);
        Assert.assertEquals(response.getKey().getGroup(), group);
    }

    @Test
    public void testAddJob() throws Exception {
        scheduler.addJob(jobDetail, false);
        JobDetail response = scheduler.getJobDetail(name, group);
        Assert.assertEquals(response.getKey().getName(), name);
        Assert.assertEquals(response.getKey().getGroup(), group);
    }

    @Test
    public void testDeleteJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        JobDetail response = scheduler.getJobDetail(name, group);
        Assert.assertEquals(response.getKey().getName(), name);
        Assert.assertEquals(response.getKey().getGroup(), group);
        scheduler.deleteJob(name, group);
        response = scheduler.getJobDetail(name, group);
        Assert.assertNull(response);
    }

    @Test
    public void testSuspendJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.suspendJob(name, group);
        JobDetail response = scheduler.getJobDetail(name, group);
        Assert.assertEquals(response.getKey().getName(), name);
        Assert.assertEquals(response.getKey().getGroup(), group);
    }

    @Test
    public void testResumeJob() throws Exception {
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.resumeJob(name, group);
        JobDetail response = scheduler.getJobDetail(name, group);
        Assert.assertEquals(response.getKey().getName(), name);
        Assert.assertEquals(response.getKey().getGroup(), group);
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
        return new JobDataMap();
    }
}