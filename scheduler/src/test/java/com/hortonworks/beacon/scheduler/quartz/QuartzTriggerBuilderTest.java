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
import org.quartz.Trigger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

/**
 * QuartzTriggerBuilder Test class.
 */
public class QuartzTriggerBuilderTest {

    private static final int FREQUENCY_IN_SEC = 120;

    private ReplicationJobDetails job;
    private QuartzTriggerBuilder triggerBuilder = new QuartzTriggerBuilder();

    @BeforeMethod
    public void setup() {
        job = new ReplicationJobDetails();
        job.setName("test-hdfs");
        job.setType(ReplicationType.FS.getName());
        job.setFrequency(FREQUENCY_IN_SEC);
    }

    @Test
    public void testCreateTriggerNeverEnding() throws Exception {
        Trigger trigger = triggerBuilder.createTrigger(job);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertNull(trigger.getEndTime(), "should be null for never ending job.");
        Assert.assertEquals(trigger.getKey().getName(), job.getName());
        Assert.assertEquals(trigger.getKey().getGroup(), job.getType());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFixedEndTimeException() throws Exception {
        // End time earlier than current time (start time will be current time)
        Date endTime = new Date(System.currentTimeMillis() - 60 * 1000); // 1 minute earlier
        job.setEndTime(endTime);
        triggerBuilder.createTrigger(job);
    }

    @Test
    public void testCreateTriggerFixedEndTime() throws Exception {
        Date endTime = new Date(System.currentTimeMillis() + 60 * 1000); // 1 minute later
        job.setEndTime(endTime);
        Trigger trigger = triggerBuilder.createTrigger(job);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getEndTime(), endTime);
        Assert.assertEquals(trigger.getKey().getName(), job.getName());
        Assert.assertEquals(trigger.getKey().getGroup(), job.getType());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFutureStartNeverEndingException() throws Exception {
        // Start time earlier than current time
        Date startTime = new Date(System.currentTimeMillis() - 60 * 1000); // 1 minute earlier
        job.setStartTime(startTime);
        triggerBuilder.createTrigger(job);
    }

    @Test
    public void testCreateTriggerFutureStartNeverEnding() throws Exception {
        Date startTime = new Date(System.currentTimeMillis() + 60 * 1000); // 1 minute later
        job.setStartTime(startTime);
        Trigger trigger = triggerBuilder.createTrigger(job);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getStartTime(), startTime);
        Assert.assertNull(trigger.getEndTime(), "should be null for never ending job.");
        Assert.assertEquals(trigger.getKey().getName(), job.getName());
        Assert.assertEquals(trigger.getKey().getGroup(), job.getType());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFutureStartEndException() throws Exception {
        long millis = System.currentTimeMillis();
        Date startTime = new Date(millis + 60 * 1000); // 1 minute later
        Date endTime = new Date(millis - 60 * 1000); // 1 minute earlier
        job.setStartTime(startTime);
        job.setEndTime(endTime);
        triggerBuilder.createTrigger(job);
    }

    @Test
    public void testCreateTriggerFutureStartEnd() throws Exception {
        long millis = System.currentTimeMillis();
        Date startTime = new Date(millis + 60 * 1000); // 1 minute later
        Date endTime = new Date(millis + 2 * 60 * 1000); // 1 minute earlier
        job.setStartTime(startTime);
        job.setEndTime(endTime);
        Trigger trigger = triggerBuilder.createTrigger(job);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getStartTime(), startTime);
        Assert.assertEquals(trigger.getEndTime(), endTime);
        Assert.assertEquals(trigger.getKey().getName(), job.getName());
        Assert.assertEquals(trigger.getKey().getGroup(), job.getType());
    }
}
