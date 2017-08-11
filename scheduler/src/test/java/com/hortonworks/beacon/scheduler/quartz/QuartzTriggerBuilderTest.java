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

import java.util.Date;

import org.quartz.Trigger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * QuartzTriggerBuilder Test class.
 */
public class QuartzTriggerBuilderTest extends XTestCase{

    private static final int FREQUENCY_IN_SEC = 120;
    private static final String POLICY_ID = "dataCenter-Cluster-0-1488946092144-000000001";

    @BeforeClass
    public void setup() throws BeaconException{
        initializeServices(null);
    }
    @Test
    public void testCreateTriggerNeverEnding() throws Exception {
        Trigger trigger = QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                null, null, FREQUENCY_IN_SEC);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertNull(trigger.getEndTime(), "should be null for never ending job.");
        Assert.assertEquals(trigger.getKey().getName(), POLICY_ID);
        Assert.assertEquals(trigger.getKey().getGroup(), BeaconQuartzScheduler.START_NODE_GROUP);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFixedEndTimeException() throws Exception {
        // End time earlier than current time (start time will be current time)
        Date endTime = new Date(System.currentTimeMillis() - 60 * 1000); // 1 minute earlier
        QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                null, endTime, FREQUENCY_IN_SEC);
    }

    @Test
    public void testCreateTriggerFixedEndTime() throws Exception {
        Date endTime = new Date(System.currentTimeMillis() + 60 * 1000); // 1 minute later
        Trigger trigger = QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                null, endTime, FREQUENCY_IN_SEC);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getEndTime(), endTime);
        Assert.assertEquals(trigger.getKey().getName(), POLICY_ID);
        Assert.assertEquals(trigger.getKey().getGroup(), BeaconQuartzScheduler.START_NODE_GROUP);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFutureStartNeverEndingException() throws Exception {
        // Start time earlier than current time
        Date startTime = new Date(System.currentTimeMillis() - 60 * 1000); // 1 minute earlier
        QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                startTime, null, FREQUENCY_IN_SEC);
    }

    @Test
    public void testCreateTriggerFutureStartNeverEnding() throws Exception {
        Date startTime = new Date(System.currentTimeMillis() + 60 * 1000); // 1 minute later
        Trigger trigger = QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                startTime, null, FREQUENCY_IN_SEC);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getStartTime(), startTime);
        Assert.assertNull(trigger.getEndTime(), "should be null for never ending job.");
        Assert.assertEquals(trigger.getKey().getName(), POLICY_ID);
        Assert.assertEquals(trigger.getKey().getGroup(), BeaconQuartzScheduler.START_NODE_GROUP);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTriggerFutureStartEndException() throws Exception {
        long millis = System.currentTimeMillis();
        Date startTime = new Date(millis + 60 * 1000); // 1 minute later
        Date endTime = new Date(millis - 60 * 1000); // 1 minute earlier
        QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                startTime, endTime, FREQUENCY_IN_SEC);
    }

    @Test
    public void testCreateTriggerFutureStartEnd() throws Exception {
        long millis = System.currentTimeMillis();
        Date startTime = new Date(millis + 60 * 1000); // 1 minute later
        Date endTime = new Date(millis + 2 * 60 * 1000); // 1 minute earlier
        Trigger trigger = QuartzTriggerBuilder.createTrigger(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP,
                startTime, endTime, FREQUENCY_IN_SEC);
        Assert.assertNotNull(trigger, "trigger should not be null.");
        Assert.assertEquals(trigger.getStartTime(), startTime);
        Assert.assertEquals(trigger.getEndTime(), endTime);
        Assert.assertEquals(trigger.getKey().getName(), POLICY_ID);
        Assert.assertEquals(trigger.getKey().getGroup(), BeaconQuartzScheduler.START_NODE_GROUP);
    }
}
