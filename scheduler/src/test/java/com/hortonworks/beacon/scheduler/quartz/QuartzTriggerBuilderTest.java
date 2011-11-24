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

import java.util.Date;

import org.quartz.Trigger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.ServiceManager;

/**
 * QuartzTriggerBuilder Test class.
 */
public class QuartzTriggerBuilderTest {

    private static final int FREQUENCY_IN_SEC = 120;
    private static final String POLICY_ID = "dataCenter-Cluster-0-1488946092144-000000001";

    @BeforeClass
    public void setup() throws BeaconException{
        ServiceManager.getInstance().initialize(null, null);
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
