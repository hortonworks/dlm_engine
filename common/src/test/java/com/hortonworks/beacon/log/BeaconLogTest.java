/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Beacon Log Test.
 */
public class BeaconLogTest {
    private static final String USER_NAME = "userA";
    private static final String CLUSTER_NAME = "source";
    private static final String POLICY_NAME = "fsRepl";
    private static final String POLICY_ID = "/dc/source/"+POLICY_NAME;

    @BeforeClass
    public void setup() {
        BeaconLog.Info.remove();
    }

    @Test
    public void testInfoParams() {
        BeaconLog.Info info = new BeaconLog.Info();
        info.setParameter(BeaconLogParams.USER.name(), USER_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.USER.name()), USER_NAME);
        info.setParameter(BeaconLogParams.CLUSTER.name(), CLUSTER_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.CLUSTER.name()), CLUSTER_NAME);
    }

    @Test
    public void testBeaconLogPolicy() {
        BeaconLogUtils.setLogInfo(USER_NAME, CLUSTER_NAME, POLICY_NAME, POLICY_ID, POLICY_ID+"@1");
        BeaconLog.Info info = BeaconLog.Info.get();
        Assert.assertNotNull(info.getInfoPrefix());
        Assert.assertEquals(info.getParameter(BeaconLogParams.USER.name()), USER_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.CLUSTER.name()), CLUSTER_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.POLICYNAME.name()), POLICY_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.POLICYID.name()), POLICY_ID);
        Assert.assertEquals(info.getParameter(BeaconLogParams.INSTANCEID.name()), POLICY_ID+"@1");
        BeaconLogUtils.clearLogPrefix();
    }

    @Test
    public void testBeaconLogPrefix() {
        BeaconLog.Info info = new BeaconLog.Info();
        info.setParameter(BeaconLogParams.USER.name(), USER_NAME);
        BeaconLog log = BeaconLog.getLog(getClass());
        log.setMsgPrefix(info.createPrefix());
        Assert.assertEquals(log.getMsgPrefix(), "USER[userA]");
    }

    @Test
    public void testBeaconLog() {
        Logger logger = LoggerFactory.getLogger(getClass());
        BeaconLog bLog = new BeaconLog(logger);
        Assert.assertNull(bLog.getMsgPrefix());
        bLog.setMsgPrefix("prefix");
        Assert.assertEquals(bLog.getMsgPrefix(), "prefix");
        Assert.assertTrue(logger.isInfoEnabled());
    }

    @AfterClass
    public void tearDown() {
        BeaconLog.Info.reset();
        BeaconLog.Info.remove();
    }
}
