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
