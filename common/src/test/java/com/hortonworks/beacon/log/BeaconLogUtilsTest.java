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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Beacon Log Test.
 */
public class BeaconLogUtilsTest {
    private static final String POLICY_NAME = "fsRepl";
    private static final String POLICY_ID = "/dc/source/"+POLICY_NAME;

    @BeforeClass
    public void setup() {
    }

    @Test
    public void testBeaconLogPolicy() {
        BeaconLogUtils.prefixPolicy(POLICY_NAME, POLICY_ID, POLICY_ID+"@1");
        BeaconLogUtils.Info info = new BeaconLogUtils.Info();
        info.setParameter(BeaconLogParams.POLICYNAME.name(), POLICY_NAME);
        info.setParameter(BeaconLogParams.POLICYID.name(), POLICY_ID);
        info.setParameter(BeaconLogParams.INSTANCEID.name(), POLICY_ID+"@1");
        info.resetPrefix();
        Assert.assertNotNull(info.getInfoPrefix());
        Assert.assertEquals(info.getParameter(BeaconLogParams.POLICYNAME.name()), POLICY_NAME);
        Assert.assertEquals(info.getParameter(BeaconLogParams.POLICYID.name()), POLICY_ID);
        Assert.assertEquals(info.getParameter(BeaconLogParams.INSTANCEID.name()), POLICY_ID+"@1");
        BeaconLogUtils.deletePrefix();
    }

    @AfterClass
    public void tearDown() {
        BeaconLogUtils.Info.reset();
    }
}
