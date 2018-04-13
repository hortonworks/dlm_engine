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

package com.hortonworks.beacon.replication.utils;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.service.ServiceManager;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Replication Utils.
 */
public class ReplicationUtilsTest {
    private List<String> fsPolicyDataset = new ArrayList<>();
    private List<String> hivePolicyDataset = new ArrayList<>();

    @BeforeClass
    public void setup() throws BeaconException {
        fsPolicyDataset.add("/user/A/1/2");
        fsPolicyDataset.add("/user/A/1/2_1");
        fsPolicyDataset.add("/user/B");
        fsPolicyDataset.add("s3://user/A/");
        hivePolicyDataset.add("sales");
        hivePolicyDataset.add("sales_marketing");
        ServiceManager.getInstance().initialize(null, null);
    }

    @Test
    public void testFSDataset() throws BeaconException {
        String sourceDataset = "/user/A";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/B";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/A/1/2/3";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/A/1/2";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "s3://user/A/";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));


        sourceDataset = "/user/A/1/2_3";
        Assert.assertFalse(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/C";
        Assert.assertFalse(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/home/A";
        Assert.assertFalse(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));
    }

    @Test
    public void testHiveDataset() {
        String sourceDataset = "sales";
        Assert.assertTrue(ReplicationUtils.checkHiveDatasetConfliction(sourceDataset, hivePolicyDataset));

        sourceDataset = "marketing";
        Assert.assertFalse(ReplicationUtils.checkHiveDatasetConfliction(sourceDataset, hivePolicyDataset));
    }
}
