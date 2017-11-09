/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
        hivePolicyDataset.add("sales");
        hivePolicyDataset.add("sales_marketing");
        ServiceManager.getInstance().initialize(null, null);
    }

    @Test
    public void testFSDataset() {
        String sourceDataset = "/user/A";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/B";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/A/1/2/3";
        Assert.assertTrue(ReplicationUtils.checkFSDatasetConfliction(sourceDataset, fsPolicyDataset));

        sourceDataset = "/user/A/1/2";
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
