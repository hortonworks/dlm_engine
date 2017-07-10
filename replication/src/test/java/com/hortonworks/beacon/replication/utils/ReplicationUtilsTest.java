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

package com.hortonworks.beacon.replication.utils;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Replication Utils.
 */
public class ReplicationUtilsTest extends XTestCase{
    private List<String> fsPolicyDataset = new ArrayList<>();
    private List<String> hivePolicyDataset = new ArrayList<>();

    @BeforeClass
    public void setup() throws BeaconException {
        fsPolicyDataset.add("/user/A/1/2");
        fsPolicyDataset.add("/user/A/1/2_1");
        fsPolicyDataset.add("/user/B");
        hivePolicyDataset.add("sales");
        hivePolicyDataset.add("sales_marketing");
        initializeServices(null);
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
