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

package com.hortonworks.beacon;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.hortonworks.beacon.BeaconIDGenerator.PolicyIdField;
import static com.hortonworks.beacon.BeaconIDGenerator.generatePolicyId;
import static com.hortonworks.beacon.BeaconIDGenerator.getPolicyIdField;

/**
 * Test class for BeaconIDGenerator.
 */
public class BeaconIDGeneratorTest {

    private static final String DATA_CENTER = "NYC";
    private static final String CLUSTER_NAME = "FinanceCluster";
    private static final String DATA_CENTER_CLUSTER_NAME = DATA_CENTER + "$" + CLUSTER_NAME;
    private static final String POLICY_NAME = "DailyReplication";
    private static final int SERVER_INDEX = 0;

    @Test
    public void testGetPolicyIdWithDataCenter() throws Exception {
        String policyId = generatePolicyId(DATA_CENTER_CLUSTER_NAME, POLICY_NAME, SERVER_INDEX);
        Assert.assertEquals(DATA_CENTER, getPolicyIdField(policyId, PolicyIdField.DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.CLUSTER));
        Assert.assertEquals(POLICY_NAME, getPolicyIdField(policyId, PolicyIdField.POLICY_NAME));
        Assert.assertEquals(String.valueOf(SERVER_INDEX), getPolicyIdField(policyId, PolicyIdField.SERVER_INDEX));
        Assert.assertEquals(1, (int) Integer.valueOf(getPolicyIdField(policyId, PolicyIdField.COUNTER)));

        policyId = generatePolicyId(CLUSTER_NAME, POLICY_NAME, SERVER_INDEX);
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.CLUSTER));
        Assert.assertEquals(POLICY_NAME, getPolicyIdField(policyId, PolicyIdField.POLICY_NAME));
        Assert.assertEquals(String.valueOf(SERVER_INDEX), getPolicyIdField(policyId, PolicyIdField.SERVER_INDEX));
        Assert.assertEquals(2, (int) Integer.valueOf(getPolicyIdField(policyId, PolicyIdField.COUNTER)));

    }
}
