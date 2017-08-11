/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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

    private static final String SRC_DATA_CENTER = "NYC-SRC";
    private static final String TGT_DATA_CENTER = "NYC-TGT";
    private static final String CLUSTER_NAME = "FinanceCluster";
    private static final String SRC_CLUSTER = SRC_DATA_CENTER + "$" + CLUSTER_NAME;
    private static final String TGT_CLUSTER = TGT_DATA_CENTER + "$" + CLUSTER_NAME;
    private static final String POLICY_NAME = "DailyReplication";
    private static final int SERVER_INDEX = 0;

    @Test
    public void testGetPolicyIdWithDataCenter() throws Exception {
        String policyId = generatePolicyId(SRC_CLUSTER, TGT_CLUSTER, POLICY_NAME, SERVER_INDEX);
        Assert.assertEquals(SRC_DATA_CENTER, getPolicyIdField(policyId, PolicyIdField.SOURCE_DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.SOURCE_CLUSTER));
        Assert.assertEquals(TGT_DATA_CENTER, getPolicyIdField(policyId, PolicyIdField.TARGET_DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.TARGET_CLUSTER));
        Assert.assertEquals(POLICY_NAME, getPolicyIdField(policyId, PolicyIdField.POLICY_NAME));
        Assert.assertEquals(String.valueOf(SERVER_INDEX), getPolicyIdField(policyId, PolicyIdField.SERVER_INDEX));
        Assert.assertEquals(1, (int) Integer.valueOf(getPolicyIdField(policyId, PolicyIdField.COUNTER)));

        policyId = generatePolicyId(CLUSTER_NAME, CLUSTER_NAME, POLICY_NAME, SERVER_INDEX);
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.SOURCE_DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.SOURCE_CLUSTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.TARGET_DATA_CENTER));
        Assert.assertEquals(CLUSTER_NAME, getPolicyIdField(policyId, PolicyIdField.TARGET_CLUSTER));
        Assert.assertEquals(POLICY_NAME, getPolicyIdField(policyId, PolicyIdField.POLICY_NAME));
        Assert.assertEquals(String.valueOf(SERVER_INDEX), getPolicyIdField(policyId, PolicyIdField.SERVER_INDEX));
        Assert.assertEquals(2, (int) Integer.valueOf(getPolicyIdField(policyId, PolicyIdField.COUNTER)));
    }
}
