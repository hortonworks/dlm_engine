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
