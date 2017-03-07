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

/**
 * Test class for BeaconIDGenerator.
 */
public class BeaconIDGeneratorTest {
    @Test
    public void testGetPolicyId() throws Exception {
        String policyId = BeaconIDGenerator.getPolicyId("NYC", "FinanceCluster", 0);
        String[] idParts = policyId.split(BeaconIDGenerator.SEPARATOR);
        Assert.assertEquals("NYC", idParts[0]);
        Assert.assertEquals("FinanceCluster", idParts[1]);
        Assert.assertEquals("0", idParts[2]);
        Assert.assertEquals(1, (int) Integer.valueOf(idParts[4]));
    }
}
