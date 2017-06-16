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
package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Policy Validator.
 */
public class PolicyValidatorTest extends XTestCase{

    @BeforeClass
    private void setup() throws Exception {
        initializeServices(null);
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
    }

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = "Start time cannot be earlier than current time.*")
    public void testValidatePolicyStartDateBeforeNow() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", "2016-11-26T23:54:50Z", null);
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
        new PolicyValidator().validate(policy);
    }

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = "End time cannot be earlier than start time.*")
    public void testValidatePolicyEndDateBeforeStartDate() throws Exception {
        final String name = "hdfsPolicy-2";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", "2017-11-26T23:54:50Z", "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
        new PolicyValidator().validate(policy);
    }
}
