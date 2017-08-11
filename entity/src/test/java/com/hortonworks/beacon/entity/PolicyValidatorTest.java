/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = "End time cannot be earlier than current time.*")
    public void testValidatePolicyEndDateBeforeCurrent() throws Exception {
        final String name = "hdfsPolicy-1";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", null, "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
        new PolicyValidator().validate(policy);
    }

}
