/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.PolicyBuilderTestUtil;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test ReplicationPolicy builder.
 */
public class ReplicationPolicyBuilderTest extends XTestCase {

    @BeforeClass
    private void setup() throws Exception {
        setHadoopConf();
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
        initializeServices(null);
    }

    @Test
    public void testBuildHdfsPolicy() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr", null, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), "hdfs://localhost:54136/apps/dr");
        Assert.assertEquals(policy.getTargetDataset(), "/apps/dr");
    }

    @Test
    public void testBuildHdfsPolicyWithBothDatasets() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                "hdfs://localhost:54137/apps/dr", "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), "hdfs://localhost:54136/apps/dr");
        Assert.assertEquals(policy.getTargetDataset(), "hdfs://localhost:54137/apps/dr");
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "Missing parameter: targetCluster")
    public void testBuildHdfsPolicyNoTargetCluster() throws Exception {
        final String name = "hdfsPolicyNoTargetCluster";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr", null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test
    public void testBuildHcfsPolicy() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                "s3n://testBucket/apps/dr", null);
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), "hdfs://localhost:54136/apps/dr");
        Assert.assertEquals(policy.getTargetDataset(), "s3n://testBucket/apps/dr");
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "Missing parameter: targetDataset")
    public void testBuildHcfsPolicyNoTargetDataset() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, "s3n://testBucket/apps/dr",
                null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "HCFS to HCFS replication is not allowed")
    public void testBuildHcfsPolicyBothHCFSDatasets() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, "s3n://testBucket/apps/dr",
                "s3n://testBucket/apps/dr1", null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidateAndGetDateWithInvalidDate() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
            "hdfs://localhost:54136/apps/dr", null, "backupCluster");
        policyProps.setProperty("startTime", "invalid");
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    private static void setHadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", "testS3KeyId");
        conf.set("fs.s3n.awsSecretAccessKey", "testS3AccessKey");
        conf.set("fs.azure.account.key.mystorage.blob.core.windows.net", "dGVzdEF6dXJlQWNjZXNzS2V5");
        FSUtils.setDefaultConf(conf);
    }
}
