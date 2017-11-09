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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.PolicyBuilderTestUtil;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;

import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test ReplicationPolicy builder.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterHelper.class, FSUtils.class})
public class ReplicationPolicyBuilderTest{

    private static final String SOURCE_DATASET = "hdfs://localhost:54136/apps/dr";
    private static final String TARGET_DATASET = "hdfs://localhost:54137/apps/dr";
    private static final String S3_TARGET_DATASET = "s3n://testBucket/apps/dr";
    private static final String S3_TARGET_DATASET_2 = "s3n://testBucket/apps/dr1";

    @BeforeClass
    public static void setup() throws Exception {
        setHadoopConf();
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
    }

    @Before
    public void methodSetup() throws BeaconException {
        PowerMockito.mockStatic(ClusterHelper.class);
        PowerMockito.mockStatic(FSUtils.class);
        Cluster cluster = getCluster();
        PowerMockito.when(ClusterHelper.getLocalCluster()).thenReturn(cluster);
        PowerMockito.when(FSUtils.isHCFS((Path) Mockito.any())).thenReturn(false);
    }

    @Test
    public void testBuildHdfsPolicy() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, null, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), "/apps/dr");
    }

    @Test
    public void testBuildHdfsPolicyWithBothDatasets() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, TARGET_DATASET, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), TARGET_DATASET);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHdfsPolicyNoTargetCluster() throws Exception {
        final String name = "hdfsPolicyNoTargetCluster";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, SOURCE_DATASET, null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    // Test is disabled. The isHCFS() needs to return true or false for different parameter.
    //@Test
    public void buildHcfsPolicy() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, S3_TARGET_DATASET, null);
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), S3_TARGET_DATASET);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHcfsPolicyNoTargetDataset() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, S3_TARGET_DATASET, null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHcfsPolicyBothHCFSDatasets() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, S3_TARGET_DATASET,
                S3_TARGET_DATASET_2, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAndGetDateWithInvalidDate() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, null, "backupCluster");
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

    private Cluster getCluster() {
        Cluster cluster = new Cluster();
        cluster.setName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
        return cluster;
    }
}
