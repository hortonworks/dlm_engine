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

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Test ReplicationPolicy builder.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterHelper.class, FSUtils.class})
public class ReplicationPolicyBuilderTest{

    protected static final String FS_ENDPOINT = "hdfs://localhost:8020";
    protected static final String SOURCE_DATASET = "hdfs://localhost:54136/apps/dr";
    protected static final String TARGET_DATASET = "hdfs://localhost:54137/apps/dr";
    protected static final String S3_TARGET_DATASET = "s3n://testBucket/apps/dr";
    protected static final String S3_TARGET_DATASET_2 = "s3n://testBucket/apps/dr1";

    @BeforeClass
    public static void setup() throws Exception {
        setHadoopConf();
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
    }

    @Before
    public void methodSetup() throws BeaconException, IOException, URISyntaxException {
        PowerMockito.mockStatic(ClusterHelper.class);
        PowerMockito.mockStatic(FSUtils.class);
        Cluster cluster = getCluster();
        PowerMockito.when(ClusterHelper.getLocalCluster()).thenReturn(cluster);
        PowerMockito.when(ClusterHelper.getActiveCluster(Mockito.anyString())).thenReturn(cluster);
        PowerMockito.when(ClusterHelper.isHDFSEnabled(cluster)).thenReturn(true);
        PowerMockito.when(FSUtils.isHCFS((Path) Mockito.any())).thenReturn(false);
    }

    @Test
    public void testBuildHdfsPolicy() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, null, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), "/apps/dr");
    }

    @Test
    public void testBuildHdfsPolicyWithBothDatasets() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, TARGET_DATASET, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), TARGET_DATASET);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHdfsPolicyNoTargetCluster() throws Exception {
        final String name = "hdfsPolicyNoTargetCluster";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, SOURCE_DATASET, null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
    }

    // Test is disabled. The isHCFS() needs to return true or false for different parameter.
    //@Test
    public void buildHcfsPolicy() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, S3_TARGET_DATASET, null);
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), SOURCE_DATASET);
        Assert.assertEquals(policy.getTargetDataset(), S3_TARGET_DATASET);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHcfsPolicyNoTargetDataset() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, S3_TARGET_DATASET, null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
    }

    @Test(expected = BeaconException.class)
    public void testBuildHcfsPolicyBothHCFSDatasets() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name, S3_TARGET_DATASET,
                S3_TARGET_DATASET_2, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAndGetDateWithInvalidDate() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, null, "backupCluster");
        policyProps.setProperty("startTime", "invalid");
        ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
    }

    private static void setHadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", "testS3KeyId");
        conf.set("fs.s3n.awsSecretAccessKey", "testS3AccessKey");
        conf.set("fs.azure.account.key.mystorage.blob.core.windows.net", "dGVzdEF6dXJlQWNjZXNzS2V5");
        FSUtils.setDefaultConf(conf);
    }

    public Cluster getCluster() {
        Cluster cluster = new Cluster();
        cluster.setName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
        return cluster;
    }
}
