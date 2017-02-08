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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test ReplicationPolicy builder.
 */
public class ReplicationPolicyBuilderTest {
    private static final String LOCAL_CLUSTER = "primaryCluster";

    @BeforeClass
    private void setup() throws Exception {
        setHadoopConf();
        BeaconConfig.getInstance().getEngine().setInTestMode(true);
        BeaconConfig.getInstance().getEngine().setLocalClusterName(LOCAL_CLUSTER);
    }

    @Test
    public void testBuildHdfsPolicy() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "hdfs://localhost:54136/apps/dr",
                null, "backupCluster");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);

        Assert.assertEquals(policy.getName(), name);
        Assert.assertEquals(policy.getSourceDataset(), "hdfs://localhost:54136/apps/dr");
        Assert.assertEquals(policy.getTargetDataset(), "/apps/dr");
    }

    @Test
    public void testBuildHdfsPolicyWithBothDatasets() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "hdfs://localhost:54136/apps/dr",
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
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "hdfs://localhost:54136/apps/dr",
                null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test
    public void testBuildHcfsPolicy() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "hdfs://localhost:54136/apps/dr",
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
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "s3n://testBucket/apps/dr",
                null, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "HCFS to HCFS replication is not allowed")
    public void testBuildHcfsPolicyBothHCFSDatasets() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = buildPolicyProps(name, "s3n://testBucket/apps/dr",
                "s3n://testBucket/apps/dr1", null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name);
    }

    private static void setHadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", "testS3KeyId");
        conf.set("fs.s3n.awsSecretAccessKey", "testS3AccessKey");
        conf.set("fs.azure.account.key.mystorage.blob.core.windows.net", "dGVzdEF6dXJlQWNjZXNzS2V5");
        FSUtils.setDefaultConf(conf);
    }

    private static PropertiesIgnoreCase buildPolicyProps(String name, String sourceDataset,
                                                         String targetDataset, String targetCluster)
            throws BeaconException {
        PropertiesIgnoreCase policyProps = new PropertiesIgnoreCase();
        policyProps.setProperty("name", name);
        policyProps.setProperty("type", "FS");
        if (StringUtils.isNotBlank(sourceDataset)) {
            policyProps.setProperty("sourceDataset", sourceDataset);
        }
        if (StringUtils.isNotBlank(targetDataset)) {
            policyProps.setProperty("targetDataset", targetDataset);
        }
        policyProps.setProperty("sourceCluster", LOCAL_CLUSTER);
        if (StringUtils.isNotBlank(targetCluster)) {
            policyProps.setProperty("targetCluster", targetCluster);
        }
        policyProps.setProperty("frequencyInSec", "120");
        return policyProps;
    }
}
