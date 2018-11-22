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

package com.hortonworks.beacon.api.cloud;

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.api.ResourceBaseTest;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.EncryptionAlgorithmType;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit Tests for cloud replication.
 */
public abstract class CloudReplicationTest extends ResourceBaseTest {

    protected Cluster targetCluster;

    protected Cluster sourceCluster;

    @BeforeClass
    public void submitClusters() throws Exception {
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        targetFs = testDataGenerator.getFileSystem(ClusterType.TARGET);
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, true);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, false);
        targetCluster = prepareTargetForCloudReplication(targetCluster);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(targetCluster.getName(), true);
    }

    private Cluster prepareTargetForCloudReplication(Cluster cluster) {
        //Test with target cluster without HDFS
        cluster.setFsEndpoint(null);
        cluster.setCustomProperties(getPropertiesTargetHiveCloudCluster());
        return cluster;
    }

    @AfterClass
    public void cleanup() throws Exception {
        targetClient.unpairClusters(targetCluster.getName(), true);
        targetClient.deleteCluster(targetCluster.getName());
        targetClient.deleteCluster(sourceCluster.getName());
    }

    public abstract Properties getPropertiesTargetHiveCloudCluster();

    public abstract String getCloudDataSet();

    public CloudCred getCloudCred() {
        return getCloudCred(testDataGenerator.getRandomString("cloudcred"));
    }

    public abstract CloudCred getCloudCred(String name);

    @Test
    public void testOnPremToCloudReplication() throws Exception{
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HDFSCloudPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                getCloudDataSet(), "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());

        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void pluginLineageForDefaultSetup() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("PluginHDFSCloudPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                getCloudDataSet(), "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(20000, "Get jobs for policy ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                String jobs = getJobs(policyName);
                return jobs != null && jobs.equals("FS,END-NODE");
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test(enabled = false)
    public void pluginLineageTestWithPreserveMeta() throws Exception {
        BeaconConfig.getInstance().getEngine().setPreserveMeta(true);
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("JobsHDFSCloudPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                getCloudDataSet(), "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(20000, "Get jobs for policy ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                String jobs = getJobs(policyName);
                return jobs != null && jobs.equals("RANGEREXPORT,ATLASEXPORT,FS,END-NODE");
            }
        });
        targetClient.deletePolicy(policyName, false);
        BeaconConfig.getInstance().getEngine().setPreserveMeta(false);
    }

    @Test
    public void testHdfsS3EncryptionBasedPolicy() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("CloudHDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        String sourceDataSet = getCloudDataSet();
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName,
                sourceDataSet, targetDataSet, "FS", 60,
                null, sourceCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testOnPremToCloudPolicyUpdate() throws Exception{
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HDFSCloudPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                getCloudDataSet(), "FS", 15,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        PropertiesIgnoreCase props = new PropertiesIgnoreCase();
        props.put(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        props.put(ReplicationPolicy.ReplicationPolicyFields.FREQUENCYINSEC.getName(), "20");
        targetClient.updatePolicy(policyName, props);
        waitOnCondition(50000, "Second Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getNthInstance(targetClient, policyName, 2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testCloudToOnPremPolicyUpdate() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("CloudHDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        String sourceDataSet = getCloudDataSet();
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName,
                sourceDataSet, targetDataSet, "FS", 15,
                null, sourceCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        PropertiesIgnoreCase props = new PropertiesIgnoreCase();
        props.put(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        props.put(ReplicationPolicy.ReplicationPolicyFields.FREQUENCYINSEC.getName(), "20");
        targetClient.updatePolicy(policyName, props);
        waitOnCondition(50000, "Second Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getNthInstance(targetClient, policyName, 2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void s3HdfsPluginLineageTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("LineageCloudHDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName,
                getCloudDataSet(), targetDataSet, "FS", 60,
                null, sourceCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(getCloudDataSet());
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(20000, "Get jobs for policy ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                String jobs = getJobs(policyName);
                return jobs != null && jobs.equals("RANGERIMPORT,FS,END-NODE");
            }
        });
        targetClient.deletePolicy(policyName, false);
    }


    @Test
    public void testHiveS3ReplicationTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HiveCloudPolicy");
        String sourceDataSet = policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        testDataGenerator.createHiveMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(10000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testHiveCloudOneToManyReplicationWithDiffTargetSameClusterShouldFailTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName1 = testDataGenerator.getRandomString("HiveCloudPolicy");
        String sourceDataSet = policyName1;
        String tgtDataSet = testDataGenerator.getRandomString("HiveTestDb");
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy1 = testDataGenerator.getPolicy(policyName1, sourceDataSet,
                tgtDataSet, "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        testDataGenerator.createHiveMocks(sourceDataSet);
        testDataGenerator.createFSMocks(tgtDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName1, policy1.asProperties());
        waitOnCondition(10000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        final String policyName2 = testDataGenerator.getRandomString("HiveCloudPolicy");
        ReplicationPolicy policy2 = testDataGenerator.getPolicy(policyName2, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        boolean exceptionThrown = false;
        try {
            targetClient.submitAndScheduleReplicationPolicy(policyName2, policy2.asProperties());
        } catch (BeaconClientException ex) {
            exceptionThrown = true;
            assertTrue(ex.getMessage()
                    .contains(StringFormat.format("Source dataset {} already in replication", sourceDataSet)));
        }
        assertTrue(exceptionThrown);
        targetClient.deletePolicy(policyName1, false);
    }

    @Test
    public void testHiveCloudOneToManyReplicationSameTargetShouldFailTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName1 = testDataGenerator.getRandomString("HiveCloudPolicy");
        String sourceDataSet = policyName1;
        String targetDataSet = testDataGenerator.getRandomString("HiveTestDb");
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy1 = testDataGenerator.getPolicy(policyName1, sourceDataSet, targetDataSet, "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(targetDataSet);
        testDataGenerator.createHiveMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName1, policy1.asProperties());
        waitOnCondition(10000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        final String policyName2 = testDataGenerator.getRandomString("HiveCloudPolicy");
        ReplicationPolicy policy2 = testDataGenerator.getPolicy(policyName2, sourceDataSet, targetDataSet, "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        boolean exceptionThrown = false;
        try {
            targetClient.submitAndScheduleReplicationPolicy(policyName2, policy2.asProperties());
        } catch (BeaconClientException ex) {
            assertTrue(ex.getMessage().contains("Target dataset already in replication"));
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        targetClient.deletePolicy(policyName1, false);
    }

    @Test
    public void testHiveCloudPolicyUpdate() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HiveCloudPolicy1");
        String sourceDataSet = policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 15,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        testDataGenerator.createHiveMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(10000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        PropertiesIgnoreCase props = new PropertiesIgnoreCase();
        props.put(ReplicationPolicy.ReplicationPolicyFields.FREQUENCYINSEC.getName(), "20");
        targetClient.updatePolicy(policyName, props);
        waitOnCondition(10000, "Second Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getNthInstance(targetClient, policyName, 2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testHdfsCloudEncryptionBasedPolicy() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HDFSCloudEncryptionBasedPolicy");
        String sourceDataSet = policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put(FSDRProperties.CLOUD_CRED.getName(), cloudCredId);
        cloudProps.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(),
                EncryptionAlgorithmType.AWS_SSES3.toString());
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                getCloudDataSet(), "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        when(targetFs.create(any(Path.class))).thenReturn(mock(FSDataOutputStream.class));
        targetClient.dryrunPolicy(policyName, policy.asProperties());
        policy.getCustomProperties().setProperty(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(), "dummyAlgo");
        boolean shouldThrowup = false;
        try {
            targetClient.dryrunPolicy(policyName, policy.asProperties());

        } catch (BeaconClientException ex) {
            String errorMessage = "Encryption algorithm dummyAlgo is not supported";
            assertTrue(ex.getMessage().endsWith(errorMessage));
            shouldThrowup = true;
        }
        assertTrue(shouldThrowup);
        policy.getCustomProperties().setProperty(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(),
                EncryptionAlgorithmType.AWS_SSES3.getName());
        shouldThrowup = false;
        try {
            targetClient.dryrunPolicy(policyName, policy.asProperties());
        } catch (BeaconClientException ex) {
            String errorMessage = "encryption algorithm " + EncryptionAlgorithmType.AWS_SSES3.getName()
                    + " is not supported";
            assertTrue(ex.getMessage().endsWith(errorMessage));
            shouldThrowup = true;
        }
        assertTrue(shouldThrowup);
    }

    @Test
    public void hdfsCloudOneToManyReplicationSrcTgtSrcTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName1 = testDataGenerator.getRandomString("HDFSCloudPolicy");
        final String policyName2 = testDataGenerator.getRandomString("HDFSCloudPolicy");

        String srcDataSet1 = SOURCE_DIR + policyName1;
        String tgtDataSet = getCloudDataSet();
        String srcDataSet2 = SOURCE_DIR + policyName2;

        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy1 = testDataGenerator.getPolicy(policyName1, srcDataSet1, tgtDataSet, "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(srcDataSet1);
        testDataGenerator.createFSMocks(srcDataSet2);

        targetClient.submitAndScheduleReplicationPolicy(policyName1, policy1.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });

        ReplicationPolicy policy2 = testDataGenerator.getPolicy(policyName2, tgtDataSet, srcDataSet2, "FS", 60,
                null, sourceCluster.getName(), cloudProps);
        targetClient.submitAndScheduleReplicationPolicy(policyName2, policy2.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName1, false);
        targetClient.deletePolicy(policyName2, false);
    }

    @Test
    public void hdfsCloudOneToManyReplicationTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName1 = testDataGenerator.getRandomString("HDFSCloudPolicy");
        final String policyName2 = testDataGenerator.getRandomString("HDFSCloudPolicy");

        String srcDataSet = SOURCE_DIR + policyName1;
        String tgtDataSet1 = getCloudDataSet();
        String tgtDataSet2 = getCloudDataSet();

        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy1 = testDataGenerator.getPolicy(policyName1, srcDataSet, tgtDataSet1, "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        testDataGenerator.createFSMocks(srcDataSet);

        targetClient.submitAndScheduleReplicationPolicy(policyName1, policy1.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });

        ReplicationPolicy policy2 = testDataGenerator.getPolicy(policyName2, srcDataSet, tgtDataSet2, "FS", 60,
                sourceCluster.getName(), null, cloudProps);
        targetClient.submitAndScheduleReplicationPolicy(policyName2, policy2.asProperties());
        waitOnCondition(50000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName1, false);
        targetClient.deletePolicy(policyName2, false);
    }

    @Test
    public void testSubmitCloudCred() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
    }

    @Test
    public void testCloudCredAlreadyExistException() throws Exception {
        String cloudCredName = testDataGenerator.getRandomString("CloudCred");
        CloudCred cloudCred = getCloudCred(cloudCredName);
        String cloudCredIdAccessKey = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredIdAccessKey);
        boolean expected = false;
        try {
            CloudCred cloudCredSasToken = getCloudCred(cloudCredName);
            String cloudCredIdSas = targetClient.submitCloudCred(cloudCredSasToken);
            assertNotNull(cloudCredIdSas);
        } catch (BeaconClientException e) {
            if (e.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                expected = true;
            }
        }
        assertTrue(expected);
    }

    @Test
    public void testDeleteCloudCred() throws Exception {
        /**
         * Creating and deleting the cloud cred without any policy should succeed.
         */
        String cloudCredName = testDataGenerator.getRandomString("Cloud-cred");
        CloudCred cloudCred = getCloudCred(cloudCredName);
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        targetClient.deleteCloudCred(cloudCredId);

        cloudCredName = testDataGenerator.getRandomString("Cloud-cred");
        cloudCred = getCloudCred(cloudCredName);
        cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("CloudHDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        String sourceDataSet = getCloudDataSet();
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName,
                sourceDataSet, targetDataSet, "FS", 60,
                null, sourceCluster.getName(), cloudProps);
        testDataGenerator.createFSMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        /**
         * deleting the cloud cred with any active policy should fail.
         */
        boolean exception = false;
        try {
            targetClient.deleteCloudCred(cloudCredId);
        } catch (BeaconClientException e) {
            if (e.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                exception = true;
            }
        }
        assertTrue(exception);
        targetClient.deletePolicy(policyName, false);
        /**
         * Deleting the cloud cred after deleting the policy should succeed.
         */
        targetClient.deleteCloudCred(cloudCredId);
    }


    @Test
    public void testHiveCloudReplicationPolicyUpdateTest() throws Exception {
        CloudCred cloudCred = getCloudCred();
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HiveS3Policy");
        String sourceDataSet = policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
        testDataGenerator.createHiveMocks(sourceDataSet);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policy.asProperties());
        waitOnCondition(10000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        ReplicationPolicy updatedPolicy = new ReplicationPolicy();
        updatedPolicy.setFrequencyInSec(240);
        targetClient.updatePolicy(policyName, updatedPolicy.asProperties());

        targetClient.deletePolicy(policyName, false);
    }

}
