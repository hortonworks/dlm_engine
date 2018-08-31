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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit Tests for cloud replication.
 */
public class CloudReplicationTest extends ResourceBaseTest {

    private Cluster targetCluster;

    private Cluster sourceCluster;

    @BeforeClass
    public void submitClusters() throws Exception {
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        targetFs = testDataGenerator.getFileSystem(ClusterType.TARGET);
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, true);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, false);
        Properties customProps = getPropertiesTargetHiveCloudCluster();
        targetCluster.setCustomProperties(customProps);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(targetCluster.getName(), true);
    }

    private Properties getPropertiesTargetHiveCloudCluster() {
        Properties customProps = new Properties();
        customProps.setProperty("hive.metastore.warehouse.dir",
                testDataGenerator.getRandomString("s3://dummy/warehouse"));
        customProps.setProperty("hive.metastore.uris", "jdbc:hive2://local-" + ClusterType.TARGET);
        customProps.setProperty("hive.warehouse.subdir.inherit.perms", "false");
        customProps.setProperty("hive.repl.replica.functions.root.dir", "s3://dummy/warehouse-root");
        return customProps;
    }

    @Test
    public void submitCloudCredTest() throws Exception {
        CloudCred cloudCred = createAwsCloudCred(testDataGenerator.getRandomString("CloudCred-Create-test"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
    }

    @Test
    public void hdfsS3ReplicationTest() throws Exception {
        CloudCred cloudCred = createAwsCloudCred(testDataGenerator.getRandomString("Run-HDFS-S3-repl"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HDFSS3Policy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("s3://dummy/test"), "FS", 60,
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
    public void hdfsWasbReplicationTest() throws Exception {
        CloudCred cloudCred = createWasbCloudCredAccessKey(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HDFSWasbPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("wasb://test-wasb/test-path"), "FS", 60,
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
    public void wasbHdfsReplicationTest() throws Exception {
        CloudCred cloudCred = createWasbCloudCredAccessKey(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("WasbHdfsPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        String sourceDataSet = testDataGenerator.getRandomString("wasb://test-wasb/test-path");
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                targetDataSet, "FS", 60,
                null, sourceCluster.getName(), cloudProps);
        // Resetting the request context as next call create mocks is not an api call.
        RequestContext.setInitialValue();
        testDataGenerator.createFSMocks(FSPolicyHelper.getDatasetWithScheme(policy, Destination.SOURCE));
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
    public void s3HdfsReplicationTest() throws Exception {
        CloudCred cloudCred = createAwsCloudCred(testDataGenerator.getRandomString("Run-S3-HDFS-repl"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("S3HDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        String sourceDataSet = testDataGenerator.getRandomString("s3://dummy/test");
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
    public void testHiveS3ReplicationTest() throws Exception {
        CloudCred cloudCred = createAwsCloudCred(testDataGenerator.getRandomString("Run-HIVE-S3-repl"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HiveS3Policy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
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
    public void testHiveWASBReplicationTest() throws Exception {
        CloudCred cloudCred = createWasbCloudCredAccessKey(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("HiveWASBPolicy");
        String sourceDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        ReplicationPolicy policy = testDataGenerator.getPolicy(policyName, sourceDataSet,
                testDataGenerator.getRandomString("HiveTestDb"), "HIVE", 60,
                sourceCluster.getName(), targetCluster.getName(), cloudProps);
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
    public void testSubmitWasbCloudCred() throws Exception {
        CloudCred cloudCred = createWasbCloudCredAccessKey(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredIdAccessKey = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredIdAccessKey);
        CloudCred cloudCredSasToken = createWasbCloudCredSasToken(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredIdSas = targetClient.submitCloudCred(cloudCredSasToken);
        assertNotNull(cloudCredIdSas);
    }

    @Test
    public void testCloudCredAlreadyExistException() throws Exception {
        String cloudCredName = testDataGenerator.getRandomString("Submit-Cloud-Cred");
        CloudCred cloudCred = createWasbCloudCredAccessKey(cloudCredName);
        String cloudCredIdAccessKey = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredIdAccessKey);
        boolean expected = false;
        try {
            CloudCred cloudCredSasToken = createWasbCloudCredSasToken(cloudCredName);
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
    public void testUpdateWasbCloudCred() throws Exception {
        String cloudCredName = testDataGenerator.getRandomString("Submit-Cloud-Cred");
        CloudCred cloudCred = createWasbCloudCredAccessKey(cloudCredName);
        String cloudCredIdAccessKey = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredIdAccessKey);
        CloudCred savedCloudCred = targetClient.getCloudCred(cloudCredIdAccessKey);
        assertEquals(savedCloudCred.getName(), cloudCredName);
        assertEquals(savedCloudCred.getProvider().name(), CloudCred.Provider.WASB.name());
        assertEquals(savedCloudCred.getConfigs().get(CloudCred.Config.WASB_ACCOUNT_NAME), "test-account");
        CloudCred newCloudCred = createWasbCloudCredAccessKey(cloudCredName);
        newCloudCred.getConfigs().put(CloudCred.Config.WASB_ACCOUNT_NAME, "updated-test-account");
        targetClient.updateCloudCred(cloudCredIdAccessKey, newCloudCred);
        CloudCred updatedCloudCred = targetClient.getCloudCred(cloudCredIdAccessKey);
        assertEquals(updatedCloudCred.getConfigs().get(CloudCred.Config.WASB_ACCOUNT_NAME), "updated-test-account");
    }

    @Test
    public void deleteCloudCred() throws Exception {
        /**
         * Creating and deleting the cloud cred without any policy should succeed.
         */
        String cloudCredName = testDataGenerator.getRandomString("Run-S3-HDFS-repl");
        CloudCred cloudCred = createAwsCloudCred(cloudCredName);
        String cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        targetClient.deleteCloudCred(cloudCredId);

        cloudCredName = testDataGenerator.getRandomString("Run-S3-HDFS-repl");
        cloudCred = createAwsCloudCred(cloudCredName);
        cloudCredId = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredId);
        final String policyName = testDataGenerator.getRandomString("S3HDFSPolicy");
        String targetDataSet = SOURCE_DIR + policyName;
        Map<String, String> cloudProps = new HashMap<>();
        cloudProps.put("cloudCred", cloudCredId);
        String sourceDataSet = testDataGenerator.getRandomString("s3://dummy/test");
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


    private CloudCred createAwsCloudCred(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(CloudCred.Config.AWS_SECRET_KEY, "secret.key.value");
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        return cloudCred;
    }

    private CloudCred createWasbCloudCredAccessKey(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.WASB_ACCOUNT_NAME, "test-account");
        configs.put(CloudCred.Config.WASB_ACCESS_KEY, "test-access-key");
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.WASB,
                CloudCred.AuthType.WASB_ACCESSKEY, configs);
        return cloudCred;
    }

    private CloudCred createWasbCloudCredSasToken(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.WASB_CONTAINER_NAME, "test-container");
        configs.put(CloudCred.Config.WASB_ACCOUNT_NAME, "test-account-name");
        configs.put(CloudCred.Config.WASB_SAS_TOKEN, "test-sas-token");
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.WASB,
                CloudCred.AuthType.WASB_SAS_TOKEN, configs);
        return cloudCred;
    }

}
