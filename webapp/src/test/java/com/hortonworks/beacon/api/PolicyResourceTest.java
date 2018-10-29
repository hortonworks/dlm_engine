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

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hive.jdbc.HiveStatement;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test class for testing methods of {@link PolicyResource}.
 */
public class PolicyResourceTest extends ResourceBaseTest {

    public static final Logger LOG = LoggerFactory.getLogger(BeaconResourceIT.class);

    private Cluster sourceCluster;

    private Cluster targetCluster;

    @BeforeClass
    public void submitAndPairClusters() throws Exception {
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        targetFs = testDataGenerator.getFileSystem(ClusterType.TARGET);
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);
    }

    @Test
    public void testSubmitAndSchedulePolicy() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testSyncPolicy() throws Exception {
        ReplicationPolicy replicationPolicy = testDataGenerator.getPolicy();
        PropertiesIgnoreCase policyRequest = replicationPolicy.asProperties();
        policyRequest.put("id", testDataGenerator.getRandomString("SyncId"));
        sourceClient.syncPolicy(replicationPolicy.getName(), policyRequest, false);
    }

    @Test
    public void testPolicyDefinition() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        PolicyList policyList = targetClient.getPolicy(policyName);
        boolean policySubmitted = policyList.getElements().length > 0;
        assertTrue(policySubmitted);
        assertEquals(policyName, policyList.getElements()[0].name);
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testGetPolicy() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicyGet");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        PolicyList policyList = targetClient.getPolicyList("name", "name", "name:" + policyName, "asc", 0, 10);
        assertEquals(1, policyList.getResults());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test(enabled = false)
    public void testUpdatePolicy() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicyUpdate");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy replicationPolicy = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, replicationPolicy.asProperties());
        PolicyList oldDef = targetClient.getPolicy(policyName);
        assertEquals(120, (long) oldDef.getElements()[0].frequencyInSec);

        waitOnCondition(20000, "Get jobs for policy ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                String jobs = getJobs(policyName);
                return jobs != null && jobs.equals("RANGEREXPORT,RANGERIMPORT,ATLASEXPORT,ATLASIMPORT,FS,END-NODE");
            }
        });

        ReplicationPolicy updatedPolicy = new ReplicationPolicy();
        updatedPolicy.setFrequencyInSec(60);
        PropertiesIgnoreCase properties = updatedPolicy.asProperties();
        properties.put("plugins", "RANGER");
        targetClient.updatePolicy(policyName, properties);
        PolicyList newDef = targetClient.getPolicy(policyName);
        assertEquals(60, (long) newDef.getElements()[0].frequencyInSec);

        waitOnCondition(60000, "Get jobs for policy ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                RequestContext.setInitialValue();
                EntityManager entityManager = RequestContext.get().getEntityManager();
                String nativeQuery = "SELECT JOBS FROM BEACON_POLICY WHERE NAME = '" + policyName + "'";
                Query query = entityManager.createNativeQuery(nativeQuery);
                String jobs = (String) query.getSingleResult();
                return jobs != null && jobs.equals("RANGEREXPORT,RANGERIMPORT,FS,END-NODE");
            }
        });

        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testPolicyStatus() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(5000, "Policy Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                StatusResult statusResult = targetClient.getPolicyStatus(policyName);
                return statusResult != null && statusResult.getStatus().equals(Entity.EntityStatus.RUNNING);
            }
        });
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testSuspendPolicy() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        StatusResult statusResult = targetClient.getPolicyStatus(policyName);
        assertEquals(Entity.EntityStatus.RUNNING, statusResult.getStatus());
        targetClient.suspendPolicy(policyName);
        waitOnCondition(5000, "Policy Suspended ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                StatusResult statusResult = targetClient.getPolicyStatus(policyName);
                return statusResult != null && statusResult.getStatus().equals(Entity.EntityStatus.SUSPENDED);
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testResumePolicy() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(5000, "Policy Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                StatusResult statusResult = targetClient.getPolicyStatus(policyName);
                return statusResult != null && statusResult.getStatus().equals(Entity.EntityStatus.RUNNING);
            }
        });
        targetClient.suspendPolicy(policyName);
        waitOnCondition(15000, "Policy Suspended ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                StatusResult statusResult = targetClient.getPolicyStatus(policyName);
                return statusResult != null && statusResult.getStatus().equals(Entity.EntityStatus.SUSPENDED);
            }
        });
        targetClient.resumePolicy(policyName);
        waitOnCondition(15000, "Policy Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                StatusResult statusResult = targetClient.getPolicyStatus(policyName);
                return statusResult != null && statusResult.getStatus().equals(Entity.EntityStatus.RUNNING);
            }
        });
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testListInstance() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testOneToManyReplication() throws Exception {
        final String policyName1 = testDataGenerator.getRandomString("FsPolicy1");
        final String policyName2 = testDataGenerator.getRandomString("FsPolicy2");
        String replicationPath = SOURCE_DIR + policyName1;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest1 = testDataGenerator.getPolicy(policyName1, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName1, policyRequest1.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest2 = testDataGenerator.getPolicy(policyName2, replicationPath);
        policyRequest2.setTargetDataset(TARGET_DIR);
        boolean exceptionThrown = false;
        try {
            targetClient.submitAndScheduleReplicationPolicy(policyName2, policyRequest2.asProperties());
        } catch (BeaconClientException ex) {
            exceptionThrown = true;
            assertTrue(ex.getMessage()
                    .contains(StringFormat.format("Source dataset {} already in replication", replicationPath)));
        }
        assertTrue(exceptionThrown);
        targetClient.deletePolicy(policyName1, false);
    }

    @Test
    public void testOneToManyReplicationFailOnSameTarget() throws Exception {
        final String policyName1 = testDataGenerator.getRandomString("FsPolicy1");
        final String policyName2 = testDataGenerator.getRandomString("FsPolicy2");
        String replicationPath = SOURCE_DIR + policyName1;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest1 = testDataGenerator.getPolicy(policyName1, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName1, policyRequest1.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName1);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest2 = testDataGenerator.getPolicy(policyName2, replicationPath);
        boolean exceptionThrown = false;
        try {
            targetClient.submitAndScheduleReplicationPolicy(policyName2, policyRequest2.asProperties());
        } catch (BeaconClientException bex) {
            assertTrue(bex.getMessage().contains("Target dataset already in replication"));
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

    }

    @Test
    public void testEnableSnapshotBasedPolicy() throws Exception{
        final String policyName = testDataGenerator.getRandomString("FsSnapshotPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        HashMap<String, String> custProps = new HashMap<>();
        custProps.put(ReplicationPolicy.ReplicationPolicyFields.ENABLE_SNAPSHOTBASEDREPLICATION.getName(), "true");
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath, custProps);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testRejectEnableSnapshotBasedPolicyIfParentSnapshottable() throws Exception{
        final String policyName = testDataGenerator.getRandomString("FsSnapshotPolicy");
        String replicationPath = "/tmp/abc";
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        HashMap<String, String> custProps = new HashMap<>();
        custProps.put(ReplicationPolicy.ReplicationPolicyFields.ENABLE_SNAPSHOTBASEDREPLICATION.getName(), "true");
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath, custProps);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);

        final String policyName1 = testDataGenerator.getRandomString("FsSnapshotPolicy");
        replicationPath = "/tmp/abc/xyz";
        DistributedFileSystem dfs = (DistributedFileSystem)targetFs;
        doThrow(new RuntimeException("Nested snapshottable directories not allowed"))
                .when(dfs).allowSnapshot(new Path(replicationPath));
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        boolean exThrown = false;
        policyRequest = testDataGenerator.getPolicy(policyName1, replicationPath, custProps);
        try {
            targetClient.submitAndScheduleReplicationPolicy(policyName1, policyRequest.asProperties());
        } catch (BeaconClientException bEx) {
            exThrown = true;
        }
        assertTrue(exThrown);
    }

    @Test
    public void testEnableSnapshotOnUpdatePolicy() throws Exception{
        final String policyName = testDataGenerator.getRandomString("FsSnapshotPolicy");
        String replicationPath = "/tmp/abc";
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        policyRequest.setFrequencyInSec(15);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        PropertiesIgnoreCase custProps = new PropertiesIgnoreCase();
        custProps.put(ReplicationPolicy.ReplicationPolicyFields.ENABLE_SNAPSHOTBASEDREPLICATION.getName(), "true");
        targetClient.updatePolicy(policyName, custProps);
        waitOnCondition(20000, "Second Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getNthInstance(targetClient, policyName, 2);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testRejectEnableSnapshotBasedPolicyOnUpdateIfParentSnapshottable() throws Exception{
        final String policyName1 = testDataGenerator.getRandomString("FsSnapshotPolicy");
        String replicationPath = "/tmp/abc/xyz";
        DistributedFileSystem dfs = (DistributedFileSystem)targetFs;
        doThrow(new RuntimeException("Nested snapshottable directories not allowed"))
                .when(dfs).allowSnapshot(new Path(replicationPath));
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName1, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName1, policyRequest.asProperties());
        //Now edit a policy and enable snapshot, it should fail.
        PropertiesIgnoreCase props = new PropertiesIgnoreCase();
        props.put(ReplicationPolicy.ReplicationPolicyFields.ENABLE_SNAPSHOTBASEDREPLICATION.getName(), "true");

        boolean exThrown = false;
        try {
            targetClient.updatePolicy(policyName1, props);
        } catch (BeaconClientException bEx) {
            exThrown = true;
        }
        assertTrue(exThrown);
    }

    @Test(enabled = false)
    public void testRerunPolicyInstance() throws Exception {
        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(5000, "Instance Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.RUNNING.name());
            }
        });
        PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
        LOG.debug(StringFormat.format("Instance status before abort {}", instanceElement.status));
        Thread.sleep(500);
        targetClient.abortPolicyInstance(policyName);
        instanceElement = getFirstInstance(targetClient, policyName);
        LOG.debug(StringFormat.format("Instance status after abort {}", instanceElement.status));
        waitOnCondition(25000, "Instance Killed ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                LOG.debug(StringFormat.format("Instance status after abort {}", instanceElement.status));
                return instanceElement != null && instanceElement.status.equals(JobStatus.KILLED.name());
            }
        });
        targetClient.rerunPolicyInstance(policyName);
        waitOnCondition(5000, "Instance Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.RUNNING.name());
            }
        });
        targetClient.deletePolicy(policyName, false);

    }

    @Test
    public void testSubmitHivePolicy() throws Exception{
        final String policyName = testDataGenerator.getRandomString("HivePolicy");
        String replicationPath = policyName;
        testDataGenerator.createHiveMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath, "HIVE");
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        testDataGenerator.createHiveMocks(replicationPath);
        waitOnCondition(10000, "Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
    }

    /**
     * Will not run in remote cluster environment.
     * Reason: specific mocks are implemented to throw {@link com.hortonworks.beacon.exceptions.BeaconSuspendException}
     * @throws Exception
     */
    @Test
    public void testRerunHiveFailedAdmin() throws Exception {
        final String policyName = testDataGenerator.getRandomString("HivePolicy");
        String replicationPath = policyName;
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath, "HIVE");
        HiveServerClient hiveServerClientForThisTest = mock(HiveServerClient.class);
        HiveClientFactory.setHiveServerClient(hiveServerClientForThisTest);
        Statement statementForThisTest = mock(HiveStatement.class);
        SQLException exceptionForThisTest = mock(SQLException.class);
        when(hiveServerClientForThisTest.createStatement()).thenReturn(statementForThisTest);
        when(((HiveStatement) statementForThisTest).executeAsync(Matchers.anyString())).thenThrow(exceptionForThisTest);
        when(exceptionForThisTest.getErrorCode()).thenReturn(20001);
        when(hiveServerClientForThisTest.createStatement()).thenReturn(statementForThisTest);
        ResultSet resultSetReplStatus = mock(ResultSet.class);
        ResultSet resultSetReplDump = mock(ResultSet.class);
        mockForRerunHiveFailedAdmin(statementForThisTest, resultSetReplStatus, resultSetReplDump);
        testDataGenerator.createHiveMocks(replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
        waitOnCondition(10000, "Instance FAILED_ADMIN ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.FAILED_ADMIN.name());
            }
        });
        reset(statementForThisTest);
        mockForRerunHiveFailedAdmin(statementForThisTest, resultSetReplStatus, resultSetReplDump);
        when(((HiveStatement) statementForThisTest).executeAsync(Matchers.anyString())).thenReturn(true);
        targetClient.resumePolicy(policyName);
        targetClient.rerunPolicyInstance(policyName);
        waitOnCondition(15000, "Instance Running ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.RUNNING.name());
            }
        });
        waitOnCondition(25000, "Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        /**
         * Reset the mocks for this tests such that next tests picks the default mocks.
         */
        reset(statementForThisTest, resultSetReplStatus, resultSetReplDump, exceptionForThisTest);
        HiveServerClient hiveServerClientForOtherTests = mock(HiveServerClient.class);
        HiveClientFactory.setHiveServerClient(hiveServerClientForOtherTests);
        Statement statementForOtherTests = mock(HiveStatement.class);
        when(hiveServerClientForOtherTests.createStatement()).thenReturn(statementForOtherTests);
        when(((HiveStatement) statementForOtherTests).executeAsync(Matchers.anyString())).thenReturn(true);
        when(statementForOtherTests.executeQuery(Matchers.anyString())).thenAnswer(new Answer<ResultSet>() {
            @Override
            public ResultSet answer(InvocationOnMock invocation) throws Throwable {
                return mock(ResultSet.class);
            }
        });
        targetClient.deletePolicy(policyName, false);

    }

    private void mockForRerunHiveFailedAdmin(Statement statementForThisTest, ResultSet resultSetReplStatus,
                                             ResultSet resultSetReplDump) throws SQLException {
        when(statementForThisTest.executeQuery(Matchers.startsWith("REPL STATUS"))).thenReturn(resultSetReplStatus);
        when(resultSetReplStatus.next()).thenReturn(true);
        when(resultSetReplStatus.getString(1)).thenReturn("1");
        when(statementForThisTest.executeQuery(Matchers.startsWith("REPL DUMP"))).thenReturn(resultSetReplDump);
        when(resultSetReplDump.next()).thenReturn(true);
        when(resultSetReplDump.getString(1)).thenReturn(testDataGenerator.getRandomString("dumpdir"));
        when(resultSetReplDump.getString(2)).thenReturn("2");
    }


    private void submitAndSchedulePolicy(String replicationPath, String policyName)
            throws IOException, BeaconClientException {
        targetFs.mkdirs(new Path(replicationPath));
        testDataGenerator.createFSMocks(replicationPath);
        ReplicationPolicy policyRequest = testDataGenerator.getPolicy(policyName, replicationPath);
        targetClient.submitAndScheduleReplicationPolicy(policyName, policyRequest.asProperties());
    }
}
