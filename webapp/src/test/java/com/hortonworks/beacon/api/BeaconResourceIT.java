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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.EventsResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventSeverity;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Integration tests for Beacon REST API.
 */
public class BeaconResourceIT extends BeaconIntegrationTest {

    public static final Logger LOG = LoggerFactory.getLogger(BeaconResourceIT.class);

    @BeforeClass
    public void setupBeaconServers() throws Exception {
        super.setupBeaconServers(null);
    }

    @AfterClass
    public void teardownBeaconServers() throws Exception {
        super.teardownBeaconServers();
    }


    public BeaconResourceIT() throws IOException {
        super();
    }

    @Test
    public void testSubmitCluster() throws Exception {
        String clusterName = SOURCE_CLUSTER;

        String fsEndPoint = srcDfsCluster.getURI().toString();
        //Submitting local cluster with name != beacon server cluster name should fail
        try {
            submitCluster(getRandomString("cluster"), getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint,
                    true);
            fail("Should have failed with status " + Response.Status.BAD_REQUEST.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode(), getStackTrace(e));
        }

        //Submitting cluster with same name as beacon server cluster name should succeed
        submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);

        //Submit local cluster again with same name should fail with conflict
        try {
            submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
            fail("Should have failed with status " + Response.Status.CONFLICT.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.CONFLICT.getStatusCode(), getStackTrace(e));
        }

        //Submit non-local cluster again with same name should fail with conflict
        try {
            submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, false);
            fail("Should have failed with status " + Response.Status.CONFLICT.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.CONFLICT.getStatusCode(), getStackTrace(e));
        }

        //Submitting another local cluster with different name should fail
        try {
            submitCluster(getRandomString("cluster"), getSourceBeaconServer(), getSourceBeaconServer(),
                    fsEndPoint, true);
            fail("Should have failed with status " + Response.Status.BAD_REQUEST.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode(), getStackTrace(e));
        }
    }

    private String getStackTrace(Exception e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    @Test
    public void testClusterUpdate() throws Exception {
        String clusterName = getRandomString("cluster");

        String fsEndPoint = srcDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("property-1", "value-1");
        customProperties.put("property-2", "value-2");
        customProperties.put(Cluster.ClusterFields.HIVE_WAREHOUSE.getName(), "s3a://beacon/");
        submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, customProperties,
                false);

        String clusterResponse = getClusterResponse(clusterName, getSourceBeaconServer());
        JSONObject clusterJson = new JSONObject(clusterResponse);
        assertEquals(clusterJson.getString("name"), clusterName);
        assertEquals(clusterJson.getString("description"), "source cluster description");
        assertEquals(clusterJson.getString("tags"), "[\"consumer\",\"owner\"]");
        JSONObject customProps = new JSONObject(clusterJson.getString("customProperties"));
        assertEquals(customProps.getString("property-1"), "value-1");
        assertEquals(customProps.getString("property-2"), "value-2");
        assertEquals(customProps.getString(Cluster.ClusterFields.CLOUDDATALAKE.getName()), "true");

        Properties properties = new Properties();
        properties.put("description", "updated source cluster description");
        properties.put("property-2", "updated-value-2");
        properties.put("property-3", "value-3");

        updateCluster(clusterName, getSourceBeaconServer(), properties);
        clusterResponse = getClusterResponse(clusterName, getSourceBeaconServer());
        JSONObject updatedClusterJson = new JSONObject(clusterResponse);
        assertEquals(updatedClusterJson.getString("name"), clusterName);
        assertEquals(updatedClusterJson.getString("description"), "updated source cluster description");
        JSONObject updatedCustomProps = new JSONObject(updatedClusterJson.getString("customProperties"));
        assertTrue(updatedCustomProps.isNull("property-1"));
        assertEquals(updatedCustomProps.getString("property-2"), "updated-value-2");
        assertEquals(updatedCustomProps.getString("property-3"), "value-3");
    }

    @Test
    public void testHiveClusterEncryptionAlgorithmSubmitAndUpdate() throws Exception {
        String clusterName = getRandomString("cluster");

        String fsEndPoint = srcDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("hive.cloud.encryptionAlgorithm", "SSE-KMS");
        customProperties.put("hive.cloud.encryptionKey", "someKey");
        customProperties.put(Cluster.ClusterFields.HIVE_WAREHOUSE.getName(), "s3a://beacon/");
        submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, customProperties,
                false);

        String clusterResponse = getClusterResponse(clusterName, getSourceBeaconServer());
        JSONObject clusterJson = new JSONObject(clusterResponse);
        JSONObject customProps = new JSONObject(clusterJson.getString("customProperties"));
        assertEquals(customProps.getString("hive.cloud.encryptionAlgorithm"), "AWS_SSEKMS");
        assertEquals(customProps.getString("hive.cloud.encryptionKey"), "someKey");
        assertEquals(customProps.getString(Cluster.ClusterFields.CLOUDDATALAKE.getName()), "true");

        Properties properties = new Properties();
        properties.put("hive.cloud.encryptionAlgorithm", "AES256");
        properties.put("hive.cloud.encryptionKey", "");

        updateCluster(clusterName, getSourceBeaconServer(), properties);
        clusterResponse = getClusterResponse(clusterName, getSourceBeaconServer());
        JSONObject updatedClusterJson = new JSONObject(clusterResponse);
        JSONObject updatedCustomProps = new JSONObject(updatedClusterJson.getString("customProperties"));
        assertEquals(updatedCustomProps.getString("hive.cloud.encryptionAlgorithm"), "AWS_SSES3");
        assertEquals(updatedCustomProps.getString("hive.cloud.encryptionKey"), "");
    }

    private void updatePolicy(String policyName, String beaconServer, PropertiesIgnoreCase properties,
                              Response.Status expectedResponse) throws BeaconClientException {

        BeaconClient client = new BeaconWebClient(beaconServer);
        int responseCode = Response.Status.OK.getStatusCode();
        try {
            client.updatePolicy(policyName, properties);
        } catch (BeaconClientException ex) {
            responseCode = ex.getStatus();
        }
        assertEquals(responseCode, expectedResponse.getStatusCode());
    }

    @Test
    public void testSubmitHACluster() throws Exception {
        String clusterName = getRandomString("cluster");

        String fsEndPoint = srcDfsCluster.getURI().toString();
        Map<String, String> clusterCustomProperties = new HashMap<>();
        String nameService = "source";
        String nameNode1 = "nn1";
        String nameNode2 = "nn2";
        clusterCustomProperties.put(BeaconConstants.DFS_NAMESERVICES, nameService);
        clusterCustomProperties.put(BeaconConstants.DFS_INTERNAL_NAMESERVICES, nameService);
        clusterCustomProperties.put(BeaconConstants.DFS_HA_NAMENODES + BeaconConstants.DOT_SEPARATOR
                + nameService, "nn1,nn2");
        String nameNodesRPCprefix = BeaconConstants.DFS_NN_RPC_PREFIX + BeaconConstants.DOT_SEPARATOR
                + nameService + BeaconConstants.DOT_SEPARATOR;
        clusterCustomProperties.put(nameNodesRPCprefix
                + nameNode1, "nn1:8020");
        clusterCustomProperties.put(nameNodesRPCprefix
                + nameNode2, "nn2:8020");
        submitCluster(clusterName, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint,
                clusterCustomProperties, false);
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testPairCluster() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER);

        // Testing the empty response
        String api = BASE_API + "policy/list?orderBy=name&fields=datasets,clusters";
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        validatePolicyList(api, 0, 0, names, types);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testClusterList() throws Exception {
        // Testing the empty results.
        String api = BASE_API + "cluster/list";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        int results = jsonObject.getInt("results");

        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        String sourceCluster = "a_" + SOURCE_CLUSTER;
        String targetCluster = "a_" + TARGET_CLUSTER;
        submitCluster(sourceCluster, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, false);
        submitCluster(targetCluster, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        int expectedTotalResults = totalResults + 2;
        int expectedResults = results + 2;
        api = BASE_API + "cluster/list";
        conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        totalResults = jsonObject.getInt("totalResults");
        results = jsonObject.getInt("results");
        assertEquals(totalResults, expectedTotalResults);
        assertEquals(results, expectedResults);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        assertEquals(cluster1.getString("name"), sourceCluster);
        assertEquals(cluster2.getString("name"), targetCluster);
        assertTrue(sourceCluster.equals(cluster1.getString("name")));
        assertTrue(targetCluster.equals(cluster2.getString("name")));

        // Using the offset and numResults parameter.
        api = BASE_API + "cluster/list?sortOrder=DESC&offset="+(expectedResults - 1)+"&numResults=2";
        conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        totalResults = jsonObject.getInt("totalResults");
        results = jsonObject.getInt("results");
        assertEquals(totalResults, expectedTotalResults);
        assertEquals(results, 1);
        cluster = jsonObject.getString("cluster");
        jsonArray = new JSONArray(cluster);
        cluster1 = jsonArray.getJSONObject(0);
        assertTrue(sourceCluster.equals(cluster1.getString("name")));
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testDeletePolicyPostSchedule() throws Exception {
        final String policyName = getRandomString("policy-delete");

        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));

        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));

        // Submit, schedule, wait for one instance start and delete policy
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());
        waitOnCondition(5000, "instance start", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.RUNNING.name());
            }
        });
        deletePolicy(policyName);

        // Submit, schedule and delete policy
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());
        Thread.sleep(10);
        deletePolicy(policyName);
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testGetCluster() throws Exception {
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        assertEquals(jsonObject.getString("beaconEndpoint"), getSourceBeaconServer());
    }

    @Test(dependsOnMethods = {"testPairCluster", "testSubmitCluster"})
    public void testDeletePolicyOnSourceCluster() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        String policyName = "deletePolicy";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test(dependsOnMethods = {"testPairCluster", "testSubmitCluster"})
    public void  testUpdatePolicy() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        String policyName = "policy-update-1";
        String type = FS;
        int freq = 10;
        submitAndSchedule(policyName, freq, dataSet, null, new Properties());
        String message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.RUNNING, freq, message);

        //change the properties
        PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
        properties.put("description", "updated policy description");
        properties.put("tde.sameKey", "true");
        properties.put("frequencyInSec", "3600");
        properties.put("distcpMapBandwidth", "25");
        properties.put("distcpMaxMaps", "10");
        properties.put("queueName", "test");
        properties.put("endTime", "2050-05-28T00:11:00");

        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.OK);

        //verify the properties on target cluster
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        verifyPolicyInfo(policyName, message);

        //verify the properties on source cluster
        message = getPolicyResponse(policyName, getSourceBeaconServer(), "");
        verifyPolicyInfo(policyName, message);

        // update operation directly on source cluster should fail.
        updatePolicy(policyName, getSourceBeaconServer(), properties, Response.Status.BAD_REQUEST);

        // endTime in the past should fail
        Date past = DateUtil.createDate(2000, 1, 1);
        properties.put("endTime", DateUtil.formatDate(past));
        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.BAD_REQUEST);

        //change of startTime for a running policy should fail
        Date future = DateUtil.createDate(2050, 1, 1);
        properties.put("startTime", DateUtil.formatDate(future));
        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.BAD_REQUEST);

        //Update to a DELETED policy should fail
        deletePolicy(policyName);
        properties.clear();
        properties.put("description", "just updating policy description");
        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.BAD_REQUEST);

        //Update to a non-existing policy should fail
        properties.clear();
        properties.put("description", "just updating policy description");
        updatePolicy("someRandomNonExistingPolicy_12345654321_asdfglkjh", getTargetBeaconServer(),
                properties, Response.Status.BAD_REQUEST);

        //Single property update should pass
        policyName = "policy-update-2";
        freq = 10;
        submitAndSchedule(policyName, freq, dataSet, null, new Properties());
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.RUNNING, freq, message);
        properties.clear();
        properties.put("description", "updated policy description again");
        properties.put("plugins", "RANGER,ATLAS");

        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.OK);

        //verify the properties on target cluster
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), 1);
        assertEquals(jsonObject.getInt("results"), 1);
        String policy = jsonObject.getString("policy");
        JSONArray jsonPolicyArray = new JSONArray(policy);
        JSONObject jsonPolicy = jsonPolicyArray.getJSONObject(0);
        assertEquals(jsonPolicy.get("name"), policyName);
        assertEquals(jsonPolicy.getInt("frequencyInSec"), 10);
        assertEquals(jsonPolicy.get("description"), "updated policy description again");
        assertEquals(jsonPolicy.getJSONArray("plugins").length(), 2);

        //Attempt to update custom props which are not allowed to update should fail
        properties.put("description", "updated policy description again");
        properties.put("source.setSnapshottable", "true");
        properties.put("tde.enabled", "true");

        updatePolicy(policyName, getTargetBeaconServer(), properties, Response.Status.BAD_REQUEST);
        deletePolicy(policyName);

        // Update to policy startTime, if policy has not yet started should pass
        Date future1 = DateUtil.createDate(2049, 1, 1);
        Date future2 = DateUtil.createDate(2050, 1, 1);
        Properties extraProps = new Properties();
        extraProps.put("startTime", DateUtil.formatDate(future1));
        submitAndSchedule(policyName, freq, dataSet, null, extraProps);
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        jsonObject = new JSONObject(message);
        policy = jsonObject.getString("policy");
        jsonPolicyArray = new JSONArray(policy);
        jsonPolicy = jsonPolicyArray.getJSONObject(0);
        assertEquals(jsonPolicy.get("name"), policyName);
        assertEquals(jsonPolicy.get("startTime"), DateUtil.formatDate(future1));

        PropertiesIgnoreCase updateProps = new PropertiesIgnoreCase();
        updateProps.put("startTime", DateUtil.formatDate(future2));
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);

        message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        jsonObject = new JSONObject(message);
        policy = jsonObject.getString("policy");
        jsonPolicyArray = new JSONArray(policy);
        jsonPolicy = jsonPolicyArray.getJSONObject(0);
        assertEquals(jsonPolicy.get("name"), policyName);
        assertEquals(jsonPolicy.get("startTime"), DateUtil.formatDate(future2));

        // Update to Policy startTime earlier than current time should fail
        updateProps.clear();
        updateProps.put("startTime", DateUtil.formatDate(past));
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.BAD_REQUEST);

        // Update to a suspended policy should not make it's quartz job active
        deletePolicy(policyName);
        updateProps.clear();
        final String policyName1 = "submit_suspend_update_policy";
        submitAndSchedule(policyName1, 60, dataSet, null, new Properties());
        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName1);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.suspendPolicy(policyName1);
        //ensures that only single instance of the policy exists
        updateProps.put("frequencyInSec", "10");
        updatePolicy(policyName1, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        callPolicyInstanceListAPI(policyName1, false);

    }

    @Test(dependsOnMethods = {"testSubmitCluster", "testPairCluster"})
    public void testPolicyUpdateRequireJobReschedule() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        final String policyName = "policy-update-job-reschedule";
        String type = FS;
        int freq = 5 * 60;
        String endTimeVal = "2050-05-28T00:11:00";
        Properties properties = new Properties();
        properties.put("endTime", endTimeVal);

        submitAndSchedule(policyName, freq, dataSet, null, properties);
        String message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.RUNNING, freq, message);
        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });

        //change the properties
        PropertiesIgnoreCase updateProps = new PropertiesIgnoreCase();
        updateProps.put("description", "updated policy description");
        updateProps.put("tde.sameKey", "true");
        updateProps.put("distcpMapBandwidth", "25");
        updateProps.put("distcpMaxMaps", "10");
        updateProps.put("queueName", "test");

        /* Update to just description, and/or tde.sameKey, and/or distcpMapBandwidth, and/or queueName, shouldn't
         reschedule the job. */
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        callPolicyInstanceListAPI(policyName, false);
        // Update to same endTime shouldn't reschedule the job.
        updateProps.clear();
        updateProps.put("endTime", endTimeVal);
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        callPolicyInstanceListAPI(policyName, false);
        // Update to same frequency shouldn't change the Job status
        updateProps.clear();
        properties.put("frequencyInSec", String.valueOf(freq));
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        callPolicyInstanceListAPI(policyName, false);
        // Update to differnt endTime should reschedule the job immediately
        updateProps.put("endTime", "2051-05-28T00:11:00");
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                return verifyLastInstanceStatus(targetClient, policyName, JobStatus.SUCCESS, 2);
            }
        });
        // Update to differnt frequency should reschedule the job immediately
        updateProps.put("frequencyInSec", String.valueOf(freq + 1));
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                return verifyLastInstanceStatus(targetClient, policyName, JobStatus.SUCCESS, 3);
            }
        });
    }


    private void verifyPolicyInfo(String policyName, String message) throws JSONException {
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), 1);
        assertEquals(jsonObject.getInt("results"), 1);
        String policy = jsonObject.getString("policy");
        JSONArray jsonPolicyArray = new JSONArray(policy);
        JSONObject jsonPolicy = jsonPolicyArray.getJSONObject(0);
        assertEquals(jsonPolicy.get("name"), policyName);
        assertEquals(jsonPolicy.getInt("frequencyInSec"), 3600);
        assertEquals(jsonPolicy.get("description"), "updated policy description");
        assertEquals(jsonPolicy.get("endTime"), "2050-05-28T00:11:00");
        JSONObject customProps = jsonPolicy.getJSONObject("customProperties");
        assertEquals(customProps.get("distcpMapBandwidth"), "25");
        assertEquals(customProps.get("distcpMaxMaps"), "10");
        assertEquals(customProps.get("queueName"), "test");
    }

    @Test(dependsOnMethods = {"testSubmitCluster", "testPairCluster"})
    public void testGetPolicy() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        String policyName = getRandomString("policy");
        String type = FS;
        int freq = 10;
        submitAndSchedule(policyName, freq, dataSet, dataSet, new Properties());
        String message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.RUNNING, freq, message);

        // On source cluster
        message = getPolicyResponse(policyName, getSourceBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.RUNNING, freq, message);

        //Delete policy
        deletePolicy(policyName);
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=true");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.DELETED, freq, message);

        // On source cluster after deletion
        message = getPolicyResponse(policyName, getSourceBeaconServer(), "?archived=true");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.DELETED, freq, message);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testDifferentHDFSPolicy() throws Exception {
        // Snapshot HDFS Policy
        String snapshotPolicyName = getRandomString("policy_1_");
        String baseReplicationPath = SOURCE_DIR + UUID.randomUUID().toString();
        String snapshotReplicationPath =  baseReplicationPath + "/policy/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(snapshotReplicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath, snapshotPolicyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(snapshotReplicationPath));

        //Get the old policy count
        String api =
                BASE_API + "policy/list?orderBy=creationTime&sortOrder=DESC&filterBy=sourcecluster:" + SOURCE_CLUSTER;
        String message = getPolicyListResponse(api, getTargetBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        int oldResults = jsonObject.getInt("results");
        int oldTotalResults = jsonObject.getInt("totalResults");

        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(snapshotReplicationPath, snapshotPolicyName)));
        submitAndSchedule(snapshotPolicyName, 10, snapshotReplicationPath, snapshotReplicationPath, new Properties());

        // HDFS Policy
        String policyName = getRandomString("policy_2_");
        String replicationPath = baseReplicationPath + "/policy-2/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());
        //start
        message = getPolicyListResponse(api, getTargetBeaconServer());
        JSONObject jsonObject1 = new JSONObject(message);
        int oldResults1 = jsonObject1.getInt("results");
        int oldTotalResults1 = jsonObject1.getInt("totalResults");
        //end

        List<String> names = Arrays.asList(policyName, snapshotPolicyName);
        List<String> types = Arrays.asList("FS", "FS");
        validatePolicyList(api, oldResults + 2, oldTotalResults + 2, names, types);

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
        targetClient.deletePolicy(snapshotPolicyName, false);
    }

    private void assertPolicyEntity(String dataSet, String policyName, String type, JobStatus status,
                                    int freq, String message) throws JSONException, IOException {
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), 1);
        assertEquals(jsonObject.getInt("results"), 1);
        String policy = jsonObject.getString("policy");
        JSONArray jsonPolicyArray = new JSONArray(policy);
        JSONObject jsonPolicy = jsonPolicyArray.getJSONObject(0);
        assertEquals(jsonPolicy.get("name"), policyName);
        assertEquals(jsonPolicy.getString("type"), type);
        assertEquals(jsonPolicy.getString("status"), status.name());
        assertEquals(jsonPolicy.getString("sourceDataset"), dataSet);
        assertEquals(jsonPolicy.getInt("frequencyInSec"), freq);
        assertEquals(jsonPolicy.getString("sourceCluster"), SOURCE_CLUSTER);
        assertEquals(jsonPolicy.getString("targetCluster"), TARGET_CLUSTER);
        assertEquals((jsonPolicy.getString("creationTime") != null), true);
        assertEquals((jsonPolicy.getString("startTime") != null), true);
        assertEquals((jsonPolicy.getString("endTime") != null), true);
        assertEquals(jsonPolicy.getString("user"), System.getProperty("user.name"));
        assertEquals(jsonPolicy.getInt("retryAttempts"), 3);
        assertEquals(jsonPolicy.getInt("retryDelay"), 120);

        // Source and target should have same number of custom properties.
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = mapper.readValue(jsonPolicy.getString("customProperties"),
                new TypeReference<Map<String, String>>(){});
        assertTrue(map.size() >= 7, "Entries: " + map.keySet().toString());

        List<String> list = mapper.readValue(jsonPolicy.getString("tags"), new TypeReference<List<String>>(){});
        assertEquals(list.size(), 2);
    }

    @Test(dependsOnMethods = {"testPairCluster", "testSubmitCluster"})
    public void testScheduleSuspendAndResumePolicy() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));

        final String policyName = getRandomString("policy");
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        waitOnCondition(15000, "first instance complete", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());

        // Suspend and check status on source and target
        String api = BASE_API + "policy/suspend/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getSourceBeaconServer());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getTargetBeaconServer());

        // Resume and check status on source and target
        String resumeApi = BASE_API + "policy/resume/" + policyName;
        conn = sendRequest(getTargetBeaconServer() + resumeApi, null, POST);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getTargetBeaconServer());

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testUnpairAfterSuspendPolicy() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));

        final String policyName = getRandomString("policy");
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        waitOnCondition(15000, "first instance complete", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());

        // Suspend and check status on source and target
        String api = BASE_API + "policy/suspend/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getSourceBeaconServer());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getTargetBeaconServer());

        // Unpair cluster fails when there is a policy in suspended state.
        unpairClusterFailed(getTargetBeaconServer(), SOURCE_CLUSTER);

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test(dependsOnMethods = {"testPairCluster", "testSubmitCluster"})
    public void testPlugin() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        Map<String, String> customProp = new HashMap<>();
        customProp.put("allowPluginsOnThisCluster", "true");
        final String policyName = "hdfsPolicy_plugin";
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        Path exportData = new Path(BeaconConfig.getInstance().getEngine().getPluginStagingPath(),
                new Path(replicationPath).getName());
        assertTrue(srcDfsCluster.getFileSystem().exists(exportData));
        assertTrue(tgtDfsCluster.getFileSystem().exists(exportData));
        Path path = new Path(exportData, "_SUCCESS");

        assertTrue(tgtDfsCluster.getFileSystem().exists(path));
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    //TODO enable test - disabled as it fails intermittently
    @Test(dependsOnMethods = "testPairCluster", enabled = false)
    public void testInstanceListing() throws Exception {
        final String policyName = getRandomString("hdfsPolicy");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Expecting four instances of the policy should be executed.
        Thread.sleep(55000);
        waitOnCondition(55000, "4 instances", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList myinstances = targetClient.listPolicyInstances(policyName);
                return myinstances != null && myinstances.getResults() == 4;
            }
        });

        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));

        // Test the list API
        JSONArray jsonArray = instanceListAPI(policyName, "&offset=-1", 4, 4);
        assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@4"));
        assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@3"));
        assertTrue(jsonArray.getJSONObject(2).getString("id").endsWith("@2"));
        assertTrue(jsonArray.getJSONObject(3).getString("id").endsWith("@1"));

        // using the offset parameter
        jsonArray = instanceListAPI(policyName, "&offset=2", 4, 2);
        assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@2"));
        assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@1"));

        // delete the policy and do listing
        deletePolicy(policyName);
        instanceListAPI(policyName, "&archived=false", 0, 0);

        jsonArray = instanceListAPI(policyName, "&archived=true", 4, 4);
        assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@4"));
        assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@3"));
        assertTrue(jsonArray.getJSONObject(2).getString("id").endsWith("@2"));
        assertTrue(jsonArray.getJSONObject(3).getString("id").endsWith("@1"));
    }

    private JSONArray instanceListAPI(String policyName, String queryParam, int totalResults, int results)
            throws IOException, JSONException {
        String server = getTargetBeaconServer();
        StringBuilder api = new StringBuilder(server + BASE_API + "instance/list");
        api.append("?").append("filterBy=");
        api.append("name").append(BeaconConstants.COLON_SEPARATOR).
                append(policyName).append(BeaconConstants.COMMA_SEPARATOR);
        api.append("type").append(BeaconConstants.COLON_SEPARATOR).append(FS).
                append(BeaconConstants.COMMA_SEPARATOR);
        api.append("endTime").append(BeaconConstants.COLON_SEPARATOR).
                append(DateUtil.formatDate(new Date()));
        api.append("&orderBy=endTime").append("&numResults=10");
        api.append(queryParam);
        HttpURLConnection conn = sendRequest(api.toString(), null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), totalResults);
        assertEquals(jsonObject.getInt("results"), results);
        return new JSONArray(jsonObject.getString("instance"));
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testFSDataList() throws Exception {
        String basePath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        String data1 = "data-1";
        String data2 = "data-2";
        String sourceDir1 =  basePath + data1;
        String sourceDir2 = basePath + data2;
        //Prepare source
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDir1));
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDir2));

        String listDataAPI = BASE_API + "file/list?filePath="+basePath;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + listDataAPI, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        assertEquals("Success", jsonObject.getString("message"));
        assertEquals(Integer.parseInt(jsonObject.getString("results")), 2);
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("fileList"));
        assertEquals(jsonArray.getJSONObject(0).get("pathSuffix"), data1);
        assertEquals(jsonArray.getJSONObject(0).get("type"), "DIRECTORY");
        assertEquals(jsonArray.getJSONObject(1).get("pathSuffix"), data2);
        assertEquals(jsonArray.getJSONObject(1).get("type"), "DIRECTORY");
    }

    //TODO enable test - disabled as it fails intermittently
    @Test(dependsOnMethods = "testPairCluster", enabled = false)
    public void testPolicyInstanceList() throws Exception {
        final String policy1 = getRandomString("policy-1");
        final String policy2 = getRandomString("policy-2");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        String sourceDirPolicy1 = replicationPath + policy1;
        String sourceDirPolicy2 = replicationPath + policy2;
        //Prepare source
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDirPolicy1));
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDirPolicy2));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(sourceDirPolicy1));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(sourceDirPolicy2));
        // file for replication
        srcDfsCluster.getFileSystem().create(new Path(sourceDirPolicy1, policy1));
        srcDfsCluster.getFileSystem().create(new Path(sourceDirPolicy2, policy2));

        tgtDfsCluster.getFileSystem().mkdirs(new Path(sourceDirPolicy1));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(sourceDirPolicy2));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(sourceDirPolicy1));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(sourceDirPolicy2));

        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy1, policy1)));
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy2, policy2)));
        // Submit and schedule two different policy
        submitAndSchedule(policy1, 60, sourceDirPolicy1, sourceDirPolicy1, new Properties());
        submitAndSchedule(policy2, 60, sourceDirPolicy2, sourceDirPolicy2, new Properties());

        // Expecting one instance of both the policy should be executed successfully.
        waitOnCondition(20000, policy1 + " first instance=SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policy1);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        waitOnCondition(5000, policy2 + " first instance=SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policy2);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });

        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy1, policy1)));
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy2, policy2)));

        // policy instance list API call
        callPolicyInstanceListAPI(policy1, false);
        callPolicyInstanceListAPI(policy2, false);

        deletePolicy(policy1);
        deletePolicy(policy2);
        callPolicyInstanceListAPI(policy1, true);
        callPolicyInstanceListAPI(policy2, true);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testPolicyInstanceListOnSource() throws Exception {
        String policy1 = getRandomString("policy-1");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policy1));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policy1)));

        // Submit and schedule policy
        submitAndSchedule(policy1, 60, replicationPath, replicationPath, new Properties());

        // Expecting one instance of the policy should be executed successfully.
        Thread.sleep(20000);
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policy1)));

        // policy instance list API call on source
        callPolicyInstanceListAPISource(policy1, false);

        // Delete the policy and do policy/instance/list on source
        deletePolicy(policy1);
        callPolicyInstanceListAPISource(policy1, true);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testPolicyType() throws Exception {
        String policyName = getRandomString("policy-1");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        submitAndSchedule(policyName, 60, new Path(replicationPath).toString(),
                new Path(replicationPath).toString(), new Properties());

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());

        StringBuilder api = new StringBuilder(getTargetBeaconServer() + BASE_API + "policy/info/" + policyName);
        HttpURLConnection connection = sendRequest(api.toString(), null, GET);
        int responseCode = connection.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = connection.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals("SUCCEEDED", jsonObject.getString("status"));
        assertEquals("Type=FS_SNAPSHOT", jsonObject.getString("message"));

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }


    @Test(dependsOnMethods = "testPairCluster")
    public void getEvents() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));

        // Before submit capture the older state
        String eventapi = BASE_API + "events/all?orderBy=eventEntityType&sortOrder=asc";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + eventapi, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());
        assertEquals("Success", jsonObject.getString("message"));
        int oldTotalRsults = Integer.parseInt(jsonObject.getString("totalResults"));
        int oldTotal = Integer.parseInt(jsonObject.getString("results"));
        int oldNumSynced = Integer.parseInt(jsonObject.getString("numSyncEvents"));

        String policyName = "policy_10";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());
        conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        response = getResponseMessage(inputStream);
        jsonObject = new JSONObject(response);
        status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());
        assertEquals("Success", jsonObject.getString("message"));
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 7);
        assertEquals(Integer.parseInt(jsonObject.getString("results")), 7);
        assertEquals(Integer.parseInt(jsonObject.getString("numSyncEvents")), 0);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("events"));
        assertEquals(jsonArray.getJSONObject(0).get("severity"), EventSeverity.INFO.getName());
        assertEquals(jsonArray.getJSONObject(0).get("eventType"), EventEntityType.CLUSTER.getName());
        assertEquals(jsonArray.getJSONObject(3).get("eventType"), EventEntityType.POLICY.getName());
        assertEquals(jsonArray.getJSONObject(6).get("eventType"), EventEntityType.SYSTEM.getName());

        conn = sendRequest(getSourceBeaconServer() + eventapi, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        response = getResponseMessage(inputStream);
        jsonObject = new JSONObject(response);
        status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());
        assertEquals("Success", jsonObject.getString("message"));
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), oldTotalRsults + 1);
        assertEquals(Integer.parseInt(jsonObject.getString("results")), oldTotal + 1);
        assertEquals(Integer.parseInt(jsonObject.getString("numSyncEvents")), oldNumSynced + 1);

        String startStr = DateUtil.getDateFormat().format(new Date().getTime() - 300000);
        String endStr = DateUtil.getDateFormat().format(new Date());
        eventapi = BASE_API + "events/entity/cluster?start="+startStr+"&end="+endStr;
        conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        response = getResponseMessage(inputStream);
        jsonObject = new JSONObject(response);
        status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());
        assertEquals("Success", jsonObject.getString("message"));
        jsonArray = new JSONArray(jsonObject.getString("events"));
        assertEquals(jsonArray.getJSONObject(0).get("eventType"), EventEntityType.CLUSTER.getName());

        eventapi = BASE_API + "events";
        conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testAbortPolicyInstance() throws Exception {
        String policyName = getRandomString("abort-policy");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Added some delay for allowing progress of policy instance execution.
        Thread.sleep(500);
        abortAPI(policyName);
        // Should execute two instances and second instance should be successful.
        Thread.sleep(25000);
        String server = getTargetBeaconServer();
        StringBuilder listAPI = new StringBuilder(server + BASE_API + "policy/instance/list/" + policyName);
        listAPI.append("?sortOrder=ASC");
        HttpURLConnection conn = sendRequest(listAPI.toString(), null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
        assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
        assertEquals(jsonArray.getJSONObject(0).getString("status"), JobStatus.KILLED.name());
        assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@2"));
        assertEquals(jsonArray.getJSONObject(1).getString("status"), JobStatus.SUCCESS.name());

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testSnapshotCleanupOnPolicySubmission() throws Exception {
        String policyName = getRandomString("snapshot-cleanup");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        Properties properties = new Properties();
        properties.setProperty(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, properties);

        // Added some delay for allowing progress of policy instance execution.
        Thread.sleep(500);
        deletePolicy(policyName);
        FileStatus[] fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length > 0);
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, properties);
        deletePolicy(policyName);
        fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertEquals(fileStatus.length, 0);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testSnapshotNoCreationOnPolicySubmission() throws Exception {
        String policyName = getRandomString("snapshot-cleanup");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Added some delay for allowing progress of policy instance execution.
        Thread.sleep(500);
        FileStatus[] fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length == 0);
        deletePolicy(policyName);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testSnapshotDisallowOnPolicyDisable() throws Exception {
        final String policyName = getRandomString("snapshot-cleanup");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        Properties props = new Properties();
        props.setProperty(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, props);

        waitOnCondition(15000, "first instance complete", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });
        FileStatus[] fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length > 0);
        fileStatus = tgtDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length > 0);

        PropertiesIgnoreCase updateProps = new PropertiesIgnoreCase();
        updateProps.setProperty(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "false");
        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(15000);
        boolean exThown = false;
        try {
            fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        } catch (org.apache.hadoop.ipc.RemoteException ex) {
            exThown = true;
        }
        Assert.assertTrue(exThown);
        exThown = false;
        try {
            fileStatus = tgtDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        } catch (org.apache.hadoop.ipc.RemoteException ex) {
            exThown = true;
        }
        Assert.assertTrue(exThown);
        deletePolicy(policyName);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testSnapshotOnPolicyEdit() throws Exception {
        String policyName = getRandomString("snapshot-cleanup");
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Added some delay for allowing progress of policy instance execution.
        Thread.sleep(500);

        PropertiesIgnoreCase updateProps = new PropertiesIgnoreCase();
        updateProps.put(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        updateProps.put("frequencyInSec", String.valueOf(16));

        updatePolicy(policyName, getTargetBeaconServer(), updateProps, Response.Status.OK);
        Thread.sleep(50000);

        FileStatus[] fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length > 0);
        deletePolicy(policyName);
    }

    private void abortAPI(String policyName) throws IOException, JSONException {
        StringBuilder abortAPI = new StringBuilder(getTargetBeaconServer() + BASE_API
                + "policy/instance/abort/" + policyName);
        HttpURLConnection connection = sendRequest(abortAPI.toString(), null, POST);
        int responseCode = connection.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = connection.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals("SUCCEEDED", jsonObject.getString("status"));
        assertTrue(jsonObject.getString("message").contains("[true]"));
    }

    @Test(dependsOnMethods = {"testPairCluster", "testSubmitCluster"})
    public void testRerunPolicyInstance() throws Exception {
        final String policyName = getRandomString("rerun-policy");
        DistributedFileSystem srcFileSystem = srcDfsCluster.getFileSystem();
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcFileSystem.mkdirs(new Path(replicationPath));
        DFSTestUtil.createFile(srcFileSystem, new Path(replicationPath, policyName),
                150*1024*1024, (short) 1, System.currentTimeMillis());
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());

        // Added some delay for allowing progress of policy instance execution.
        waitOnCondition(5000, "instance status = RUNNING", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.RUNNING.name());
            }
        });
        targetClient.abortPolicyInstance(policyName);

        // Use list API and check the status.
        waitOnCondition(5000, "instance status = KILLED", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.KILLED.name());
            }
        });

        //Rerun the instance and check status.
        targetClient.rerunPolicyInstance(policyName);

        // Wait for the job to complete successfully.
        waitOnCondition(50000, "instance status = SUCCESS", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instance = getFirstInstance(targetClient, policyName);
                return instance != null && instance.status.equals(JobStatus.SUCCESS.name());
            }
        });

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testBeaconMetricsServlet() throws Exception {
        //String fsEndPoint = srcDfsCluster.getURI().toString();
        //submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
        String api = BASE_API + "admin/metrics";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        int count = jsonObject.getJSONObject("timers").getJSONObject("api.beacon.cluster.submit").getInt("count");
        Assert.assertTrue(count>1, "Metric [api.beacon.cluster.submit] value should be greater than 1");
    }

    private PolicyInstanceList.InstanceElement getFirstInstance(BeaconClient client, String policyName)
            throws BeaconClientException {
        PolicyInstanceList myinstances = client.listPolicyInstances(policyName);
        if (myinstances.getElements().length > 0) {
            return myinstances.getElements()[myinstances.getElements().length - 1];
        }
        return null;
    }

    private boolean verifyLastInstanceStatus(BeaconClient client, String policyName, JobStatus jobStatus,
                                             int totalInstanceCount) throws BeaconClientException {
        PolicyInstanceList myinstances = client.listPolicyInstances(policyName);
        if (myinstances.getElements().length == totalInstanceCount) {
            PolicyInstanceList.InstanceElement lastInstance = myinstances.getElements()[0];
            return lastInstance.status.equals(jobStatus.name());
        } else {
            return false;
        }
    }

    private void waitOnCondition(int timeout, String message, Condition condition) throws Exception {
        long endTime = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTime) {
            if (condition.exit()) {
                return;
            }
            LOG.debug("Waiting for {}", message);
            Thread.sleep(100);
        }
        if (!condition.exit()) {
            fail("Timedout waiting for " + message);
        }
    }

    private interface Condition {
        boolean exit() throws BeaconClientException;
    }

    @Test
    public void testServerVersion() throws Exception {
        ServerVersionResult versionResult = targetClient.getServiceVersion();
        assertEquals(versionResult.getStatus(), "RUNNING");
        assertEquals(versionResult.getVersion(), System.getProperty(BeaconConstants.BEACON_VERSION_CONST));
    }

    //TODO enable the test
    @Test(enabled = false)
    public void testGetUserPrivileges() throws Exception {
        UserPrivilegesResult privileges = targetClient.getUserPrivileges();
        assertFalse(privileges.isHdfsSuperUser());

        //TODO add real test
    }

    @Test
    public void testServerStatus() throws Exception {
        ServerStatusResult statusResult = targetClient.getServiceStatus();
        assertEquals(statusResult.getStatus(), "RUNNING");
        assertEquals(statusResult.getPlugins(), "RANGER,ATLAS");
        assertEquals(statusResult.getSecurity(), "None");
        assertFalse(statusResult.isWireEncryptionEnabled());
        assertFalse(statusResult.doesRangerCreateDenyPolicy());
        assertTrue(statusResult.isTDEReplicationEnabled());
        assertTrue(statusResult.isCloudFSReplicationEnabled());
        assertTrue(statusResult.isCloudHiveReplicationWithClusterEnabled());
        assertFalse(statusResult.isCloudHosted());

        //assert that the flags are serialised as string
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + BASE_API + "admin/status", null, GET);
        InputStream inputStream = conn.getInputStream();
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("wireEncryption");
        assertEquals(status, "false");
        String snapshotReady = jsonObject.getString("enableSourceSnapshottable");
        assertEquals(snapshotReady, "true");
        String useSourceSnapshots = jsonObject.getString("enableSnapshotBasedReplication");
        assertEquals(useSourceSnapshots, "true");
        String clusterUpdateSupported = jsonObject.getString("clusterUpdateSupported");
        assertEquals(clusterUpdateSupported, "true");
        String policyEdit = jsonObject.getString("policy_edit");
        assertEquals(policyEdit, "true");
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testPolicyCompletionStatus() throws Exception {
        String policyName = "completed-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + File.separator;
        DistributedFileSystem srcFileSystem = srcDfsCluster.getFileSystem();
        srcFileSystem.mkdirs(new Path(replicationPath));
        srcFileSystem.mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        Properties properties = new Properties();
        int frequency = 10;
        Date endTime = new Date(System.currentTimeMillis() + frequency*1000);
        properties.setProperty("endTime", DateUtil.formatDate(endTime));
        submitAndSchedule(policyName, frequency, replicationPath, replicationPath, properties);

        // Wait for the completion and final status to be updated.
        Thread.sleep(frequency*1000);
        String response = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=false");
        verifyPolicyCompletionStatus(response, JobStatus.SUCCEEDED.name());
        // Submit another policy with same.
        DFSTestUtil.createFile(srcFileSystem, new Path(replicationPath, policyName + File.separator + "sample.txt"),
                200*1024*1024, (short) 1, System.currentTimeMillis());
        endTime = new Date(System.currentTimeMillis() + 3*frequency*1000);
        properties.setProperty("endTime", DateUtil.formatDate(endTime));
        submitAndSchedule(policyName, frequency, replicationPath, replicationPath, properties);
        Thread.sleep(3*frequency*1000);
        response = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=false");
        verifyPolicyCompletionStatus(response, JobStatus.SUCCEEDEDWITHSKIPPED.name());
        verifyPolicyStatus(policyName, JobStatus.SUCCEEDEDWITHSKIPPED, getTargetBeaconServer());

        // Submit one more policy with same name and abort the instance while it is running.
        tgtDfsCluster.getFileSystem().delete(new Path(replicationPath, policyName), true);
        endTime = new Date(System.currentTimeMillis() + 3*frequency*1000);
        properties.setProperty("endTime", DateUtil.formatDate(endTime));
        submitAndSchedule(policyName, frequency, replicationPath, replicationPath, properties);
        Thread.sleep(2*frequency*1000);
        abortAPI(policyName);
        Thread.sleep(frequency*1000);
        response = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=false");
        //start
        JSONObject jsonObject = new JSONObject(response);
        String policyArray = jsonObject.getString("policy");
        //end
        verifyPolicyCompletionStatus(response, JobStatus.FAILEDWITHSKIPPED.name());
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testAllowSnapshotOnSource() throws Exception {
        String policyName = "allow-snapshot-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        Properties properties = new Properties();
        properties.setProperty(FSDRProperties.ENABLE_SNAPSHOTBASED_REPLICATION.getName(), "true");
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, properties);
        Assert.assertTrue(isDirectorySnapshottable(srcDfsCluster.getFileSystem(), replicationPath)
                && isDirectorySnapshottable(tgtDfsCluster.getFileSystem(), replicationPath));

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }

    @Test
    public void testSubmitCloudCred() throws Exception {
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");

        CloudCred cloudCred = buildCloudCred("cloud-cred-submit", CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        String entityId = client.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);
    }

    @Test
    public void testUpdateCloudCred() throws Exception {
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");
        configs.put(Config.VERSION, "1");

        CloudCred cloudCred = buildCloudCred("cloud-cred-update", CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        String entityId = client.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);

        configs.clear();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.update");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.update");
        configs.put(Config.VERSION, "2");
        CloudCred updateCloudCred = buildCloudCred(cloudCred.getName(), cloudCred.getProvider(),
                cloudCred.getAuthType(), configs);
        client.updateCloudCred(entityId, updateCloudCred);
        CloudCred serverCloudCred = client.getCloudCred(entityId);
        assertEquals(serverCloudCred.getConfigs().get(Config.VERSION), "2");

        //Update the auth type
        configs.clear();
        configs.put(Config.VERSION, "3");
        updateCloudCred = buildCloudCred(cloudCred.getName(), cloudCred.getProvider(),
                CloudCred.AuthType.AWS_INSTANCEPROFILE, configs);
        client.updateCloudCred(entityId, updateCloudCred);
        serverCloudCred = client.getCloudCred(entityId);
        assertEquals(serverCloudCred.getAuthType(), CloudCred.AuthType.AWS_INSTANCEPROFILE);
        assertEquals(serverCloudCred.getConfigs().get(Config.VERSION), "3");
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testDeleteCloudCred() throws Exception {
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");

        CloudCred cloudCred = buildCloudCred("cloud-cred-delete", CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        String entityId = client.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);
        String credProviderPath = BeaconConfig.getInstance().getEngine().getCloudCredProviderPath();
        credProviderPath = credProviderPath + entityId + BeaconConstants.JCEKS_EXT;
        String[] credPath = credProviderPath.split(BeaconConstants.JCEKS_HDFS_FILE_REGEX);
        assertTrue(srcDfsCluster.getFileSystem().exists(new Path(credPath[1])));
        client.deleteCloudCred(entityId);
        assertFalse(srcDfsCluster.getFileSystem().exists(new Path(credPath[1])));
    }

    @Test
    public void testGetCloudCred() throws Exception {
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");
        configs.put(Config.VERSION, "1");

        CloudCred cloudCred = buildCloudCred("cloud-cred-get", CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        String entityId = client.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);

        CloudCred clientCloudCred = client.getCloudCred(entityId);
        assertEquals(clientCloudCred.getId(), entityId);
        assertEquals(clientCloudCred.getConfigs().size(), 1);
        assertEquals(clientCloudCred.getConfigs().get(Config.VERSION), "1");

        //Fail if required configs are missing
        configs.clear();
        String credName = getRandomString("cred");
        cloudCred = buildCloudCred(credName, CloudCred.Provider.AWS, CloudCred.AuthType.AWS_ACCESSKEY,
                configs);
        try {
            client.submitCloudCred(cloudCred);
            fail("Expected BeaconClientException");
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        }

        //Create AWS_INSTANCEPROFILE based credential
        configs.clear();
        configs.put(Config.VERSION, "2");
        cloudCred = buildCloudCred(credName, CloudCred.Provider.AWS, CloudCred.AuthType.AWS_INSTANCEPROFILE,
                configs);
        entityId = client.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);

        clientCloudCred = client.getCloudCred(entityId);
        assertEquals(clientCloudCred.getId(), entityId);
        assertEquals(clientCloudCred.getConfigs().size(), 1);
        assertEquals(clientCloudCred.getConfigs().get(Config.VERSION), "2");

        //Fail if there are extra password configs which are not required
        configs.clear();
        configs.put(Config.AWS_ACCESS_KEY, "accesskey");
        cloudCred = buildCloudCred(getRandomString("cred"), CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_INSTANCEPROFILE, configs);
        try {
            client.submitCloudCred(cloudCred);
            fail("Expected BeaconClientException");
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        }

        //Test delete cloud cred and then get should fail
        client.deleteCloudCred(entityId);
        try {
            client.getCloudCred(entityId);
            fail("Expected BeaconClientException");
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        }
    }

    @Test
    public void testListCloudCred() throws Exception {
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        CloudCredList cloudCredList = client.listCloudCred("provider=AWS", null, null, null, null);
        int initialLen = cloudCredList.getResults();
        int expectedLen = initialLen + 2;
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");

        CloudCred cloudCred1 = buildCloudCred(getRandomString("cred_1"), CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        CloudCred cloudCred2 = buildCloudCred(getRandomString("cred_2"), CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        String entityId1 = client.submitCloudCred(cloudCred1);
        Assert.assertNotNull(entityId1);

        String entityId2 = client.submitCloudCred(cloudCred2);
        Assert.assertNotNull(entityId2);

        cloudCredList = client.listCloudCred("provider=AWS", null, "DESC", null, null);
        assertEquals(cloudCredList.getResults(), expectedLen);
        assertEquals(cloudCredList.getTotalResults(), expectedLen);
        CloudCred[] elements = cloudCredList.getCloudCreds();
        assertEquals(elements.length, expectedLen);
        assertEquals(elements[1].getId(), entityId1);
        assertEquals(elements[0].getId(), entityId2);
    }

    private CloudCred buildCloudCred(String name, CloudCred.Provider provider, CloudCred.AuthType authType,
                                     Map<Config, String> configs) {
        CloudCred cloudCred = new CloudCred();
        cloudCred.setName(name);
        cloudCred.setAuthType(authType);
        cloudCred.setProvider(provider);
        cloudCred.setConfigs(configs);
        return cloudCred;
    }

    private void verifyPolicyCompletionStatus(String response, String expectedResponse) throws JSONException {
        JSONObject jsonObject = new JSONObject(response);
        String policyArray = jsonObject.getString("policy");
        JSONObject policyJson = new JSONArray(policyArray).getJSONObject(0);
        assertEquals(policyJson.getString("status"), expectedResponse);
    }

    private void callPolicyInstanceListAPI(String policyName, boolean isArchived) throws IOException, JSONException {
        String server = getTargetBeaconServer();
        StringBuilder api = new StringBuilder(server + BASE_API + "policy/instance/list/" + policyName);
        api.append("?").append("filterBy=");
        api.append("name").append(BeaconConstants.COLON_SEPARATOR).
                append(policyName).append(BeaconConstants.COMMA_SEPARATOR);
        api.append("type").append(BeaconConstants.COLON_SEPARATOR).append(FS).
                append(BeaconConstants.COMMA_SEPARATOR);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, 1);
        api.append("endTime").append(BeaconConstants.COLON_SEPARATOR).
                append(DateUtil.formatDate(cal.getTime()));
        api.append("&archived=").append(isArchived);
        HttpURLConnection conn = sendRequest(api.toString(), null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        LOG.debug("Response: {}", message);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getInt("totalResults"), 1);
        assertEquals(jsonObject.getInt("results"), 1);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
        assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
    }

    private void callPolicyInstanceListAPISource(String policyName, boolean isArchived)
            throws IOException, JSONException {
        String server = getSourceBeaconServer();
        StringBuilder api = new StringBuilder(server + BASE_API + "policy/instance/list/" + policyName);
        api.append("?").append("filterBy=");
        api.append("name").append(BeaconConstants.COLON_SEPARATOR).
                append("Random").append(BeaconConstants.COMMA_SEPARATOR);
        api.append("type").append(BeaconConstants.COLON_SEPARATOR).append(FS).
                append(BeaconConstants.COMMA_SEPARATOR);
        api.append("endTime").append(BeaconConstants.COLON_SEPARATOR).
                append(DateUtil.formatDate(new Date()));
        api.append("&archived=").append(isArchived);
        HttpURLConnection conn = sendRequest(api.toString(), null, GET);
        int responseCode = conn.getResponseCode();
        if (isArchived) {
            assertEquals(responseCode, Response.Status.OK.getStatusCode());
            InputStream inputStream = conn.getInputStream();
            Assert.assertNotNull(inputStream);
            String message = getResponseMessage(inputStream);
            JSONObject jsonObject = new JSONObject(message);
            assertEquals(jsonObject.getInt("totalResults"), 0);
            assertEquals(jsonObject.getInt("results"), 0);
        } else {
            assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
        }
    }

    private String getPolicyStatus(String policyName, String server) throws IOException {
        String api = BASE_API + "policy/status/" + policyName;
        HttpURLConnection conn = sendRequest(server + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        return getResponseMessage(inputStream);
    }

    private String getPolicyResponse(String policyName, String server, String queryParameter) throws IOException {
        String api = BASE_API + "policy/getEntity/" + policyName + queryParameter;
        HttpURLConnection conn = sendRequest(server + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        return getResponseMessage(inputStream);
    }

    private void verifyPolicyStatus(String policyName, JobStatus expectedStatus,
                                    String server) throws IOException, JSONException {
        String response = getPolicyStatus(policyName, server);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        assertEquals(status, expectedStatus.name());
        String name = jsonObject.getString("name");
        assertEquals(name, policyName);
    }

    private boolean isDirectorySnapshottable(DistributedFileSystem dfs, String path) throws IOException {
        SnapshottableDirectoryStatus[] snapshottableDirListing = dfs.getSnapshottableDirListing();
        boolean snapshottable = false;
        for (SnapshottableDirectoryStatus dir : snapshottableDirListing) {
            if (dir.getFullPath().toString().equalsIgnoreCase(path)) {
                snapshottable = true;
                break;
            }
        }
        return snapshottable;
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testPolicyList() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        String policy3 = "policy-3";
        submitAndSchedule(policy3, 10, dataSet, null, new Properties());

        String dataSetSource = dataSet+"-source";
        String dataSetTarget = dataSet+"-target";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSetSource));
        String policy2 = "policy-2";
        submitAndSchedule(policy2, 10, dataSetSource, dataSetTarget, new Properties());

        String policyNameFilter = StringFormat.format("name:{},name:{}", policy2, policy3);
        String api = BASE_API + "policy/list?orderBy=name&fields=datasets,clusters&filterBy=" + policyNameFilter;
        List<String> names = Arrays.asList(policy2, policy3);
        List<String> types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 2, names, types);

        String dataSet3 = dataSet+"3";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet3));
        String policy1 = "policy-1";
        submitAndSchedule(policy1, 10, dataSet3, null, new Properties());

        policyNameFilter = policyNameFilter + ",name:" + policy1;
        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER + "," + policyNameFilter;
        names = Arrays.asList(policy1, policy2, policy3);
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=targetcluster:" + TARGET_CLUSTER + "," + policyNameFilter;
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER
                + ",targetcluster:" + TARGET_CLUSTER + "," + policyNameFilter;
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:"+ SOURCE_CLUSTER + "|" + TARGET_CLUSTER
                + "," + policyNameFilter;
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=creationtime&filterBy=" + policyNameFilter;
        names = Arrays.asList("policy-3", "policy-2", "policy-1");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3,  3, names, types);

        //delete policies at the end
        targetClient.deletePolicy(policy1, false);
        targetClient.deletePolicy(policy2, false);
        targetClient.deletePolicy(policy3, false);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testDeletePolicy() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        String policyName = "deletePolicy";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());
        deletePolicy(policyName);

        String eventapi = BASE_API + "events/all?orderBy=eventEntityType&sortOrder=asc";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + eventapi, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());

        EventsResult events = sourceClient.getAllEvents();
        EventsResult.EventInstance expectedEvent = new EventsResult.EventInstance();
        expectedEvent.event = Events.DELETED.getName();
        expectedEvent.eventType = EventEntityType.POLICY.getName();
        expectedEvent.syncEvent = true;
        assertExists(events, expectedEvent);
    }

    @Test(dependsOnMethods = "testPairCluster")
    public void testPolicyListFields() throws Exception {
        final String policyName = "policy-list";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        waitOnCondition(50000, "4th instance start", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList myinstances = targetClient.listPolicyInstances(policyName);
                return myinstances != null && myinstances.getResults() == 4;
            }
        });
        int instanceCount = 2;
        String fields = "datasets,clusters,instances,executionType,customProperties,report";
        String api = BASE_API + "policy/list?orderBy=name&fields=" + fields + "&instanceCount=" + instanceCount
                + "&filterBy=name:" + policyName;
        String response = getPolicyListResponse(api, getTargetBeaconServer());
        JSONObject jsonObject = new JSONObject(response);
        JSONArray policyArray = new JSONArray(jsonObject.getString("policy"));
        JSONObject policyJson = new JSONObject(policyArray.getString(0));
        Assert.assertNotNull(policyJson.getString("targetDataset"), "targetDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceDataset"), "sourceDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceCluster"), "sourceCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("targetCluster"), "targetCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("executionType"), "executionType should not be null.");
        Assert.assertNotNull(policyJson.getString("customProperties"), "customProperties should not be null.");
        JSONObject policyReportJson = new JSONObject(policyJson.getString("report"));
        String lastSucceededInstance = policyReportJson.getString("lastSucceededInstance");
        Assert.assertNotNull(lastSucceededInstance, "lastSucceededInstance should be present in the report.");
        JSONArray instanceArray = new JSONArray(policyJson.getString("instances"));
        assertEquals(instanceArray.length(), instanceCount);
        JSONObject instanceJson3 = new JSONObject(instanceArray.getString(0));
        JSONObject instanceJson2 = new JSONObject(instanceArray.getString(1));
        assertTrue(instanceJson3.getString("id").endsWith("@4"));
        assertTrue(instanceJson2.getString("id").endsWith("@3"));
        assertEquals(policyName, instanceJson2.getString("name"));

        //Policy List API on source cluster
        response = getPolicyListResponse(api, getSourceBeaconServer());
        jsonObject = new JSONObject(response);
        policyArray = new JSONArray(jsonObject.getString("policy"));
        policyJson = new JSONObject(policyArray.getString(0));
        assertEquals(policyName, policyJson.getString("name"));
        Assert.assertNotNull(policyJson.getString("targetDataset"), "targetDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceDataset"), "sourceDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceCluster"), "sourceCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("targetCluster"), "targetCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("executionType"), "executionType should not be null.");
        Assert.assertNotNull(policyJson.getString("customProperties"), "customProperties should not be null.");
        instanceArray = new JSONArray(policyJson.getString("instances"));
        assertEquals(instanceArray.length(), 0);

        //delete policy at the end
        targetClient.deletePolicy(policyName, false);
    }
}
