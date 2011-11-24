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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.util.ClusterStatus;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventSeverity;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.plugin.service.BeaconInfoImpl;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
import com.hortonworks.beacon.test.PluginTest;
import com.hortonworks.beacon.util.DateUtil;

/**
 * Integration tests for Beacon REST API.
 */
public class BeaconResourceIT extends BeaconIntegrationTest {

    private static final String BASE_API = "/api/beacon/";
    private static final String NEW_LINE = System.lineSeparator();
    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String SOURCE_DFS = System.getProperty("beacon.data.dir") + "/dfs/" + SOURCE_CLUSTER;
    private static final String TARGET_DFS = System.getProperty("beacon.data.dir") + "/dfs/" + TARGET_CLUSTER;
    private static final String SOURCE_DIR = "/apps/beacon/snapshot-replication/sourceDir/";
    private static final String FS = "FS";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private MiniDFSCluster srcDfsCluster;
    private MiniDFSCluster tgtDfsCluster;

    public static final Logger LOG = LoggerFactory.getLogger(BeaconResourceIT.class);

    @BeforeClass
    public void setup() throws Exception {
        srcDfsCluster = startMiniHDFS(SOURCE_PORT, SOURCE_DFS);
        tgtDfsCluster = startMiniHDFS(TARGET_PORT, TARGET_DFS);
    }

    @AfterClass
    public void cleanup() {
        shutdownMiniHDFS(srcDfsCluster);
        shutdownMiniHDFS(tgtDfsCluster);
    }

    public BeaconResourceIT() throws IOException {
        super();
    }

    @Test
    public void testSubmitCluster() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        //Submitting local cluster with name != beacon server cluster name should fail
        try {
            submitCluster(randomString(), getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
            fail("Should have failed with status " + Response.Status.BAD_REQUEST.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        }

        //Submitting cluster with same name as beacon server cluster name should succeed
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);

        //Submit local cluster again with same name should fail with conflict
        try {
            submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
            fail("Should have failed with status " + Response.Status.CONFLICT.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.CONFLICT.getStatusCode());
        }

        //Submit non-local cluster again with same name should fail with conflict
        try {
            submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, false);
            fail("Should have failed with status " + Response.Status.CONFLICT.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.CONFLICT.getStatusCode());
        }

        //Submitting another local cluster with different name should fail
        try {
            submitCluster(randomString(), getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
            fail("Should have failed with status " + Response.Status.BAD_REQUEST.getStatusCode());
        } catch (BeaconClientException e) {
            assertEquals(e.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        }
    }

    @Test
    public void testClusterUpdate() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("property-1", "value-1");
        customProperties.put("property-2", "value-2");
        customProperties.put(Cluster.ClusterFields.HIVE_WAREHOUSE.getName(), "s3a://beacon/");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, customProperties,
                true);
        String clusterResponse = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject clusterJson = new JSONObject(clusterResponse);
        assertEquals(clusterJson.getString("name"), SOURCE_CLUSTER);
        assertEquals(clusterJson.getString("description"), "source cluster description");
        assertEquals(clusterJson.getString("tags"), "consumer,owner");
        JSONObject customProps = new JSONObject(clusterJson.getString("customProperties"));
        assertEquals(customProps.getString("property-1"), "value-1");
        assertEquals(customProps.getString("property-2"), "value-2");
        assertEquals(customProps.getString(Cluster.ClusterFields.CLOUDDATALAKE.getName()), "true");

        Properties properties = new Properties();
        properties.put("description", "updated source cluster description");
        properties.put("property-2", "updated-value-2");
        properties.put("property-3", "value-3");
        properties.put("tags", "sales");

        updateCluster(SOURCE_CLUSTER, getSourceBeaconServer(), properties);
        clusterResponse = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject updatedClusterJson = new JSONObject(clusterResponse);
        assertEquals(updatedClusterJson.getString("name"), SOURCE_CLUSTER);
        assertEquals(updatedClusterJson.getString("description"), "updated source cluster description");
        assertEquals(updatedClusterJson.getString("tags"), "consumer,owner,sales");
        JSONObject updatedCustomProps = new JSONObject(updatedClusterJson.getString("customProperties"));
        assertEquals(updatedCustomProps.getString("property-1"), "value-1");
        assertEquals(updatedCustomProps.getString("property-2"), "updated-value-2");
        assertEquals(updatedCustomProps.getString("property-3"), "value-3");
    }

    @Test
    public void testHiveClusterEncryptionAlgorithmSubmitAndUpdate() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("hive.cloud.encryptionAlgorithm", "SSE-KMS");
        customProperties.put("hive.cloud.encryptionKey", "someKey");
        customProperties.put(Cluster.ClusterFields.HIVE_WAREHOUSE.getName(), "s3a://beacon/");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, customProperties,
                true);
        String clusterResponse = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject clusterJson = new JSONObject(clusterResponse);
        JSONObject customProps = new JSONObject(clusterJson.getString("customProperties"));
        assertEquals(customProps.getString("hive.cloud.encryptionAlgorithm"), "AWS_SSEKMS");
        assertEquals(customProps.getString("hive.cloud.encryptionKey"), "someKey");
        assertEquals(customProps.getString(Cluster.ClusterFields.CLOUDDATALAKE.getName()), "true");

        Properties properties = new Properties();
        properties.put("hive.cloud.encryptionAlgorithm", "AES256");
        properties.put("hive.cloud.encryptionKey", "");

        updateCluster(SOURCE_CLUSTER, getSourceBeaconServer(), properties);
        clusterResponse = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject updatedClusterJson = new JSONObject(clusterResponse);
        JSONObject updatedCustomProps = new JSONObject(updatedClusterJson.getString("customProperties"));
        assertEquals(updatedCustomProps.getString("hive.cloud.encryptionAlgorithm"), "AWS_SSES3");
        assertEquals(updatedCustomProps.getString("hive.cloud.encryptionKey"), "");
    }

    private void updateCluster(String cluster, String beaconServer, Properties properties) throws IOException {
        String api = BASE_API + "cluster/" + cluster;

        StringBuilder data = new StringBuilder();
        for (String property : properties.stringPropertyNames()) {
            data.append(property).append("=").append(properties.getProperty(property))
                    .append(System.lineSeparator());
        }
        HttpURLConnection conn = sendRequest(beaconServer + api, data.toString(), PUT);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
    }

    @Test
    public void testSubmitHACluster() throws Exception {
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint,
                clusterCustomProperties, true);
    }

    @Test
    public void testPairCluster() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER);
    }

    @Test
    public void testPairOnlyOneClusterKerberized() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        String nnPricipal = "nnAdmin" + BeaconConstants.DOT_SEPARATOR + getTargetBeaconServerHostName();
        customProperties.put(BeaconConstants.NN_PRINCIPAL, nnPricipal);

        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, customProperties,
                false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, customProperties,
                true);
        pairClusterFailed(getSourceBeaconServer(), TARGET_CLUSTER);
    }

    @Test
    public void testPairClusterSuspendAndBackToPaired() throws Exception {
        String policyName = "pairCluster-SUCCESS-FAILED-SUCCESS-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER);
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());

        // Added some delay to allow policy instance execution.
        Thread.sleep(15000);

        // Verify policy instance status to be SUCCESS
        verifyLatestPolicyInstanceStatus(targetClient, policyName, JobStatus.SUCCESS);

        //Update the cluster to make only one cluster Kerberized.
        Properties properties = new Properties();
        String nnPricipal = "nnAdmin" + BeaconConstants.DOT_SEPARATOR + getTargetBeaconServerHostName();
        properties.put(BeaconConstants.NN_PRINCIPAL, nnPricipal);
        updateCluster(SOURCE_CLUSTER, getTargetBeaconServer(), properties);

        //Make sure the cluster pair is in 'SUSPENDED' state.
        verifyClusterPairStatus(getTargetBeaconServer(), ClusterStatus.SUSPENDED);

        //Added some delay to allow policy instance execution.
        Thread.sleep(15000);

        //The policy instance status should be in 'FAILED' state as the cluster pair is in 'SUSPENDED' state.
        verifyLatestPolicyInstanceStatus(targetClient, policyName, JobStatus.FAILED);

        //Now update the other cluster as well
        updateCluster(TARGET_CLUSTER, getTargetBeaconServer(), properties);

        //Make sure the cluster pair is in 'PAIRED' state.
        verifyClusterPairStatus(getTargetBeaconServer(), ClusterStatus.PAIRED);

        //Added some delay to allow policy instance execution.
        Thread.sleep(15000);

        //The policy instance status should be back to 'SUCCESS' as the cluster pair is back to 'PAIRED' state.
        verifyLatestPolicyInstanceStatus(targetClient, policyName, JobStatus.SUCCESS);
    }

    @Test
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
        assertEquals(totalResults, 0);
        assertEquals(results, 0);

        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
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
        assertEquals(totalResults, 2);
        assertEquals(results, 2);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        assertTrue(SOURCE_CLUSTER.equals(cluster1.getString("name")));
        assertTrue(TARGET_CLUSTER.equals(cluster2.getString("name")));

        // Using the offset and numResults parameter.
        api = BASE_API + "cluster/list?offset=1&numResults=5";
        conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        totalResults = jsonObject.getInt("totalResults");
        results = jsonObject.getInt("results");
        assertEquals(totalResults, 2);
        assertEquals(results, 1);
        cluster = jsonObject.getString("cluster");
        jsonArray = new JSONArray(cluster);
        cluster1 = jsonArray.getJSONObject(0);
        assertTrue(TARGET_CLUSTER.equals(cluster1.getString("name")));
    }

    @Test
    public void testPolicyList() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        // Testing the empty response
        String api = BASE_API + "policy/list?orderBy=name&fields=datasets,clusters";
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        validatePolicyList(api, 0, 0, names, types);

        submitAndSchedule("policy-3", 10, dataSet, null, new Properties());
        String dataSetSource = dataSet+"-source";
        String dataSetTarget = dataSet+"-target";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSetSource));
        submitAndSchedule("policy-2", 10, dataSetSource, dataSetTarget, new Properties());
        api = BASE_API + "policy/list?orderBy=name&fields=datasets,clusters";
        names = Arrays.asList("policy-2", "policy-3");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 2, names, types);

        String dataSet3 = dataSet+"3";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet3));
        submitAndSchedule("policy-1", 10, dataSet3, null, new Properties());

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=targetcluster:" + TARGET_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER
                + ",targetcluster:" + TARGET_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:"+ SOURCE_CLUSTER + "|" + TARGET_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=creationtime";
        names = Arrays.asList("policy-3", "policy-2", "policy-1");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=creationtime&sortOrder=DESC";
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);
    }

    @Test
    public void testPolicyListFields() throws Exception {
        String policyName = "policy-list";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        Thread.sleep(50000);
        int instanceCount = 2;
        String fields = "datasets,clusters,instances,executionType,customProperties,report";
        String api = BASE_API + "policy/list?orderBy=name&fields=" + fields + "&instanceCount=" + instanceCount;
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
    }

    @Test
    public void testDeleteLocalCluster() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
        String api = BASE_API + "cluster/delete/" + SOURCE_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), SOURCE_CLUSTER);
    }

    @Test
    public void testDeleteCluster() throws Exception {
        testPairCluster();
        //Delete cluster fails if they are paired
        try {
            targetClient.deleteCluster(TARGET_CLUSTER);
            fail("Delete cluster should have failed");
        } catch (BeaconClientException e) {
            assertTrue(e.getMessage().contains("Can't delete cluster"));
            assertTrue(e.getMessage().contains("as its paired with"));
        }

        //Delete cluster succeeds when not paired
        targetClient.unpairClusters(SOURCE_CLUSTER, false);
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), TARGET_CLUSTER);
    }

    @Test
    public void testDeletePolicyOnSourceCluster() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "deletePolicy";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testDeletePolicy() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 6);
        assertEquals(Integer.parseInt(jsonObject.getString("results")), 6);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("events"));
        assertEquals(jsonArray.getJSONObject(4).get("event"), Events.DELETED.getName());
        assertEquals(jsonArray.getJSONObject(4).get("eventType"), EventEntityType.POLICY.getName());
        assertEquals(jsonArray.getJSONObject(4).get("syncEvent"), true);
    }

    @Test
    public void testDeletePolicyPostSchedule() throws Exception {
        String policyName = "policy-delete";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit, schedule and delete policy
        submitScheduleDelete(policyName, replicationPath, replicationPath, 5000);
        submitScheduleDelete(policyName, replicationPath, replicationPath, 10);
    }

    private void submitScheduleDelete(String policyName, String sourceDataSet, String targetDataSet,
                                      int sleepTime) throws Exception {
        submitAndSchedule(policyName, 10, sourceDataSet, targetDataSet, new Properties());
        Thread.sleep(sleepTime);
        deletePolicy(policyName);
    }

    private void deletePolicy(String policyName) throws IOException, JSONException {
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        assertTrue(jsonObject.getString("message").contains("removed successfully"));
    }

    @Test
    public void testGetCluster() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, false);
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        assertEquals(jsonObject.getString("beaconEndpoint"), getSourceBeaconServer());
        assertEquals(jsonObject.getString("entityType"), "CLUSTER");
    }

    @Test
    public void testGetPolicy() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "policy";
        String type = FS;
        int freq = 10;
        submitAndSchedule(policyName, freq, dataSet, null, new Properties());
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

    @Test
    public void testDifferentHDFSPolicy() throws Exception {
        // Snapshot HDFS Policy
        String snapshotPolicyName = "snapshot-hdfs-policy";
        String baseReplicationPath = SOURCE_DIR + UUID.randomUUID().toString();
        String snapshotReplicationPath =  baseReplicationPath + "/policy/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(snapshotReplicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath, snapshotPolicyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(snapshotReplicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(snapshotReplicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(snapshotReplicationPath, snapshotPolicyName)));
        submitAndSchedule(snapshotPolicyName, 10, snapshotReplicationPath, snapshotReplicationPath, new Properties());

        // HDFS Policy
        String policyName = "hdfs-policy-test";
        String replicationPath = baseReplicationPath + "/policy-1/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        submitAndSchedule(policyName, 10, replicationPath, replicationPath, new Properties());

        String api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER;
        List<String> names = Arrays.asList(policyName, snapshotPolicyName);
        List<String> types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 2, names, types);
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
        assertEquals(jsonPolicy.getString("user"), System.getProperty("user.name"));
        assertEquals(jsonPolicy.getInt("retryAttempts"), 3);
        assertEquals(jsonPolicy.getInt("retryDelay"), 120);

        // Source and target should have same number of custom properties.
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = mapper.readValue(jsonPolicy.getString("customProperties"),
                new TypeReference<Map<String, String>>(){});
        assertEquals(map.size(), 7);

        List<String> list = mapper.readValue(jsonPolicy.getString("tags"), new TypeReference<List<String>>(){});
        assertEquals(list.size(), 2);
    }

    @Test
    public void testScheduleSuspendAndResumePolicy() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "hdfsPolicy";
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        Thread.sleep(15000);
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
    }

    @Test
    public void testUnpairAfterSuspendPolicy() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "hdfsPolicy_1";
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        Thread.sleep(15000);
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
    }

    @Test
    public void testPlugin() throws Exception {
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();

        Map<String, String> customProp = new HashMap<>();
        customProp.put("allowPluginsOnThisCluster", "true");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, customProp,
                true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, customProp,
                false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, customProp,
                false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, customProp,
                true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "hdfsPolicy_plugin";
        submitAndSchedule(policyName, 120, replicationPath, replicationPath, new Properties());
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        Thread.sleep(35000);
        Path pluginStagingPath = new Path(new BeaconInfoImpl().getStagingDir(), PluginTest.getPluginName());
        Path exportData = new Path(pluginStagingPath, new Path(replicationPath).getName());
        assertTrue(srcDfsCluster.getFileSystem().exists(exportData));
        assertTrue(tgtDfsCluster.getFileSystem().exists(exportData));
        Path path = new Path(exportData, "_SUCCESS");

        assertTrue(tgtDfsCluster.getFileSystem().exists(path));
        assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());
    }

    @Test
    public void testUnpairClusters() throws Exception {
        String dataSet = "/tmp" + UUID.randomUUID().toString();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        unpairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);

        // Pair, unpair and list
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        validateListClusterWithPeers(true);
        unpairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        validateListClusterWithPeers(false);
        // Same unpair operation again.
        unpairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER);
        validateListClusterWithPeers(false);

        unpairWrongClusters(getTargetBeaconServer(), OTHER_CLUSTER);


        // Pair cluster - submit policy - UnPair Cluster
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "policy";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());
        unpairClusterFailed(getTargetBeaconServer(), SOURCE_CLUSTER);
    }

    private void validateListClusterWithPeers(boolean hasPeers) throws Exception {
        String api = BASE_API + "cluster/list?fields=peers";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        int results = jsonObject.getInt("results");
        assertEquals(totalResults, 2);
        assertEquals(results, 2);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        assertTrue(SOURCE_CLUSTER.equals(cluster1.getString("name")));
        assertTrue(TARGET_CLUSTER.equals(cluster2.getString("name")));

        JSONArray cluster1Peers = new JSONArray(cluster1.getString("peers"));
        JSONArray cluster2Peers = new JSONArray(cluster2.getString("peers"));
        if (hasPeers) {
            assertTrue(TARGET_CLUSTER.equals(cluster1Peers.get(0)));
            assertTrue(SOURCE_CLUSTER.equals(cluster2Peers.get(0)));
        } else {
            assertTrue(cluster1Peers.length() == 0);
            assertTrue(cluster2Peers.length() == 0);
        }
    }

    @Test
    public void testInstanceListing() throws Exception {
        String policyName = "hdfsPolicy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Expecting four instances of the policy should be executed.
        Thread.sleep(55000);
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

    @Test
    public void testFSDataList() throws Exception {
        String basePath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        String data1 = "data-1";
        String data2 = "data-2";
        String sourceDir1 =  basePath + data1;
        String sourceDir2 = basePath + data2;
        //Prepare source
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDir1));
        srcDfsCluster.getFileSystem().mkdirs(new Path(sourceDir2));

        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);

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
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("fileList"));
        assertEquals(jsonArray.getJSONObject(0).get("pathSuffix"), data1);
        assertEquals(jsonArray.getJSONObject(0).get("type"), "DIRECTORY");
        assertEquals(jsonArray.getJSONObject(1).get("pathSuffix"), data2);
        assertEquals(jsonArray.getJSONObject(1).get("type"), "DIRECTORY");
    }

    @Test
    public void testPolicyInstanceList() throws Exception {
        String policy1 = "policy-1";
        String policy2 = "policy-2";
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

        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy1, policy1)));
        assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy2, policy2)));
        // Submit and schedule two different policy
        submitAndSchedule(policy1, 60, sourceDirPolicy1, sourceDirPolicy1, new Properties());
        submitAndSchedule(policy2, 60, sourceDirPolicy2, sourceDirPolicy2, new Properties());

        // Expecting one instance of both the policy should be executed successfully.
        Thread.sleep(20000);
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

    @Test
    public void testPolicyInstanceListOnSource() throws Exception {
        String policy1 = "policy-1";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policy1));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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

    @Test
    public void testPolicyType() throws Exception {
        String policyName = "policy-1";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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
    }


    @Test
    public void getEvents() throws Exception {
        String dataSet = "/tmp/" + UUID.randomUUID().toString();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "policy";
        submitAndSchedule(policyName, 10, "/tmp", null, new Properties());

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());
        String eventapi = BASE_API + "events/all?orderBy=eventEntityType&sortOrder=asc";
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
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
        assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 5);
        assertEquals(Integer.parseInt(jsonObject.getString("results")), 5);
        assertEquals(Integer.parseInt(jsonObject.getString("numSyncEvents")), 1);

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
    }

    @Test
    public void testAbortPolicyInstance() throws Exception {
        String policyName = "abort-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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
    }

    @Test
    public void testSnapshotCleanupOnPolicySubmission() throws Exception {
        String policyName = "snapshot-cleanup";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Added some delay for allowing progress of policy instance execution.
        Thread.sleep(500);
        deletePolicy(policyName);
        FileStatus[] fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length > 0);
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());
        deletePolicy(policyName);
        fileStatus = srcDfsCluster.getFileSystem().listStatus(new Path(replicationPath, ".snapshot"));
        Assert.assertTrue(fileStatus.length == 0);

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

    @Test
    public void testRerunPolicyInstance() throws Exception {
        final String policyName = "rerun-policy";
        DistributedFileSystem srcFileSystem = srcDfsCluster.getFileSystem();
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + "/";
        srcFileSystem.mkdirs(new Path(replicationPath));
        DFSTestUtil.createFile(srcFileSystem, new Path(replicationPath, policyName),
                150*1024*1024, (short) 1, System.currentTimeMillis());
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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
    }

    private PolicyInstanceList.InstanceElement getFirstInstance(BeaconClient client, String policyName)
            throws BeaconClientException {
        PolicyInstanceList myinstances = client.listPolicyInstances(policyName);
        if (myinstances.getElements().length > 0) {
            return myinstances.getElements()[myinstances.getElements().length - 1];
        }
        return null;
    }

    private void waitOnCondition(int timeout, String message, Condition condition) throws Exception {
        long endTime = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTime) {
            if (condition.exit()) {
                return;
            }
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
        assertEquals(statusResult.getPlugins(), "None");
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
    }

    @Test
    public void testPolicyCompletionStatus() throws Exception {
        String policyName = "completed-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + File.separator;
        DistributedFileSystem srcFileSystem = srcDfsCluster.getFileSystem();
        srcFileSystem.mkdirs(new Path(replicationPath));
        srcFileSystem.mkdirs(new Path(replicationPath, policyName));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
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

        // Submit one more policy with same name and abort the instance while it is running.
        tgtDfsCluster.getFileSystem().delete(new Path(replicationPath, policyName), true);
        endTime = new Date(System.currentTimeMillis() + 3*frequency*1000);
        properties.setProperty("endTime", DateUtil.formatDate(endTime));
        submitAndSchedule(policyName, frequency, replicationPath, replicationPath, properties);
        Thread.sleep(2*frequency*1000);
        abortAPI(policyName);
        Thread.sleep(frequency*1000);
        response = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=false");
        verifyPolicyCompletionStatus(response, JobStatus.FAILEDWITHSKIPPED.name());
    }

    @Test
    public void testAllowSnapshotOnSource() throws Exception {
        String policyName = "allow-snapshot-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath));
        srcDfsCluster.getFileSystem().mkdirs(new Path(replicationPath, policyName));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        Properties properties = new Properties();
        properties.setProperty(FSDRProperties.SOURCE_SETSNAPSHOTTABLE.getName(), "true");
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, properties);
        Assert.assertTrue(isDirectorySnapshottable(srcDfsCluster.getFileSystem(), replicationPath)
                && isDirectorySnapshottable(tgtDfsCluster.getFileSystem(), replicationPath));
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

    @Test
    public void testDeleteCloudCred() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);

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
    public void testCloudCredDeleteWithPolicy() throws Exception {
        String policyName = "cloud-policy";
        String replicationPath = SOURCE_DIR + UUID.randomUUID().toString() + File.separator;
        DistributedFileSystem srcFileSystem = srcDfsCluster.getFileSystem();
        srcFileSystem.mkdirs(new Path(replicationPath));
        DFSTestUtil.createFile(srcFileSystem, new Path(replicationPath, policyName),
                1024, (short) 1, System.currentTimeMillis());
        String srcFsEndPoint = srcDfsCluster.getURI().toString();

        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);

        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "dummy.access.key");
        configs.put(Config.AWS_SECRET_KEY, "dummy.secret.key");

        String cloudCredName = "cloud-cred";
        CloudCred cloudCred = buildCloudCred(cloudCredName, CloudCred.Provider.AWS,
                CloudCred.AuthType.AWS_ACCESSKEY, configs);
        String entityId = sourceClient.submitCloudCred(cloudCred);
        Assert.assertNotNull(entityId);
        String s3Path = "s3://dummy-bucket/"+policyName+"";
        String policyData = getPolicyData(policyName, "FS", 60,
                replicationPath, s3Path, SOURCE_CLUSTER, null, entityId);

        java.nio.file.Path policyFile = null;
        boolean policySubmit = false;
        try {
            policyFile = createTempFile(policyData, "policy_");
            sourceClient.submitAndScheduleReplicationPolicy(policyName, policyFile.toString());
            policySubmit = true;
            sourceClient.deleteCloudCred(entityId);
            Assert.fail("cloud cred delete operation should have failed.");
        } catch (BeaconClientException e) {
            Assert.assertEquals(e.getMessage(),
                    "Active cloud policies are present. Operation can not be performed.");
        } finally {
            if (policyFile != null) {
                Files.deleteIfExists(policyFile);
            }
            if (!policySubmit) {
                Assert.fail("policy submission have failed. Test failed.");
            }
        }
    }

    private java.nio.file.Path createTempFile(String data, String prefix) throws IOException {
        java.nio.file.Path policyFile = Files.createTempFile(prefix, ".txt");
        FileWriter writer = new FileWriter(policyFile.toFile());
        writer.write(data);
        writer.close();
        return policyFile;
    }

    private String getPolicyData(String policyName, String type, int freq, String sourceDataset, String targetDataSet,
                                 String sourceCluster, String targetCluster, String cloudCred) {
        String data = getPolicyData(policyName, type, freq, sourceDataset, targetDataSet, sourceCluster, targetCluster);
        return data + "cloudCred=" + cloudCred + NEW_LINE;
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
        cloudCred = buildCloudCred(randomString(), CloudCred.Provider.AWS, CloudCred.AuthType.AWS_ACCESSKEY,
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
        cloudCred = buildCloudCred(randomString(), CloudCred.Provider.AWS, CloudCred.AuthType.AWS_INSTANCEPROFILE,
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
        cloudCred = buildCloudCred(randomString(), CloudCred.Provider.AWS, CloudCred.AuthType.AWS_INSTANCEPROFILE,
                configs);
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

    private String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    @Test
    public void testListCloudCred() throws Exception {
        Map<Config, String> configs = new HashMap<>();
        configs.put(Config.AWS_ACCESS_KEY, "access.key.value");
        configs.put(Config.AWS_SECRET_KEY, "secret.key.value");

        CloudCred cloudCred1 = buildCloudCred(randomString(), CloudCred.Provider.AWS, CloudCred.AuthType.AWS_ACCESSKEY,
                configs);
        CloudCred cloudCred2 = buildCloudCred(randomString(), CloudCred.Provider.AWS, CloudCred.AuthType.AWS_ACCESSKEY,
                configs);
        BeaconClient client = new BeaconWebClient(getSourceBeaconServer());
        String entityId1 = client.submitCloudCred(cloudCred1);
        Assert.assertNotNull(entityId1);

        String entityId2 = client.submitCloudCred(cloudCred2);
        Assert.assertNotNull(entityId2);

        CloudCredList cloudCredList = client.listCloudCred("provider=AWS", null, null, null, null);
        assertEquals(cloudCredList.getResults(), 2);
        assertEquals(cloudCredList.getTotalResults(), 2);
        CloudCred[] elements = cloudCredList.getCloudCreds();
        assertEquals(elements.length, 2);
        assertEquals(elements[0].getId(), entityId1);
        assertEquals(elements[1].getId(), entityId2);
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
                append("Random").append(BeaconConstants.COMMA_SEPARATOR);
        api.append("type").append(BeaconConstants.COLON_SEPARATOR).append(FS).
                append(BeaconConstants.COMMA_SEPARATOR);
        api.append("endTime").append(BeaconConstants.COLON_SEPARATOR).
                append(DateUtil.formatDate(new Date()));
        api.append("&archived=").append(isArchived);
        HttpURLConnection conn = sendRequest(api.toString(), null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
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

    private void submitAndSchedule(String policyName, int frequency, String sourceDataset, String targetDataSet,
                                   Properties properties) throws IOException, JSONException {
        String data = getPolicyData(policyName, FS, frequency, sourceDataset, targetDataSet,
                SOURCE_CLUSTER, TARGET_CLUSTER);
        StringBuilder builder = new StringBuilder(data);
        if (properties != null && !properties.isEmpty()) {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                builder.append(entry.getKey()).append("=").append(entry.getValue()).append(NEW_LINE);
            }
        }
        data = builder.toString();
        StringBuilder api = new StringBuilder(getTargetBeaconServer() + BASE_API + "policy/submitAndSchedule/"
                + policyName);
        // Submit and Schedule job using submitAndSchedule API
        HttpURLConnection conn = sendRequest(api.toString(), data, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        assertTrue(jsonObject.getString("message").contains(policyName));
        assertTrue(jsonObject.getString("message").contains("submitAndSchedule successful"));
    }

    private void pairCluster(String beaconServer, String localCluster, String remoteCluster)
            throws IOException, JSONException {
        String api = BASE_API + "cluster/pair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());

        // Get cluster and verify if paired
        String cluster1Message = getClusterResponse(localCluster, getTargetBeaconServer());
        jsonObject = new JSONObject(cluster1Message);
        assertEquals(jsonObject.getString("name"), localCluster);
        validatePeers(jsonObject.getString("peers"), remoteCluster);

        String cluster2Message = getClusterResponse(remoteCluster, getTargetBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        assertEquals(jsonObject.getString("name"), remoteCluster);
        validatePeers(jsonObject.getString("peers"), localCluster);
    }

    private void pairClusterFailed(String beaconServer, String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/pair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    private void validatePeers(String peers, String cluster) {
        if (StringUtils.isNotBlank(peers)) {
            String[] peerList = peers.split(",");
            if (peerList.length > 1) {
                boolean found = false;
                for (String peer : peerList) {
                    if (peer.trim().equalsIgnoreCase(cluster)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            } else {
                assertEquals(peerList[0], cluster);
            }
        }
    }

    private String getClusterResponse(String clusterName, String serverEndpoint) throws IOException {
        String api = BASE_API + "cluster/getEntity/" + clusterName;
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        return getResponseMessage(inputStream);
    }

    private String getPolicyListResponse(String api, String beaconServer) throws IOException {
        HttpURLConnection conn = sendRequest(beaconServer + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        return getResponseMessage(inputStream);
    }

    private void validatePolicyList(String api, int numResults, int totalResults,
                                    List<String> names, List<String> types) throws IOException, JSONException {
        String message = getPolicyListResponse(api, getTargetBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        int result = jsonObject.getInt("results");
        int totalResult = jsonObject.getInt("totalResults");
        assertEquals(result, numResults);
        assertEquals(totalResult, totalResults);
        String policy = jsonObject.getString("policy");
        JSONArray jsonArray = new JSONArray(policy);

        int i = 0;
        for (String policyName : names) {
            JSONObject replicationPolicy = jsonArray.getJSONObject(i);
            assertTrue(policyName.equals(replicationPolicy.getString("name")));
            assertTrue(types.get(i).equals(replicationPolicy.getString("type")));
            ++i;
        }
    }

    private void deleteClusterAndValidate(String api, String serverEndpoint, String cluster)
            throws IOException, JSONException {
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        assertTrue(jsonObject.getString("message").contains("removed successfully"));
        assertTrue(jsonObject.getString("message").contains(cluster));
    }

    private void unpairClusterFailed(String beaconServer, String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    private void unpairCluster(String beaconServer, String localCluster,
                               String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        assertEquals(status, APIResult.Status.SUCCEEDED.name());

        // Get cluster and verify if unpaired
        String cluster1Message = getClusterResponse(localCluster, getSourceBeaconServer());
        jsonObject = new JSONObject(cluster1Message);
        assertEquals(jsonObject.getString("name"), localCluster);
        assertEquals(jsonObject.getString("peers"), "null");

        String cluster2Message = getClusterResponse(remoteCluster, getSourceBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        assertEquals(jsonObject.getString("name"), remoteCluster);
        assertEquals(jsonObject.getString("peers"), "null");
    }

    private void unpairWrongClusters(String beaconServer, String remoteCluster) throws IOException, JSONException {
        StringBuilder unPairAPI = new StringBuilder(BASE_API + "cluster/unpair");
        unPairAPI.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + unPairAPI.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.NOT_FOUND.getStatusCode());
    }

    private void submitCluster(String cluster, String clusterBeaconServer,
                               String server, String fsEndPoint, boolean isLocal) throws Exception {
        submitCluster(cluster, clusterBeaconServer, server, fsEndPoint, null, isLocal);
    }

    private void submitCluster(String cluster, String clusterBeaconServer,
                               String server, String fsEndPoint,
                               Map<String, String> clusterCustomProperties, boolean isLocal) throws Exception {
        String data = getClusterData(cluster, clusterBeaconServer, fsEndPoint, clusterCustomProperties, isLocal);
        BeaconClient client = new BeaconWebClient(server);
        java.nio.file.Path clusterFile = null;
        try {
            clusterFile = createTempFile(data, "cluster_");
            client.submitCluster(cluster, clusterFile.toString());
        } finally {
            if (clusterFile != null) {
                Files.deleteIfExists(clusterFile);
            }
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

    private String getResponseMessage(InputStream inputStream) throws IOException {
        StringBuilder response = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();
        inputStream.close();
        return response.toString();
    }

    private String getClusterData(String clusterName, String server,
                                  String fsEndPoint, Map<String, String> customProperties, boolean isLocal) {
        StringBuilder builder = new StringBuilder();
        builder.append("fsEndpoint=").append(fsEndPoint).append(NEW_LINE);
        builder.append("name=").append(clusterName).append(NEW_LINE);
        builder.append("description=").append("source cluster description").append(NEW_LINE);
        builder.append("beaconEndpoint=").append(server).append(NEW_LINE);
        builder.append("local=").append(isLocal).append(NEW_LINE);
        builder.append("tags=consumer,owner").append(NEW_LINE);
        if (customProperties != null && !customProperties.isEmpty()) {
            for (Map.Entry<String, String> entry : customProperties.entrySet()) {
                builder.append(entry.getKey()).append("=").append(entry.getValue()).append(NEW_LINE);
            }
        }
        return builder.toString();
    }

    private String getPolicyData(String policyName, String type, int freq, String sourceDataset, String targetDataSet,
                                 String sourceCluster, String targetCluster) {
        StringBuilder builder = new StringBuilder();
        builder.append("name=").append(policyName).append(NEW_LINE);
        builder.append("type=").append(type).append(NEW_LINE);
        builder.append("description=").append("Beacon test policy.").append(NEW_LINE);
        builder.append("frequencyInSec=").append(freq).append(NEW_LINE);
        builder.append("sourceDataset=").append(sourceDataset).append(NEW_LINE);
        if (StringUtils.isNotBlank(targetDataSet)) {
            builder.append("targetDataset=").append(targetDataSet).append(NEW_LINE);
        }
        if (StringUtils.isNotBlank(sourceCluster)) {
            builder.append("sourceCluster=").append(sourceCluster).append(NEW_LINE);
        }
        if (StringUtils.isNotBlank(targetCluster)) {
            builder.append("targetCluster=").append(targetCluster).append(NEW_LINE);
        }
        builder.append("distcpMaxMaps=1").append(NEW_LINE);
        builder.append("distcpMapBandwidth=10").append(NEW_LINE);
        builder.append("tde.enabled=false").append(NEW_LINE);
        builder.append("sourceSnapshotRetentionAgeLimit=10").append(NEW_LINE);
        builder.append("sourceSnapshotRetentionNumber=1").append(NEW_LINE);
        builder.append("targetSnapshotRetentionAgeLimit=10").append(NEW_LINE);
        builder.append("targetSnapshotRetentionNumber=1").append(NEW_LINE);
        builder.append("tags=owner=producer@xyz.com,component=sales").append(NEW_LINE);
        builder.append("retryAttempts=3").append(NEW_LINE);
        builder.append("retryDelay=120").append(NEW_LINE);
        builder.append("user=").append(System.getProperty("user.name")).append(NEW_LINE);

        return builder.toString();
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

    private HttpURLConnection sendRequest(String beaconUrl, String data, String method) throws IOException {
        URL url = new URL(beaconUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setDoInput(true);
        String authorization = USERNAME + ":" + PASSWORD;
        String encodedAuthorization= new String(Base64.encodeBase64(authorization.getBytes()));
        if (data != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "text/plain");
            connection.setRequestProperty("Content-Length", Integer.toString(data.getBytes().length));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuthorization);
            OutputStream outputStreamObj=null;
            int retry=0;
            while (outputStreamObj==null && retry<10) {
                try{
                    outputStreamObj=connection.getOutputStream();
                    retry++;
                }catch(Exception ex){
                    outputStreamObj=null;
                }
            }
            DataOutputStream outputStream = new DataOutputStream(outputStreamObj);
            outputStream.write(data.getBytes());
            outputStream.flush();
            outputStream.close();
        } else {
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + encodedAuthorization);
        }
        return connection;
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

    private void verifyLatestPolicyInstanceStatus(BeaconClient client, String policyName, JobStatus jobStatus)
            throws Exception {
        PolicyInstanceList instances = client.listPolicyInstances(policyName);
        for (int i = 0; i < instances.getElements().length; i++) {
            JobStatus status = JobStatus.valueOf(instances.getElements()[i].status);
            if (status == jobStatus) {
                return;
            } else if (status == JobStatus.RUNNING) {
                continue;
            }
        }
        throw new Exception("Expected " + jobStatus);
    }

    private void verifyClusterPairStatus(String beaconServer, ClusterStatus clusterStatus)
            throws JSONException, IOException {
        String peersInfoAPI = beaconServer + BASE_API + "cluster/list?fields=peersInfo";
        HttpURLConnection conn = sendRequest(peersInfoAPI, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);

        String peersInfoC1 = cluster1.getString("peersInfo");
        JSONArray jsonArrayPeersInfoc1 = new JSONArray(peersInfoC1);
        JSONObject peersInfo1 = jsonArrayPeersInfoc1.getJSONObject(0);
        assertEquals(peersInfo1.getString("pairStatus"),  clusterStatus.name());

        String peersInfoC2 = cluster2.getString("peersInfo");
        JSONArray jsonArrayPeersInfoc2 = new JSONArray(peersInfoC2);
        JSONObject peersInfo2 = jsonArrayPeersInfoc2.getJSONObject(0);
        assertEquals(peersInfo2.getString("pairStatus"), clusterStatus.name());
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
}
