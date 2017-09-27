/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventSeverity;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.plugin.service.BeaconInfoImpl;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
import com.hortonworks.beacon.test.PluginTest;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Properties;

/**
 * Integration tests for Beacon REST API.
 */
public class BeaconResourceIT extends BeaconIntegrationTest {

    private static final String BASE_API = "/api/beacon/";
    private static final String NEW_LINE = System.lineSeparator();
    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String DELETE = "DELETE";
    private static final String SOURCE_DFS = beaconTestBaseDir + "dfs/" + SOURCE_CLUSTER;
    private static final String TARGET_DFS = beaconTestBaseDir + "dfs/" + TARGET_CLUSTER;
    private static final String SOURCE_DIR = "/apps/beacon/snapshot-replication/sourceDir/";
    private static final String FS = "FS";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private MiniDFSCluster srcDfsCluster;
    private MiniDFSCluster tgtDfsCluster;

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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
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
    public void testSubmitPolicy() throws Exception {
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
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.SUBMITTED, getSourceBeaconServer());
    }

    @Test
    public void testClusterList() throws Exception {
        // Testing the empty results.
        String api = BASE_API + "cluster/list";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        int results = jsonObject.getInt("results");
        Assert.assertEquals(totalResults, 0);
        Assert.assertEquals(results, 0);

        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        api = BASE_API + "cluster/list";
        conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        totalResults = jsonObject.getInt("totalResults");
        results = jsonObject.getInt("results");
        Assert.assertEquals(totalResults, 2);
        Assert.assertEquals(results, 2);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        Assert.assertTrue(SOURCE_CLUSTER.equals(cluster1.getString("name")));
        Assert.assertTrue(TARGET_CLUSTER.equals(cluster2.getString("name")));

        // Using the offset and numResults parameter.
        api = BASE_API + "cluster/list?offset=1&numResults=5";
        conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        totalResults = jsonObject.getInt("totalResults");
        results = jsonObject.getInt("results");
        Assert.assertEquals(totalResults, 2);
        Assert.assertEquals(results, 1);
        cluster = jsonObject.getString("cluster");
        jsonArray = new JSONArray(cluster);
        cluster1 = jsonArray.getJSONObject(0);
        Assert.assertTrue(TARGET_CLUSTER.equals(cluster1.getString("name")));
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

        submitPolicy("policy-1", FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String dataSet2 = dataSet+"2";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet2));
        submitPolicy("policy-2", FS, 10, dataSet2, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        api = BASE_API + "policy/list?orderBy=name&fields=datasets,clusters";
        names = Arrays.asList("policy-1", "policy-2");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 2, names, types);

        // Test filterBy
        submitCluster(OTHER_CLUSTER, getOtherBeaconServer(), getOtherBeaconServer(), srcFsEndPoint, true);
        submitCluster(OTHER_CLUSTER, getOtherBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getOtherBeaconServer(), tgtFsEndPoint, false);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, OTHER_CLUSTER);
        String dataSet3 = dataSet+"3";
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet3));
        submitPolicy("policy-3", FS, 10, dataSet3, null, OTHER_CLUSTER, TARGET_CLUSTER);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 2, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=targetcluster:target-cluster";
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + OTHER_CLUSTER;
        names = Arrays.asList("policy-3");
        types = Arrays.asList("FS");
        validatePolicyList(api, 1, 1, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:" + OTHER_CLUSTER
                + ",targetcluster:target-cluster";
        names = Arrays.asList("policy-3");
        types = Arrays.asList("FS");
        validatePolicyList(api, 1, 1, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=sourcecluster:"+ OTHER_CLUSTER + "|" + SOURCE_CLUSTER;
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, 3, names, types);

        api = BASE_API + "policy/list?offset=1&orderBy=name&filterBy=sourcecluster:"
                + OTHER_CLUSTER + "|" + SOURCE_CLUSTER;
        names = Arrays.asList("policy-2", "policy-3");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 3, names, types);

        api = BASE_API + "policy/list?offset=1&orderBy=name&filterBy=sourcecluster:" + SOURCE_CLUSTER;
        names = Arrays.asList("policy-2");
        types = Arrays.asList("FS");
        validatePolicyList(api, 1, 2, names, types);

        api = BASE_API + "policy/list?offset=1&orderBy=name&filterBy=targetcluster:target-cluster";
        names = Arrays.asList("policy-2", "policy-3");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, 3, names, types);
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
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        Thread.sleep(50000);
        int instanceCount = 2;
        String fields = "datasets,clusters,instances,executionType,customProperties";
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
        JSONArray instanceArray = new JSONArray(policyJson.getString("instances"));
        Assert.assertEquals(instanceArray.length(), instanceCount);
        JSONObject instanceJson3 = new JSONObject(instanceArray.getString(0));
        JSONObject instanceJson2 = new JSONObject(instanceArray.getString(1));
        Assert.assertTrue(instanceJson3.getString("id").endsWith("@4"));
        Assert.assertTrue(instanceJson2.getString("id").endsWith("@3"));
        Assert.assertEquals(policyName, instanceJson2.getString("name"));

        //Policy List API on source cluster
        response = getPolicyListResponse(api, getSourceBeaconServer());
        jsonObject = new JSONObject(response);
        policyArray = new JSONArray(jsonObject.getString("policy"));
        policyJson = new JSONObject(policyArray.getString(0));
        Assert.assertEquals(policyName, policyJson.getString("name"));
        Assert.assertNotNull(policyJson.getString("targetDataset"), "targetDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceDataset"), "sourceDataset should not be null.");
        Assert.assertNotNull(policyJson.getString("sourceCluster"), "sourceCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("targetCluster"), "targetCluster should not be null.");
        Assert.assertNotNull(policyJson.getString("executionType"), "executionType should not be null.");
        Assert.assertNotNull(policyJson.getString("customProperties"), "customProperties should not be null.");
        instanceArray = new JSONArray(policyJson.getString("instances"));
        Assert.assertEquals(instanceArray.length(), 0);
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
        String fsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(TARGET_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, false);
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), TARGET_CLUSTER);
    }

    @Test
    public void testDeletePairedCluster() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), TARGET_CLUSTER);

        // Verify cluster paired with was unpaired
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("peers"), "null");
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
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
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
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        deletePolicy(policyName);
        String eventapi = BASE_API + "events/all?orderBy=eventEntityType&sortOrder=asc";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + eventapi, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        Assert.assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 6);
        Assert.assertEquals(Integer.parseInt(jsonObject.getString("results")), 6);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("events"));
        Assert.assertEquals(jsonArray.getJSONObject(4).get("event"), Events.DELETED.getName());
        Assert.assertEquals(jsonArray.getJSONObject(4).get("eventType"), EventEntityType.POLICY.getName());
        Assert.assertEquals(jsonArray.getJSONObject(4).get("syncEvent"), true);
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
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertTrue(jsonObject.getString("message").contains("removed successfully"));
        Assert.assertNotNull(jsonObject.getString("requestId"));
    }

    @Test
    public void testGetCluster() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, false);
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("beaconEndpoint"), getSourceBeaconServer());
        Assert.assertEquals(jsonObject.getString("entityType"), "CLUSTER");
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
        submitPolicy(policyName, type, freq, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String message = getPolicyResponse(policyName, getTargetBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.SUBMITTED, freq, message);

        // On source cluster
        message = getPolicyResponse(policyName, getSourceBeaconServer(), "");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.SUBMITTED, freq, message);

        //Delete policy
        deletePolicy(policyName);
        message = getPolicyResponse(policyName, getTargetBeaconServer(), "?archived=true");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.DELETED, freq, message);

        // On source cluster after deletion
        message = getPolicyResponse(policyName, getSourceBeaconServer(), "?archived=true");
        assertPolicyEntity(dataSet, policyName, type, JobStatus.DELETED, freq, message);
    }

    private void assertPolicyEntity(String dataSet, String policyName, String type, JobStatus status,
                                    int freq, String message) throws JSONException, IOException {
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 1);
        Assert.assertEquals(jsonObject.getInt("results"), 1);
        String policy = jsonObject.getString("policy");
        JSONArray jsonPolicyArray = new JSONArray(policy);
        JSONObject jsonPolicy = jsonPolicyArray.getJSONObject(0);
        Assert.assertEquals(jsonPolicy.get("name"), policyName);
        Assert.assertEquals(jsonPolicy.getString("type"), type);
        Assert.assertEquals(jsonPolicy.getString("status"), status.name());
        Assert.assertEquals(jsonPolicy.getString("sourceDataset"), dataSet);
        Assert.assertEquals(jsonPolicy.getInt("frequencyInSec"), freq);
        Assert.assertEquals(jsonPolicy.getString("sourceCluster"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonPolicy.getString("targetCluster"), TARGET_CLUSTER);
        Assert.assertEquals(jsonPolicy.getString("user"), System.getProperty("user.name"));
        Assert.assertEquals(jsonPolicy.getInt("retryAttempts"), 3);
        Assert.assertEquals(jsonPolicy.getInt("retryDelay"), 120);

        // Source and target should have same number of custom properties.
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = mapper.readValue(jsonPolicy.getString("customProperties"),
                new TypeReference<Map<String, String>>(){});
        Assert.assertEquals(map.size(), 7);

        List<String> list = mapper.readValue(jsonPolicy.getString("tags"), new TypeReference<List<String>>(){});
        Assert.assertEquals(list.size(), 2);
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
        submitPolicy(policyName, FS, 120, replicationPath, replicationPath, SOURCE_CLUSTER, TARGET_CLUSTER);
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        String api = BASE_API + "policy/schedule/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertTrue(jsonObject.getString("message").contains(policyName));
        Assert.assertTrue(jsonObject.getString("message").contains("scheduled successfully"));
        Assert.assertNotNull(jsonObject.getString("requestId"));
        Thread.sleep(15000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());

        // Suspend and check status on source and target
        api = BASE_API + "policy/suspend/" + policyName;
        conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getSourceBeaconServer());
        verifyPolicyStatus(policyName, JobStatus.SUSPENDED, getTargetBeaconServer());

        // Resume and check status on source and target
        api = BASE_API + "policy/resume/" + policyName;
        conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getSourceBeaconServer());
        verifyPolicyStatus(policyName, JobStatus.RUNNING, getTargetBeaconServer());
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
        submitPolicy(policyName, FS, 120, replicationPath, replicationPath, SOURCE_CLUSTER, TARGET_CLUSTER);
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));
        String api = BASE_API + "policy/schedule/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertTrue(jsonObject.getString("message").contains(policyName));
        Assert.assertTrue(jsonObject.getString("message").contains("scheduled successfully"));
        Assert.assertNotNull(jsonObject.getString("requestId"));


        Thread.sleep(35000);
        Path pluginStagingPath = new Path(new BeaconInfoImpl().getStagingDir(), PluginTest.getPluginName());
        Path exportData = new Path(pluginStagingPath, new Path(replicationPath).getName());
        Assert.assertTrue(srcDfsCluster.getFileSystem().exists(exportData));
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(exportData));
        Path path = new Path(exportData, "_SUCCESS");

        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(path));
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, "dir1")));

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

        // Pair cluster - submit policy - UnPair Cluster
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "policy";
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        unpairClusterFailed(getTargetBeaconServer(), SOURCE_CLUSTER);
    }

    private void validateListClusterWithPeers(boolean hasPeers) throws Exception {
        String api = BASE_API + "cluster/list?fields=peers";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        int results = jsonObject.getInt("results");
        Assert.assertEquals(totalResults, 2);
        Assert.assertEquals(results, 2);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        Assert.assertTrue(SOURCE_CLUSTER.equals(cluster1.getString("name")));
        Assert.assertTrue(TARGET_CLUSTER.equals(cluster2.getString("name")));

        JSONArray cluster1Peers = new JSONArray(cluster1.getString("peers"));
        JSONArray cluster2Peers = new JSONArray(cluster2.getString("peers"));
        if (hasPeers) {
            Assert.assertTrue(TARGET_CLUSTER.equals(cluster1Peers.get(0)));
            Assert.assertTrue(SOURCE_CLUSTER.equals(cluster2Peers.get(0)));
        } else {
            Assert.assertTrue(cluster1Peers.length() == 0);
            Assert.assertTrue(cluster2Peers.length() == 0);
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
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));
        // Submit and schedule policy
        submitAndSchedule(policyName, 15, replicationPath, replicationPath, new Properties());

        // Expecting four instances of the policy should be executed.
        Thread.sleep(55000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policyName)));

        // Test the list API
        JSONArray jsonArray = instanceListAPI(policyName, "&offset=-1", 4, 4);
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@4"));
        Assert.assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@3"));
        Assert.assertTrue(jsonArray.getJSONObject(2).getString("id").endsWith("@2"));
        Assert.assertTrue(jsonArray.getJSONObject(3).getString("id").endsWith("@1"));

        // using the offset parameter
        jsonArray = instanceListAPI(policyName, "&offset=2", 4, 2);
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@2"));
        Assert.assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@1"));

        // delete the policy and do listing
        deletePolicy(policyName);
        instanceListAPI(policyName, "&archived=false", 0, 0);

        jsonArray = instanceListAPI(policyName, "&archived=true", 4, 4);
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@4"));
        Assert.assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@3"));
        Assert.assertTrue(jsonArray.getJSONObject(2).getString("id").endsWith("@2"));
        Assert.assertTrue(jsonArray.getJSONObject(3).getString("id").endsWith("@1"));
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), totalResults);
        Assert.assertEquals(jsonObject.getInt("results"), results);
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

        String listDataAPI = BASE_API + "file/list?path="+basePath;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + listDataAPI, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertEquals("Success", jsonObject.getString("message"));
        Assert.assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("fileList"));
        Assert.assertEquals(jsonArray.getJSONObject(0).get("pathSuffix"), data1);
        Assert.assertEquals(jsonArray.getJSONObject(0).get("type"), "DIRECTORY");
        Assert.assertEquals(jsonArray.getJSONObject(1).get("pathSuffix"), data2);
        Assert.assertEquals(jsonArray.getJSONObject(1).get("type"), "DIRECTORY");
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
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy1, policy1)));
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy2, policy2)));
        // Submit and schedule two different policy
        submitAndSchedule(policy1, 60, sourceDirPolicy1, sourceDirPolicy1, new Properties());
        submitAndSchedule(policy2, 60, sourceDirPolicy2, sourceDirPolicy2, new Properties());

        // Expecting one instance of both the policy should be executed successfully.
        Thread.sleep(20000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy1, policy1)));
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(sourceDirPolicy2, policy2)));

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
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policy1)));

        // Submit and schedule policy
        submitAndSchedule(policy1, 60, replicationPath, replicationPath, new Properties());

        // Expecting one instance of the policy should be executed successfully.
        Thread.sleep(20000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(replicationPath, policy1)));

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
        submitPolicy(policyName, FS, 60, new Path(replicationPath).toString(),
                new Path(replicationPath).toString(), SOURCE_CLUSTER, TARGET_CLUSTER);

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.SUBMITTED, getSourceBeaconServer());

        StringBuilder api = new StringBuilder(getTargetBeaconServer() + BASE_API + "policy/info/" + policyName);
        HttpURLConnection connection = sendRequest(api.toString(), null, GET);
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = connection.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals("SUCCEEDED", jsonObject.getString("status"));
        Assert.assertEquals("Type=FS_SNAPSHOT", jsonObject.getString("message"));
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
        submitPolicy(policyName, FS, 10, "/tmp", null, SOURCE_CLUSTER, TARGET_CLUSTER);

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, JobStatus.SUBMITTED, getSourceBeaconServer());
        String eventapi = BASE_API + "events/all?orderBy=eventEntityType&sortOrder=asc";
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        Assert.assertEquals("Success", jsonObject.getString("message"));
        Assert.assertEquals(Integer.parseInt(jsonObject.getString("totalResults")), 6);
        Assert.assertEquals(Integer.parseInt(jsonObject.getString("results")), 6);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("events"));
        Assert.assertEquals(jsonArray.getJSONObject(0).get("severity"), EventSeverity.INFO.getName());
        Assert.assertEquals(jsonArray.getJSONObject(0).get("eventType"), EventEntityType.CLUSTER.getName());
        Assert.assertEquals(jsonArray.getJSONObject(3).get("eventType"), EventEntityType.POLICY.getName());
        Assert.assertEquals(jsonArray.getJSONObject(5).get("eventType"), EventEntityType.SYSTEM.getName());

        String startStr = DateUtil.getDateFormat().format(new Date().getTime() - 300000);
        String endStr = DateUtil.getDateFormat().format(new Date());
        eventapi = BASE_API + "events/entity/cluster?start="+startStr+"&end="+endStr;
        conn = sendRequest(getTargetBeaconServer() + eventapi, null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        response = getResponseMessage(inputStream);
        jsonObject = new JSONObject(response);
        status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        Assert.assertEquals("Success", jsonObject.getString("message"));
        jsonArray = new JSONArray(jsonObject.getString("events"));
        Assert.assertEquals(jsonArray.getJSONObject(0).get("eventType"), EventEntityType.CLUSTER.getName());
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
        Assert.assertEquals(jsonArray.getJSONObject(0).getString("status"), JobStatus.KILLED.name());
        Assert.assertTrue(jsonArray.getJSONObject(1).getString("id").endsWith("@2"));
        Assert.assertEquals(jsonArray.getJSONObject(1).getString("status"), JobStatus.SUCCESS.name());
    }

    private void abortAPI(String policyName) throws IOException, JSONException {
        StringBuilder abortAPI = new StringBuilder(getTargetBeaconServer() + BASE_API
                + "policy/instance/abort/" + policyName);
        HttpURLConnection connection = sendRequest(abortAPI.toString(), null, POST);
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = connection.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals("SUCCEEDED", jsonObject.getString("status"));
        Assert.assertTrue(jsonObject.getString("message").contains("[true]"));
    }

    @Test
    public void testRerunPolicyInstance() throws Exception {
        String policyName = "rerun-policy";
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
        Thread.sleep(12000);
        String abortAPI = getTargetBeaconServer() + BASE_API + "policy/instance/abort/" + policyName;
        HttpURLConnection connection = sendRequest(abortAPI, null, POST);
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = connection.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals("SUCCEEDED", jsonObject.getString("status"));
        Assert.assertTrue(jsonObject.getString("message").contains("[true]"));
        // Use list API and check the status.
        Thread.sleep(1000);
        String server = getTargetBeaconServer();
        String listAPI = server + BASE_API + "policy/instance/list/" + policyName + "?sortOrder=ASC";
        HttpURLConnection conn = sendRequest(listAPI, null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 2);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
        Assert.assertEquals(jsonArray.getJSONObject(0).getString("status"), JobStatus.KILLED.name());
        //Rerun the instance and check status.
        String rerunAPI = server + BASE_API + "policy/instance/rerun/" + policyName;
        conn = sendRequest(rerunAPI, null, POST);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        // Wait for the job to complete successfully.
        Thread.sleep(25000);
        conn = sendRequest(listAPI, null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 4);
        jsonArray = new JSONArray(jsonObject.getString("instance"));
        Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
        Assert.assertEquals(jsonArray.getJSONObject(0).getString("status"), JobStatus.SUCCESS.name());
    }

    @Test
    public void testServerVersion() throws Exception {
        String server = getTargetBeaconServer();
        String statusApi = server + BASE_API + "admin/version";
        HttpURLConnection conn = sendRequest(statusApi, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), "RUNNING");
    }

    @Test
    public void testServerStatus() throws Exception {
        String server = getTargetBeaconServer();
        String statusApi = server + BASE_API + "admin/status";
        HttpURLConnection conn = sendRequest(statusApi, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), "RUNNING");
        Assert.assertEquals(jsonObject.getString("plugins"), "None");
        Assert.assertEquals(jsonObject.getString("security"), "None");
        Assert.assertEquals(jsonObject.getString("wireEncryption"), "false");
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

        //Deletion of completed policy should not happen.
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());

        shutdownMiniHDFS(srcDfsCluster);
        shutdownMiniHDFS(tgtDfsCluster);
    }

    private void verifyPolicyCompletionStatus(String response, String expectedResponse) throws JSONException {
        JSONObject jsonObject = new JSONObject(response);
        String policyArray = jsonObject.getString("policy");
        JSONObject policyJson = new JSONArray(policyArray).getJSONObject(0);
        Assert.assertEquals(policyJson.getString("status"), expectedResponse);
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        System.out.println("policy/instance/list/" + policyName +" : "+ message);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 1);
        Assert.assertEquals(jsonObject.getInt("results"), 1);
        if (1 > 0) {
            JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
            Assert.assertTrue(jsonArray.getJSONObject(0).getString("id").endsWith("@1"));
        }
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
            Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
            InputStream inputStream = conn.getInputStream();
            Assert.assertNotNull(inputStream);
            String message = getResponseMessage(inputStream);
            JSONObject jsonObject = new JSONObject(message);
            Assert.assertEquals(jsonObject.getInt("totalResults"), 0);
            Assert.assertEquals(jsonObject.getInt("results"), 0);
        } else {
            Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertTrue(jsonObject.getString("message").contains(policyName));
        Assert.assertTrue(jsonObject.getString("message").contains("submitAndSchedule successful"));
        Assert.assertNotNull(jsonObject.getString("requestId"));
    }

    private void submitPolicy(String policyName, String type, int freq, String sourceDataSet, String targetDataSet,
                              String sourceCluster, String targetCluster) throws IOException, JSONException {
        String server = getTargetBeaconServer();
        String api = BASE_API + "policy/submit/" + policyName;
        String data = getPolicyData(policyName, type, freq, sourceDataSet, targetDataSet, sourceCluster, targetCluster);
        HttpURLConnection conn = sendRequest(server + api, data, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        String requestId = jsonObject.getString("requestId");
        Assert.assertNotNull(requestId, "should not be null.");
        String message = jsonObject.getString("message");
        Assert.assertTrue(message.contains(policyName));
    }

    private void pairCluster(String beaconServer, String localCluster, String remoteCluster)
            throws IOException, JSONException {
        String api = BASE_API + "cluster/pair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        String requestId = jsonObject.getString("requestId");
        Assert.assertNotNull(requestId, "should not be null.");

        // Get cluster and verify if paired
        String cluster1Message = getClusterResponse(localCluster, getTargetBeaconServer());
        jsonObject = new JSONObject(cluster1Message);
        Assert.assertEquals(jsonObject.getString("name"), localCluster);
        validatePeers(jsonObject.getString("peers"), remoteCluster);

        String cluster2Message = getClusterResponse(remoteCluster, getTargetBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        Assert.assertEquals(jsonObject.getString("name"), remoteCluster);
        validatePeers(jsonObject.getString("peers"), localCluster);
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
                Assert.assertTrue(found);
            } else {
                Assert.assertEquals(peerList[0], cluster);
            }
        }
    }

    private String getClusterResponse(String clusterName, String serverEndpoint) throws IOException {
        String api = BASE_API + "cluster/getEntity/" + clusterName;
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        return getResponseMessage(inputStream);
    }

    private String getPolicyListResponse(String api, String beaconServer) throws IOException {
        HttpURLConnection conn = sendRequest(beaconServer + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
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
        Assert.assertEquals(result, numResults);
        Assert.assertEquals(totalResult, totalResults);
        String policy = jsonObject.getString("policy");
        JSONArray jsonArray = new JSONArray(policy);

        int i = 0;
        for (String policyName : names) {
            JSONObject replicationPolicy = jsonArray.getJSONObject(i);
            Assert.assertTrue(policyName.equals(replicationPolicy.getString("name")));
            Assert.assertTrue(types.get(i).equals(replicationPolicy.getString("type")));
            ++i;
        }
    }

    private void deleteClusterAndValidate(String api, String serverEndpoint, String cluster)
            throws IOException, JSONException {
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("status"), APIResult.Status.SUCCEEDED.name());
        Assert.assertTrue(jsonObject.getString("message").contains("removed successfully"));
        Assert.assertTrue(jsonObject.getString("message").contains(cluster));
        Assert.assertNotNull(jsonObject.getString("requestId"));
    }

    private void unpairClusterFailed(String beaconServer, String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    private void unpairCluster(String beaconServer, String localCluster,
                               String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        String requestId = jsonObject.getString("requestId");
        Assert.assertNotNull(requestId, "should not be null.");

        // Get cluster and verify if unpaired
        String cluster1Message = getClusterResponse(localCluster, getSourceBeaconServer());
        jsonObject = new JSONObject(cluster1Message);
        Assert.assertEquals(jsonObject.getString("name"), localCluster);
        Assert.assertEquals(jsonObject.getString("peers"), "null");

        String cluster2Message = getClusterResponse(remoteCluster, getSourceBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        Assert.assertEquals(jsonObject.getString("name"), remoteCluster);
        Assert.assertEquals(jsonObject.getString("peers"), "null");
    }

    private void submitCluster(String cluster, String clusterBeaconServer,
                               String server, String fsEndPoint, boolean isLocal) throws IOException, JSONException {
        submitCluster(cluster, clusterBeaconServer, server, fsEndPoint, null, isLocal);
    }

    private void submitCluster(String cluster, String clusterBeaconServer,
                               String server, String fsEndPoint,
                               Map<String, String> clusterCustomProperties, boolean isLocal)
            throws IOException, JSONException {
        String api = BASE_API + "cluster/submit/" + cluster;
        String data = getClusterData(cluster, clusterBeaconServer, fsEndPoint, clusterCustomProperties, isLocal);
        HttpURLConnection conn = sendRequest(server + api, data, POST);
        int responseCode = conn.getResponseCode();
        int retry=0;
        while (responseCode!=Response.Status.OK.getStatusCode() && retry<10) {
            conn = sendRequest(server + api, data, POST);
            responseCode = conn.getResponseCode();
            retry++;
        }
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String response = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        String message = jsonObject.getString("message");
        Assert.assertTrue(message.contains(cluster));
        String requestId = jsonObject.getString("requestId");
        Assert.assertNotNull(requestId, "should not be null.");
    }

    private String getPolicyStatus(String policyName, String server) throws IOException {
        String api = BASE_API + "policy/status/" + policyName;
        HttpURLConnection conn = sendRequest(server + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
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
        builder.append("tdeEncryptionEnabled=false").append(NEW_LINE);
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
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
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
        Assert.assertEquals(status, expectedStatus.name());
        String name = jsonObject.getString("name");
        Assert.assertEquals(name, policyName);
    }
}
