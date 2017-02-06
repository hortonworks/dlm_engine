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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestBeaconResource extends BeaconIntegrationTest {

    private static final String BASE_API = "/api/beacon/";
    private static final String NEW_LINE = System.lineSeparator();
    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String DELETE = "DELETE";
    private static final String SOURCE_DFS = BEACON_TEST_BASE_DIR + "dfs/" + SOURCE_CLUSTER;
    private static final String TARGET_DFS = BEACON_TEST_BASE_DIR + "dfs/" + TARGET_CLUSTER;
    private static final String SOURCE_DIR = "/apps/beacon/snapshot-replication/sourceDir/";
    private static final String TARGET_DIR = "/apps/beacon/snapshot-replication/targetDir/";
    private static final String FS = "FS";


    public TestBeaconResource() throws IOException {
        super();
    }

    @Test
    public void testSubmitCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
    }

    @Test
    public void testPairCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER, getTargetBeaconServer());
    }

    @Test
    public void testSubmitPolicy() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "policy";
        submitPolicy(policyName, FS, 10, "/tmp", null, SOURCE_CLUSTER, TARGET_CLUSTER);

        // After submit verify policy was synced and it's status on remote source cluster
        verifyPolicyStatus(policyName, "SUBMITTED", getSourceBeaconServer());
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testClusterList() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String api = BASE_API + "cluster/list";
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        Assert.assertEquals(totalResults, 2);
        String cluster = jsonObject.getString("cluster");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        Assert.assertTrue(SOURCE_CLUSTER.equals(cluster1.getString("name")));
        Assert.assertTrue(TARGET_CLUSTER.equals(cluster2.getString("name")));
    }

    @Test
    public void testPolicyList() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        submitPolicy("policy-1", FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        submitPolicy("policy-2", FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String api = BASE_API + "policy/list?orderBy=name";
        List<String> names = Arrays.asList("policy-1", "policy-2");
        List<String> types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, names, types);

        // Test filterBy
        submitCluster(OTHER_CLUSTER, getOtherBeaconServer(), getOtherBeaconServer(), "hdfs://localhost:8020");
        submitCluster(OTHER_CLUSTER, getOtherBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getOtherBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, OTHER_CLUSTER, getOtherBeaconServer());
        submitPolicy("policy-3", FS, 10, dataSet, null, OTHER_CLUSTER, TARGET_CLUSTER);

        api = BASE_API + "policy/list?orderBy=name&filterBy=SOURCECLUSTER:source-cluster";
        names = Arrays.asList("policy-1", "policy-2");
        types = Arrays.asList("FS", "FS");
        validatePolicyList(api, 2, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=TARGETCLUSTER:target-cluster";
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=SOURCECLUSTER:other-cluster";
        names = Arrays.asList("policy-3");
        types = Arrays.asList("FS");
        validatePolicyList(api, 1, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=SOURCECLUSTER:other-cluster,TARGETCLUSTER:target-cluster";
        names = Arrays.asList("policy-3");
        types = Arrays.asList("FS");
        validatePolicyList(api, 1, names, types);

        api = BASE_API + "policy/list?orderBy=name&filterBy=SOURCECLUSTER:other-cluster|source-cluster";
        names = Arrays.asList("policy-1", "policy-2", "policy-3");
        types = Arrays.asList("FS", "FS", "FS");
        validatePolicyList(api, 3, names, types);
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testDeleteLocalCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String api = BASE_API + "cluster/delete/" + SOURCE_CLUSTER;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testDeleteCluster() throws Exception {
        submitCluster(TARGET_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer());
    }

    @Test
    public void testDeletePairedCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer());

        // Verify cluster paired with was unpaired
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("peers"), "null");
    }

    @Test
    public void testDeletePolicyOnSourceCluster() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "deletePolicy";
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String api = BASE_API + "policy/delete/" + policyName;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testDeletePolicy() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "deletePolicy";
        submitPolicy(policyName, FS, 10, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
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
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testGetCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String message = getClusterResponse(SOURCE_CLUSTER, getSourceBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("beaconEndpoint"), getSourceBeaconServer());
        Assert.assertEquals(jsonObject.getString("entityType"), "CLUSTER");
    }

    @Test
    public void testGetPolicy() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "policy";
        String type = FS;
        int freq = 10;
        submitPolicy(policyName, type, freq, dataSet, null, SOURCE_CLUSTER, TARGET_CLUSTER);
        String message = getPolicyResponse(policyName, getTargetBeaconServer());
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), policyName);
        Assert.assertEquals(jsonObject.getString("type"), type);
        Assert.assertEquals(jsonObject.getString("sourceDataset"), dataSet);
        Assert.assertEquals(jsonObject.getInt("frequencyInSec"), freq);
        Assert.assertEquals(jsonObject.getString("sourceCluster"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("targetCluster"), TARGET_CLUSTER);

        // On source cluster
        String api = BASE_API + "policy/getEntity/" + policyName;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testScheduleSuspendAndResumePolicy() throws Exception {
        MiniDFSCluster srcDfsCluster = startMiniHDFS(54138, SOURCE_DFS);
        MiniDFSCluster tgtDfsCluster = startMiniHDFS(54139, TARGET_DFS);
        srcDfsCluster.getFileSystem().mkdirs(new Path(SOURCE_DIR));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(SOURCE_DIR));
        srcDfsCluster.getFileSystem().mkdirs(new Path(SOURCE_DIR, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(TARGET_DIR));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(TARGET_DIR));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "hdfsPolicy";
        submitPolicy(policyName, FS, 120, SOURCE_DIR, TARGET_DIR, SOURCE_CLUSTER, TARGET_CLUSTER);
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(TARGET_DIR, "dir1")));
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
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(TARGET_DIR, "dir1")));

        // Verify status was updated on remote source cluster after schedule
        verifyPolicyStatus(policyName, "RUNNING", getSourceBeaconServer());

        // Suspend and check status on source and target
        api = BASE_API + "policy/suspend/" + policyName;
        conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, "SUSPENDED", getSourceBeaconServer());
        verifyPolicyStatus(policyName, "SUSPENDED", getTargetBeaconServer());

        // Resume and check status on source and target
        api = BASE_API + "policy/resume/" + policyName;
        conn = sendRequest(getTargetBeaconServer() + api, null, POST);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        verifyPolicyStatus(policyName, "RUNNING", getSourceBeaconServer());
        verifyPolicyStatus(policyName, "RUNNING", getTargetBeaconServer());

        shutdownMiniHDFS(srcDfsCluster);
        shutdownMiniHDFS(tgtDfsCluster);
    }

    @Test
    public void testUnpairClusters() throws Exception {
        String dataSet = "/tmp";
        MiniDFSCluster miniDFSCluster = startMiniHDFS(8020, dataSet);
        miniDFSCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        unpairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        shutdownMiniHDFS(miniDFSCluster);
    }

    @Test
    public void testJobInstanceListing() throws Exception {
        MiniDFSCluster srcDfsCluster = startMiniHDFS(54136, SOURCE_DFS);
        MiniDFSCluster tgtDfsCluster = startMiniHDFS(54137, TARGET_DFS);
        srcDfsCluster.getFileSystem().mkdirs(new Path(SOURCE_DIR));
        srcDfsCluster.getFileSystem().allowSnapshot(new Path(SOURCE_DIR));
        srcDfsCluster.getFileSystem().mkdirs(new Path(SOURCE_DIR, "dir1"));
        tgtDfsCluster.getFileSystem().mkdirs(new Path(TARGET_DIR));
        tgtDfsCluster.getFileSystem().allowSnapshot(new Path(TARGET_DIR));
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "hdfsPolicy";
        String data = getPolicyData(policyName, FS, 10, "hdfs://localhost:54136/" + SOURCE_DIR,
                "hdfs://localhost:54137/" + TARGET_DIR, SOURCE_CLUSTER, TARGET_CLUSTER);
        StringBuilder api = new StringBuilder(getTargetBeaconServer() + BASE_API + "policy/submitAndSchedule/" + policyName);
        Assert.assertFalse(tgtDfsCluster.getFileSystem().exists(new Path(TARGET_DIR, "dir1")));
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
        Assert.assertTrue(jsonObject.getString("message").contains("scheduled successfully"));
        Assert.assertNotNull(jsonObject.getString("requestId"));
        Thread.sleep(35000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(TARGET_DIR, "dir1")));
        shutdownMiniHDFS(srcDfsCluster);
        shutdownMiniHDFS(tgtDfsCluster);
        // Test the list API
        String server = getTargetBeaconServer();
        api = new StringBuilder(server + BASE_API + "policy/instance/list");
        api.append("?").append("filter=");
        api.append("name=").append(policyName).append(";");
        api.append("type=").append(FS).append(";");
        api.append("endTime=").append(DateUtil.formatDate(new Date()));
        api.append("&orderBy=endTime").append("&sortBy=DESC").append("&numResults=10");
        conn = sendRequest(api.toString(), null, GET);
        responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        message = getResponseMessage(inputStream);
        jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getInt("totalResults"), 4);
        JSONArray jsonArray = new JSONArray(jsonObject.getString("instance"));
        Assert.assertEquals(jsonArray.getJSONObject(0).getString("id"), policyName + "@4");
        Assert.assertEquals(jsonArray.getJSONObject(1).getString("id"), policyName + "@3");
        Assert.assertEquals(jsonArray.getJSONObject(2).getString("id"), policyName + "@2");
        Assert.assertEquals(jsonArray.getJSONObject(3).getString("id"), policyName + "@1");
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

    private void pairCluster(String beaconServer, String localCluster, String remoteCluster, String remoteBeaconServer)
            throws IOException, JSONException {
        String api = BASE_API + "cluster/pair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteBeaconEndpoint=").append(remoteBeaconServer);
        builder.append("&").append("remoteClusterName=").append(remoteCluster);
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

    private String getPolicyListResponse(String api) throws IOException {
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        return getResponseMessage(inputStream);
    }

    private void validatePolicyList(String api, int numResults,
                                    List<String> names, List<String> types) throws IOException, JSONException {
        String message = getPolicyListResponse(api);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        Assert.assertEquals(totalResults, numResults);
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

    private void deleteClusterAndValidate(String api, String serverEndpoint) throws IOException, JSONException {
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, DELETE);
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

    private void unpairCluster(String beaconServer, String localCluster, String remoteCluster,
                               String remoteBeaconServer) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteBeaconEndpoint=").append(remoteBeaconServer);
        builder.append("&").append("remoteClusterName=").append(remoteCluster);
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

    private void submitCluster(String cluster, String clusterBeaconServer, String server, String fsEndPoint) throws IOException, JSONException {
        String api = BASE_API + "cluster/submit/" + cluster;
        String data = getClusterData(cluster, clusterBeaconServer, fsEndPoint);
        HttpURLConnection conn = sendRequest(server + api, data, POST);
        int responseCode = conn.getResponseCode();
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

    private String getClusterData(String clusterName, String server, String fsEndPoint) {
        StringBuilder builder = new StringBuilder();
        builder.append("fsEndpoint=").append(fsEndPoint).append(NEW_LINE);
        builder.append("name=").append(clusterName).append(NEW_LINE);
        builder.append("description=").append("source cluster description").append(NEW_LINE);
        builder.append("beaconEndpoint=").append(server).append(NEW_LINE);
        return builder.toString();
    }

    private String getPolicyData(String policyName, String type, int freq, String sourceDataset, String targetDataSet,
                                 String sourceCluster, String targetCluster) {
        StringBuilder builder = new StringBuilder();
        builder.append("name=").append(policyName).append(NEW_LINE);
        builder.append("type=").append(type).append(NEW_LINE);
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
        builder.append("sourceDir=").append(SOURCE_DIR).append(NEW_LINE);
        builder.append("targetDir=").append(TARGET_DIR).append(NEW_LINE);
        builder.append("distcpMaxMaps=1").append(NEW_LINE);
        builder.append("distcpMapBandwidth=10").append(NEW_LINE);
        builder.append("tdeEncryptionEnabled=false").append(NEW_LINE);
        builder.append("sourceSnapshotRetentionAgeLimit=10").append(NEW_LINE);
        builder.append("sourceSnapshotRetentionNumber=1").append(NEW_LINE);
        builder.append("targetSnapshotRetentionAgeLimit=10").append(NEW_LINE);
        builder.append("targetSnapshotRetentionNumber=1").append(NEW_LINE);

        return builder.toString();
    }

    private String getPolicyResponse(String policyName, String server) throws IOException {
        String api = BASE_API + "policy/getEntity/" + policyName;
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
        if (data != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "text/plain");
            connection.setRequestProperty("Content-Length", Integer.toString(data.getBytes().length));
            DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());
            outputStream.write(data.getBytes());
            outputStream.flush();
            outputStream.close();
        }
        return connection;
    }

    private void verifyPolicyStatus(String policyName, String expectedStatus,
                                    String server) throws IOException, JSONException {
        String response = getPolicyStatus(policyName, server);
        JSONObject jsonObject = new JSONObject(response);
        String status = jsonObject.getString("status");
        Assert.assertEquals(status, APIResult.Status.SUCCEEDED.name());
        String message = jsonObject.getString("message");
        Assert.assertEquals(message, expectedStatus);
    }
}