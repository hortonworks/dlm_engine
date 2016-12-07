package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
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


    public TestBeaconResource () throws IOException {
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
        pairCluster(getSourceBeaconServer(), TARGET_CLUSTER, getTargetBeaconServer());
    }

    @Test
    public void testSubmitPolicy() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        submitPolicy("policy", FS, 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        submitPolicy("policy-1", FS, 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
        submitPolicy("policy-2", FS, 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
        String api = BASE_API + "policy/list";
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        int totalResults = jsonObject.getInt("totalResults");
        Assert.assertEquals(totalResults, 2);
        String cluster = jsonObject.getString("policy");
        JSONArray jsonArray = new JSONArray(cluster);
        JSONObject cluster1 = jsonArray.getJSONObject(0);
        JSONObject cluster2 = jsonArray.getJSONObject(1);
        Assert.assertTrue("policy-1".equals(cluster1.getString("name")));
        Assert.assertTrue(FS.equals(cluster1.getString("type")));
        Assert.assertTrue("policy-2".equals(cluster2.getString("name")));
        Assert.assertTrue(FS.equals(cluster2.getString("type")));
    }

    @Test
    public void testDeleteCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String api = BASE_API + "cluster/delete/" + SOURCE_CLUSTER;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, DELETE);
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
    public void testDeletePolicy() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "deletePolicy";
        submitPolicy(policyName, FS, 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        String api = BASE_API + "cluster/getEntity/" + SOURCE_CLUSTER;
        HttpURLConnection conn = sendRequest(getSourceBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("beaconEndpoint"), getSourceBeaconServer());
        Assert.assertEquals(jsonObject.getString("entityType"), "CLUSTER");
    }

    @Test
    public void testGetPolicy() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), "hdfs://localhost:8020");
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), "hdfs://localhost:8020");
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "policy";
        String type = FS;
        int freq = 10;
        String dataSet = "/tmp";
        submitPolicy(policyName, type, freq, dataSet,  SOURCE_CLUSTER, TARGET_CLUSTER);
        String api = BASE_API + "policy/getEntity/" + policyName;
        HttpURLConnection conn = sendRequest(getTargetBeaconServer() + api, null, GET);
        int responseCode = conn.getResponseCode();
        Assert.assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        String message = getResponseMessage(inputStream);
        JSONObject jsonObject = new JSONObject(message);
        Assert.assertEquals(jsonObject.getString("name"), policyName);
        Assert.assertEquals(jsonObject.getString("type"), type);
        Assert.assertEquals(jsonObject.getString("dataset"), dataSet);
        Assert.assertEquals(jsonObject.getInt("frequencyInSec"), freq);
        Assert.assertEquals(jsonObject.getString("sourceCluster"), SOURCE_CLUSTER);
        Assert.assertEquals(jsonObject.getString("targetCluster"), TARGET_CLUSTER);
    }

    @Test
    public void testSchedulePolicy() throws Exception {
        MiniDFSCluster srcDfsCluster  = startMiniHDFS(54136, SOURCE_DFS);
        MiniDFSCluster tgtDfsCluster  = startMiniHDFS(54137, TARGET_DFS);
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
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "hdfsPolicy";
        submitPolicy(policyName, FS, 120, "/apps/beacon/snapshot-replication/sourceDir/", SOURCE_CLUSTER, TARGET_CLUSTER);
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
        Thread.sleep(20000);
        Assert.assertTrue(tgtDfsCluster.getFileSystem().exists(new Path(TARGET_DIR, "dir1")));
        shutdownMiniHDFS(srcDfsCluster);
        shutdownMiniHDFS(tgtDfsCluster);
    }

    private void submitPolicy(String policyName, String type, int freq, String dataSet, String sourceCluster, String
            targetCluster) throws IOException, JSONException {
        String server = getTargetBeaconServer();
        String api = BASE_API + "policy/submit/" + policyName;
        String data = getPolicyData(policyName, type, freq, dataSet, sourceCluster, targetCluster);
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

    private void pairCluster(String beaconServer, String remoteCluster, String remoteBeaconServer) throws IOException,
            JSONException {
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

    private String getPolicyData(String policyName, String type, int freq, String dataSet, String sourceCluster, String
            targetCluster) {
        StringBuilder builder = new StringBuilder();
        builder.append("name=").append(policyName).append(NEW_LINE);
        builder.append("type=").append(type).append(NEW_LINE);
        builder.append("frequencyInSec=").append(freq).append(NEW_LINE);
        builder.append("dataset=").append(dataSet).append(NEW_LINE);
        builder.append("sourceCluster=").append(sourceCluster).append(NEW_LINE);
        builder.append("targetCluster=").append(targetCluster).append(NEW_LINE);
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
}