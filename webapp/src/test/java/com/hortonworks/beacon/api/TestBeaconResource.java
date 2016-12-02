package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
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

    public TestBeaconResource () throws IOException {
        super();
    }
    @Test
    public void testSubmitCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
    }

    @Test
    public void testPairCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer());
        pairCluster(getSourceBeaconServer(), TARGET_CLUSTER, getTargetBeaconServer());
    }

    @Test
    public void testSubmitPolicy() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer());
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        submitPolicy("policy", "HDFS", 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
    }

    @Test
    public void testClusterList() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer());
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        submitPolicy("policy-1", "HDFS", 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
        submitPolicy("policy-2", "HDFS", 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
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
        Assert.assertTrue("HDFS".equals(cluster1.getString("type")));
        Assert.assertTrue("policy-2".equals(cluster2.getString("name")));
        Assert.assertTrue("HDFS".equals(cluster2.getString("type")));
    }

    @Test
    public void testDeleteCluster() throws Exception {
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer());
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "deletePolicy";
        submitPolicy(policyName, "HDFS", 10, "/tmp", SOURCE_CLUSTER, TARGET_CLUSTER);
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
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
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer());
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer());
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer());
        pairCluster(getTargetBeaconServer(), SOURCE_CLUSTER, getSourceBeaconServer());
        String policyName = "policy";
        String type = "HDFS";
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

    private void submitCluster(String cluster, String clusterBeaconServer, String server) throws IOException, JSONException {
        String api = BASE_API + "cluster/submit/" + cluster;
        String data = getClusterData(cluster, clusterBeaconServer);
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

    private String getClusterData(String clusterName, String server) {
        StringBuilder builder = new StringBuilder();
        builder.append("fsEndpoint=").append("hdfs://localhost:8020").append(NEW_LINE);
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