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

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.result.EventsResult;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;
import com.hortonworks.beacon.util.ClusterStatus;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.NDC;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Base class for setup and teardown IT test cluster.
 */
public class BeaconIntegrationTest {
    public static final Logger LOG = LoggerFactory.getLogger(BeaconIntegrationTest.class);

    protected static final String GET = HttpMethod.GET;
    protected static final String POST = HttpMethod.POST;
    protected static final String PUT = HttpMethod.PUT;
    protected static final String DELETE = HttpMethod.DELETE;

    protected static final String SOURCE_DIR = "/apps/beacon/snapshot-replication/sourceDir/";
    protected static final String SOURCE_CLUSTER = "dc$source-cluster";
    protected static final String TARGET_CLUSTER = "target-cluster";
    protected static final String OTHER_CLUSTER = "dc$other-cluster";
    private static List<String> sourceJVMOptions = new ArrayList<>();
    private static List<String> targetJVMOptions = new ArrayList<>();
    protected static final int SOURCE_PORT = 8021;
    protected static final int TARGET_PORT = 8022;

    protected static final String BASE_API = "/api/beacon/";
    protected static final String USERNAME = "admin";
    protected static final String PASSWORD = "admin";

    protected MiniDFSCluster srcDfsCluster;
    protected MiniDFSCluster tgtDfsCluster;
    private static final String SOURCE_DFS = System.getProperty("beacon.data.dir") + "/dfs/" + SOURCE_CLUSTER;
    private static final String TARGET_DFS = System.getProperty("beacon.data.dir") + "/dfs/" + TARGET_CLUSTER;

    protected static final String FS = "FS";

    static {
        String commonOptions = "-Dlog4j.configuration=beacon-log4j.properties -Dbeacon.version="
                + System.getProperty(BeaconConstants.BEACON_VERSION_CONST)
                + " -Dbeacon.log.appender=FILE";
        sourceJVMOptions.add(commonOptions + " -Dbeacon.log.filename=beacon-application.log." + SOURCE_CLUSTER);
        String sourceBeaconOpts = "source.beacon.server.opts";
        if (System.getProperty(sourceBeaconOpts) != null) {
            sourceJVMOptions.add(System.getProperty(sourceBeaconOpts));
        }
        targetJVMOptions.add(commonOptions + " -Dbeacon.log.filename=beacon-application.log." + TARGET_CLUSTER);
        String targetBeaconOpts = "target.beacon.server.opts";
        if (System.getProperty(targetBeaconOpts) != null) {
            targetJVMOptions.add(System.getProperty(targetBeaconOpts));
        }
    }

    private Process sourceCluster;
    private Process targetCluster;
    private Properties sourceProp;
    private Properties targetProp;

    protected BeaconClient sourceClient;
    protected BeaconClient targetClient;
    private long startTime;

    public BeaconIntegrationTest() throws IOException {
        sourceProp = BeaconTestUtil.getProperties("beacon-source-server.properties");
        targetProp = BeaconTestUtil.getProperties("beacon-target-server.properties");
    }

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

    @BeforeMethod
    public void printTestname(Method testMethod) {
        String testName = this.getClass().getSimpleName() + "#" + testMethod.getName();
        NDC.push(testName);
        startTime = System.currentTimeMillis();
    }

    @AfterMethod
    public void afterTest(ITestResult result) {
        LOG.debug("Time taken: {} msecs", System.currentTimeMillis() - startTime);

        if (result.getStatus() == ITestResult.FAILURE) {
            LOG.error("Test Failed", result.getThrowable());
        }
        NDC.pop();
    }

    public void setupBeaconServers(Method testMethod) throws Exception {
        String sourceExtraClassPath;
        String submitHAClusterTestName = "testSubmitHACluster";
        if (testMethod != null && testMethod.getName().equals(submitHAClusterTestName)) {
            sourceExtraClassPath = System.getProperty("user.dir") + "/src/test/resources/sourceHA/:";
        } else {
            sourceExtraClassPath = System.getProperty("user.dir") + "/src/test/resources/source/:";
        }
        sourceCluster = ProcessHelper.startNew(StringUtils.join(sourceJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(), sourceExtraClassPath,
                new String[]{"beacon-source-server.properties"});

        targetCluster = ProcessHelper.startNew(StringUtils.join(targetJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(), System.getProperty("user.dir")
                        + "/src/test/resources/tgt/:", new String[]{"beacon-target-server.properties"});

        sourceClient = new BeaconWebClient(getSourceBeaconServer());
        targetClient = new BeaconWebClient(getTargetBeaconServer());
    }

    public void teardownBeaconServers() throws Exception {
        ProcessHelper.killProcess(sourceCluster);
        ProcessHelper.killProcess(targetCluster);
    }

    public String getSourceBeaconServer() {
        return "http://" + sourceProp.getProperty("beacon.host") + ":" + sourceProp.getProperty("beacon.port");
    }

    public String getTargetBeaconServer() {
        return "http://" + targetProp.getProperty("beacon.host") + ":" + targetProp.getProperty("beacon.port");
    }

    public String getTargetBeaconServerHostName() {
        return targetProp.getProperty("beacon.host");
    }

    /**
     * I am keeping the port option for potential future use.  For now all callers use 0 for this.
     * @param port
     * @param path
     * @return
     * @throws Exception
     */
    protected MiniDFSCluster startMiniHDFS(int port, String path) throws Exception {
        MiniDFSCluster dfsCluster = MiniHDFSClusterUtil.initMiniDfs(port, new File(path));
        LOG.debug("Started mini dfs cluster at {}", dfsCluster.getURI());
        return dfsCluster;
    }

    protected void shutdownMiniHDFS(MiniDFSCluster dfsCluster) {
        if (dfsCluster != null) {
            LOG.debug("Shutting down dfs cluster at {}", dfsCluster.getURI());
            dfsCluster.shutdown(true);
        }
    }

    protected void validatePolicyList(String api, int numResults, int totalResults,
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

    protected String getRandomString(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(5);
    }

    protected String getPolicyListResponse(String api, String beaconServer) throws IOException {
        HttpURLConnection conn = sendRequest(beaconServer + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream, "should not be null.");
        return getResponseMessage(inputStream);
    }

    protected void submitCluster(String cluster, String clusterBeaconServer,
                               String server, String fsEndPoint, boolean isLocal) throws Exception {
        submitCluster(cluster, clusterBeaconServer, server, fsEndPoint, null, isLocal);
    }

    protected void submitCluster(String clusterName, String clusterBeaconServer,
                               String server, String fsEndPoint,
                               Map<String, String> clusterCustomProperties, boolean isLocal) throws Exception {
        PropertiesIgnoreCase cluster =
                getClusterData(clusterName, clusterBeaconServer, fsEndPoint, clusterCustomProperties, isLocal);
        BeaconClient client = new BeaconWebClient(server);
        client.submitCluster(clusterName, cluster);
    }

    protected PropertiesIgnoreCase getClusterData(String clusterName, String server, String fsEndPoint,
                                                Map<String, String> customProperties, boolean isLocal) {
        Cluster cluster = new Cluster();
        cluster.setName(clusterName);
        cluster.setFsEndpoint(fsEndPoint);
        cluster.setDescription("source cluster description");
        cluster.setBeaconEndpoint(server);
        cluster.setLocal(isLocal);
        cluster.setTags(Arrays.asList("consumer", "owner"));
        cluster.setAtlasEndpoint("http://localhost:21000");
        cluster.setRangerEndpoint("http://localhost:6080");
        if (customProperties != null) {
            cluster.getCustomProperties().putAll(customProperties);
        }
        return cluster.asProperties();
    }

    protected void pairClusterFailed(String beaconServer, String remoteCluster) throws Exception {
        String api = BASE_API + "cluster/pair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    protected void unpairCluster(String beaconServer, String localCluster,
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
        assertEquals(jsonObject.getString("peers"), "[]");

        String cluster2Message = getClusterResponse(remoteCluster, getSourceBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        assertEquals(jsonObject.getString("name"), remoteCluster);
        assertEquals(jsonObject.getString("peers"), "[]");
    }

    protected void validateListClusterWithPeers(boolean hasPeers) throws Exception {
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

    protected void unpairWrongClusters(String beaconServer, String remoteCluster) throws IOException, JSONException {
        StringBuilder unPairAPI = new StringBuilder(BASE_API + "cluster/unpair");
        unPairAPI.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + unPairAPI.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.NOT_FOUND.getStatusCode());
    }

    protected void unpairClusterFailed(String beaconServer, String remoteCluster) throws IOException, JSONException {
        String api = BASE_API + "cluster/unpair";
        StringBuilder builder = new StringBuilder(api);
        builder.append("?").append("remoteClusterName=").append(remoteCluster);
        HttpURLConnection conn = sendRequest(beaconServer + builder.toString(), null, POST);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.BAD_REQUEST.getStatusCode());
    }

    protected HttpURLConnection sendRequest(String beaconUrl, String data, String method) throws IOException {
        LOG.debug("Calling API path: {}, method: {}", beaconUrl, method);
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

    protected void pairCluster(String beaconServer, String localCluster, String remoteCluster)
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
        validatePeers(jsonObject.getJSONArray("peers"), remoteCluster);

        String cluster2Message = getClusterResponse(remoteCluster, getTargetBeaconServer());
        jsonObject = new JSONObject(cluster2Message);
        assertEquals(jsonObject.getString("name"), remoteCluster);
        validatePeers(jsonObject.getJSONArray("peers"), localCluster);
    }

    protected String getResponseMessage(InputStream inputStream) throws IOException {
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

    protected String getClusterResponse(String clusterName, String serverEndpoint) throws IOException {
        String api = BASE_API + "cluster/getEntity/" + clusterName;
        HttpURLConnection conn = sendRequest(serverEndpoint + api, null, GET);
        int responseCode = conn.getResponseCode();
        assertEquals(responseCode, Response.Status.OK.getStatusCode());
        InputStream inputStream = conn.getInputStream();
        Assert.assertNotNull(inputStream);
        return getResponseMessage(inputStream);
    }

    protected void validatePeers(JSONArray peers, String cluster) throws JSONException {
        if (peers.length() > 1) {
            boolean found = false;
            for (int i=0; i<peers.length(); i++) {
                String peer = peers.getString(i);
                if (peer.equalsIgnoreCase(cluster)) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        } else {
            assertEquals(peers.get(0), cluster);
        }
    }

    protected void submitAndSchedule(String policyName, int frequency, String sourceDataset, String targetDataSet,
                                   Properties properties) throws IOException, JSONException {
        PropertiesIgnoreCase policy = getPolicyData(policyName, FS, frequency, sourceDataset, targetDataSet,
                SOURCE_CLUSTER, TARGET_CLUSTER);
        policy.putAll(properties);
        StringBuilder api = new StringBuilder(getTargetBeaconServer() + BASE_API + "policy/submitAndSchedule/"
                + policyName);
        // Submit and Schedule job using submitAndSchedule API
        StringWriter stringWriter = new StringWriter();
        policy.store(stringWriter, "");
        HttpURLConnection conn = sendRequest(api.toString(), stringWriter.toString(), POST);
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

    protected PropertiesIgnoreCase getPolicyData(String policyName, String type, int freq, String sourceDataset,
                                               String targetDataSet, String srcCluster, String trgtCluster) {
        ReplicationPolicy policy = new ReplicationPolicy();
        policy.setName(policyName);
        policy.setType(type);
        policy.setDescription("Beacon test policy");
        policy.setFrequencyInSec(freq);
        policy.setSourceDataset(sourceDataset);
        policy.setTargetDataset(targetDataSet);
        policy.setSourceCluster(srcCluster);
        policy.setTargetCluster(trgtCluster);
        policy.getCustomProperties().setProperty("distcpMaxMaps", "1");
        policy.getCustomProperties().setProperty("distcpMapBandwidth", "10");
        policy.getCustomProperties().setProperty("sourceSnapshotRetentionAgeLimit", "10");
        policy.getCustomProperties().setProperty("sourceSnapshotRetentionNumber", "1");
        policy.getCustomProperties().setProperty("targetSnapshotRetentionAgeLimit", "10");
        policy.getCustomProperties().setProperty("targetSnapshotRetentionNumber", "1");
        policy.getCustomProperties().setProperty("tags", "owner=producer@xyz.com,component=sales");
        policy.getCustomProperties().setProperty("retryAttempts", "3");
        policy.getCustomProperties().setProperty("retryDelay", "120");
        policy.getCustomProperties().setProperty("user", System.getProperty("user.name"));
        return policy.asProperties();
    }

    protected void deleteClusterAndValidate(String api, String serverEndpoint, String cluster)
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

    protected void deletePolicy(String policyName) throws IOException, JSONException {
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

    protected void verifyLatestPolicyInstanceStatus(BeaconClient client, String policyName, JobStatus jobStatus)
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

    protected void verifyClusterPairStatus(String beaconServer, ClusterStatus clusterStatus)
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

    protected void updateCluster(String cluster, String beaconServer, Properties properties) throws IOException {
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

    protected void assertExists(EventsResult events, EventsResult.EventInstance expectedEvent) {
        if (events != null) {
            for (EventsResult.EventInstance event : events.getEvents()) {
                if (equals(event.event, expectedEvent.event)
                        && equals(event.eventType, expectedEvent.eventType)
                        && equals(event.syncEvent, expectedEvent.syncEvent)) {
                    return;
                }
            }
        }
        fail(StringFormat.format("Didn't find {} in {}", expectedEvent, events));
    }

    private boolean equals(Object actual, Object expected) {
        if (expected != null) {
            return actual.equals(expected);
        }
        return true;
    }
}
