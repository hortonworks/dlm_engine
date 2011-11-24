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

package com.hortonworks.beacon.client;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import com.hortonworks.beacon.util.SSLUtils;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.web.KerberosUgiAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Properties;

/**
 * Client API to submit and manage Beacon resources (Cluster, Policies).
 */
public class BeaconWebClient implements BeaconClient {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconWebClient.class);
    private static final String IS_INTERNAL_PAIRING = "isInternalPairing";
    private static final String IS_INTERNAL_DELETE = "isInternalSyncDelete";
    private static final String IS_INTERNAL_STATUSSYNC = "isInternalStatusSync";

    private static final String IS_INTERNAL_UNPAIRING = "isInternalUnpairing";

    private static final String NEW_LINE = System.lineSeparator();
    public static final String ORDER_BY = "orderBy";
    public static final String SORT_ORDER = "sortOrder";
    public static final String OFFSET = "offset";
    public static final String NUM_RESULTS = "numResults";
    private static final String FIELDS = "fields";

    private static final String PARAM_FILTERBY = "filterBy";
    private static final String PATH = "filePath";
    private static final String CRED_ID = "credId";

    public static final String REMOTE_CLUSTERNAME = "remoteClusterName";
    public static final String STATUS = "status";

    private static final PropertiesUtil AUTHCONFIG = PropertiesUtil.getInstance();
    private static final String BEACON_BASIC_AUTH_ENABLED="beacon.basic.authentication.enabled";
    private static final String BEACON_USERNAME = "beacon.username";
    private static final String BEACON_PASSWORD = "beacon.password";


    private final WebResource service;

    private AuthenticatedURL.Token authToken;
    private final String knoxBaseURL;
    /**
     * debugMode=false means no debugging. debugMode=true means debugging on.
     */
    private boolean debugMode = false;

    private final Properties clientProperties;
    private String ssoToken;

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl of the server to which client interacts
     * @ - If unable to initialize SSL Props
     */
    public BeaconWebClient(String beaconUrl) throws BeaconClientException {
        this(beaconUrl, null, new Properties());
    }

    public BeaconWebClient(String beaconUrl, String knoxBaseUrl) throws BeaconClientException {
        this(beaconUrl, knoxBaseUrl, new Properties());
    }

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl  of the server to which client interacts
     * @param properties client properties
     * @ - If unable to initialize SSL Props
     */
    public BeaconWebClient(String beaconUrl, String knoxBaseUrl, Properties properties) throws BeaconClientException {
        this.knoxBaseURL = knoxBaseUrl;
        try {
            String baseUrl = notEmpty(beaconUrl, "BeaconUrl");
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            this.clientProperties = properties;
            SSLContext sslContext = SSLUtils.getSSLContext();
            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                    new HTTPSProperties(SSLUtils.HOSTNAME_VERIFIER, sslContext)
            );
            config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
            boolean isBasicAuthentication = AUTHCONFIG.getBooleanProperty(BEACON_BASIC_AUTH_ENABLED, true);


            Client client =  Client.create(config);

            if (isBasicAuthentication) {
                String username=AUTHCONFIG.getProperty(BEACON_USERNAME);
                LOG.debug("Beacon Username: {}", username);
                String password = null;
                try {
                    password = AUTHCONFIG.resolvePassword(BEACON_PASSWORD);
                } catch (Exception ex) {
                    password = null;
                }
                if (StringUtils.isEmpty(password)) {
                    password = AUTHCONFIG.getProperty(BEACON_PASSWORD);
                }
                client.addFilter(new HTTPBasicAuthFilter(username, password));
            }
            client.setConnectTimeout(Integer.parseInt(clientProperties.getProperty("beacon.connect.timeout",
                    "180000")));
            client.setReadTimeout(Integer.parseInt(clientProperties.getProperty("beacon.read.timeout", "180000")));
            service = client.resource(UriBuilder.fromUri(baseUrl).build());
        } catch (Exception e) {
            LOG.error("Unable to initialize Beacon Client object. Cause: {}", e.getMessage(), e);
            throw new BeaconClientException(e, "Unable to initialize Beacon Client object. Cause: {}", e.getMessage());
        }
    }

    /**
     * @return current debug Mode
     */
    public boolean getDebugMode() {
        return debugMode;
    }

    private String getSSOToken() {
        if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            try {
                return KnoxTokenUtils.getKnoxSSOToken(knoxBaseURL);
            } catch (Exception e) {
                LOG.error("Unable to get knox sso token from {} : {} . Cause: {}", knoxBaseURL, e.getMessage(), e);
                return null;
            }
        }
        return null;
    }

    private AuthenticatedURL.Token getToken(final String baseUrl)  {


        if (UserGroupInformation.isSecurityEnabled()) {
            try {
                return UserGroupInformation.getLoginUser().doAs(
                        new PrivilegedAction<AuthenticatedURL.Token>() {
                            public AuthenticatedURL.Token run() {
                                try {
                                    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
                                    Authenticator authenticator = new KerberosUgiAuthenticator();
                                    authenticator.authenticate(new URL(baseUrl + "api/beacon/admin/status"), token);
                                    return token;
                                } catch (Exception e) {
                                    return null;
                                }
                            }
                        });
            } catch (IOException ioe) {
                return null;
            }
        } else {
            return null;
        }
    }

    private String getJWTToken() {
        return null;
    }

    /**
     * Set debug mode.
     *
     * @param debugMode : debugMode=false means no debugging. debugMode=true means debugging on
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    /**
     * Methods allowed on Entity Resources.
     */
    private static final String API_PREFIX = "api/beacon/";
    private static final String API_CLOUD_CRED = API_PREFIX + "cloudcred/";

    protected enum API {
        //Cluster operations
        CLUSTER_LIST("api/beacon/cluster/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTER_SUBMIT("api/beacon/cluster/submit/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        CLUSTER_GET("api/beacon/cluster/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTER_STATUS("api/beacon/cluster/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTER_DELETE("api/beacon/cluster/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        CLUSTER_PAIR("api/beacon/cluster/pair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        CLUSTER_UNPAIR("api/beacon/cluster/unpair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        CLUSTER_UPDATE("api/beacon/cluster/", HttpMethod.PUT, MediaType.APPLICATION_JSON),

        //Policy operations
        POLICY_SUBMITANDSCHEDULE("api/beacon/policy/submitAndSchedule/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_DRYRUN("api/beacon/policy/dryrun/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_LIST("api/beacon/policy/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICY_STATUS("api/beacon/policy/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICY_GET("api/beacon/policy/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICY_DELETE("api/beacon/policy/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        POLICY_SUSPEND("api/beacon/policy/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_RESUME("api/beacon/policy/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_INSTANCE_LIST(API_PREFIX + "instance/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICY_INSTANCE_ABORT("api/beacon/policy/instance/abort/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_INSTANCE_RERUN("api/beacon/policy/instance/rerun/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_LOGS(API_PREFIX + "logs", HttpMethod.GET, MediaType.APPLICATION_JSON),

        //Internal policy operations
        POLICY_SYNC("api/beacon/policy/sync/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_SYNCSTATUS("api/beacon/policy/syncStatus/", HttpMethod.POST, MediaType.APPLICATION_JSON),

        //Admin operations
        ADMIN_STATUS(API_PREFIX + "admin/status", HttpMethod.GET, MediaType.APPLICATION_JSON),
        ADMIN_VERSION(API_PREFIX + "admin/version", HttpMethod.GET, MediaType.APPLICATION_JSON),

        //Beacon Resource operations
        LIST_FILES(API_PREFIX + "file/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LIST_DBS(API_PREFIX + "hive/listDBs", HttpMethod.GET, MediaType.APPLICATION_JSON),
        USER_PRIVILEGES_GET(API_PREFIX + "user/", HttpMethod.GET, MediaType.APPLICATION_JSON),

        //Cloud Cred operations
        SUBMIT_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.POST, MediaType.APPLICATION_JSON),
        UPDATE_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.PUT, MediaType.APPLICATION_JSON),
        DELETE_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        GET_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.GET, MediaType.APPLICATION_JSON),
        LIST_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.GET, MediaType.APPLICATION_JSON),
        VALIDATE_CLOUD_CRED(API_CLOUD_CRED, HttpMethod.GET, MediaType.APPLICATION_JSON);

        private String path;
        private String method;
        private String mimeType;

        API(String path, String method, String mimeType) {
            this.path = path;
            this.method = method;
            this.mimeType = mimeType;
        }
    }

    private String notEmpty(String str, String name) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(name + "cannot be null or empty");
        }
        return str;
    }

    @Override
    public void submitCluster(String clusterName, String filePath) throws BeaconClientException {
        submitEntity(API.CLUSTER_SUBMIT, clusterName, filePath);
    }

    @Override
    public void submitAndScheduleReplicationPolicy(String policyName, String filePath) throws BeaconClientException {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_SUBMITANDSCHEDULE.path, policyName)
                .call(API.POLICY_SUBMITANDSCHEDULE, entityStream);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public void dryrunPolicy(String policyName, String filePath) throws BeaconClientException {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_DRYRUN.path, policyName)
                .call(API.POLICY_DRYRUN, entityStream);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                      Integer offset, Integer numResults) throws BeaconClientException {
        return getClusterList(API.CLUSTER_LIST, fields, orderBy, null, sortOrder, offset, numResults);
    }

    @Override
    public PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder,
                                    Integer offset, Integer numResults) throws BeaconClientException {
        return getPolicyList(API.POLICY_LIST, fields, orderBy, filterBy, sortOrder, offset, numResults);
    }

    @Override
    public Entity.EntityStatus getClusterStatus(String clusterName) throws BeaconClientException {
        return getEntityStatus(API.CLUSTER_STATUS, clusterName);
    }

    @Override
    public Entity.EntityStatus getPolicyStatus(String policyName) throws BeaconClientException {
        return getEntityStatus(API.POLICY_STATUS, policyName);
    }

    @Override
    public Cluster getCluster(String clusterName) throws BeaconClientException {
        return getEntity(API.CLUSTER_GET, clusterName, Cluster.class);
    }

    @Override
    public ReplicationPolicy getPolicy(String policyName) throws BeaconClientException {
        return getEntity(API.POLICY_GET, policyName, ReplicationPolicy.class);
    }

    @Override
    public void deleteCluster(String clusterName) throws BeaconClientException {
        doEntityOperation(API.CLUSTER_DELETE, clusterName);
    }

    @Override
    public void deletePolicy(String policyName, boolean isInternalSyncDelete) throws BeaconClientException {
        deletePolicyInternal(policyName, isInternalSyncDelete);
    }

    @Override
    public void suspendPolicy(String policyName) throws BeaconClientException {
        doEntityOperation(API.POLICY_SUSPEND, policyName);
    }

    @Override
    public void resumePolicy(String policyName) throws BeaconClientException {
        doEntityOperation(API.POLICY_RESUME, policyName);
    }

    @Override
    public void pairClusters(String remoteClusterName, boolean isInternalPairing) throws BeaconClientException {
        pair(remoteClusterName, isInternalPairing);
    }

    @Override
    public void unpairClusters(String remoteClusterName,
                                    boolean isInternalUnpairing) throws BeaconClientException {
        unpair(remoteClusterName, isInternalUnpairing);
    }

    @Override
    public void syncPolicy(String policyName, String policyDefinition) throws BeaconClientException {
        syncEntity(API.POLICY_SYNC, policyName, policyDefinition);
    }

    @Override
    public void syncPolicyStatus(String policyName, String status,
                                      boolean isInternalStatusSync) throws BeaconClientException {
        syncStatus(policyName, status, isInternalStatusSync);
    }

    @Override
    public ServerStatusResult getServiceStatus() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_STATUS.path).call(API.ADMIN_STATUS);
        return getResponse(clientResponse, ServerStatusResult.class);
    }

    @Override
    public ServerVersionResult getServiceVersion() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_VERSION.path).call(API.ADMIN_VERSION);
        return getResponse(clientResponse, ServerVersionResult.class);
    }

    @Override
    public PolicyInstanceList listPolicyInstances(String policyName) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_INSTANCE_LIST.path)
                .addQueryParam(PARAM_FILTERBY, "name:" + policyName)
                .call(API.POLICY_INSTANCE_LIST);
        return getResponse(clientResponse, PolicyInstanceList.class);
    }

    @Override
    public void abortPolicyInstance(String policyName) throws BeaconClientException {
        doEntityOperation(API.POLICY_INSTANCE_ABORT, policyName);
    }

    @Override
    public void updateCluster(String clusterName, String filePath) throws BeaconClientException {
        submitEntity(API.CLUSTER_UPDATE, clusterName, filePath);
    }

    @Override
    public void rerunPolicyInstance(String policyName) throws BeaconClientException {
        doEntityOperation(API.POLICY_INSTANCE_RERUN, policyName);
    }

    @Override
    public String getPolicyLogs(String policyName) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_LOGS.path)
                .addQueryParam(PARAM_FILTERBY, "policyname:" + policyName)
                .call(API.POLICY_LOGS);
        APIResult result = getResponse(clientResponse, APIResult.class);
        return result.getMessage();
    }

    @Override
    public String getPolicyLogsForId(String policId) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_LOGS.path)
                .addQueryParam(PARAM_FILTERBY, "policyid:" + policId)
                .call(API.POLICY_LOGS);
        APIResult result = getResponse(clientResponse, APIResult.class);
        return result.getMessage();
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     */
    private InputStream getServletInputStreamFromFile(String filePath) throws BeaconClientException {

        if (filePath == null) {
            return null;
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new BeaconClientException(e, "File not found:"  + filePath);
        }
        return stream;
    }

    private <T> T getResponse(ClientResponse clientResponse, Class<T> clazz) throws BeaconClientException {
        logClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(clazz);
    }

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
    private class ResourceBuilder {
        private WebResource resource;

        private ResourceBuilder path(String... paths) {
            for (String path : paths) {
                if (resource == null) {
                    resource = service.path(path);
                } else {
                    resource = resource.path(path);
                }
            }
            return this;
        }

        public ResourceBuilder addQueryParam(String paramName, Integer value) {
            if (value != null) {
                resource = resource.queryParam(paramName, value.toString());
            }
            return this;
        }

        public ResourceBuilder addQueryParam(String paramName, String paramValue) {
            if (StringUtils.isNotBlank(paramValue)) {
                resource = resource.queryParam(paramName, paramValue);
            }
            return this;
        }

        private ClientResponse call(API entities) throws BeaconClientException {
            setSSOToken(getSSOToken());
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(entities.mimeType);
            builder.type(MediaType.TEXT_PLAIN);
            if (ssoToken != null) {
                builder.cookie(new Cookie("hadoop-jwt", ssoToken));
            } else {
                if (authToken != null && authToken.isSet()) {
                    builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
                }
            }
            try {
                return builder.method(entities.method, ClientResponse.class);
            } catch (ClientHandlerException e) {
                throw new BeaconClientException(e, "Failed to connect to {}", service.getURI());
            }
        }

        public ClientResponse call(API operation, Object entityDefinition) {
            setSSOToken(getSSOToken());
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(operation.mimeType);
            builder.type(MediaType.APPLICATION_OCTET_STREAM_TYPE);
            if (ssoToken != null) {
                builder.cookie(new Cookie("hadoop-jwt", ssoToken));
            } else {
                if (authToken != null && authToken.isSet()) {
                    builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
                }
            }
            return builder.method(operation.method, ClientResponse.class, entityDefinition);
        }
    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    private void checkIfSuccessful(ClientResponse clientResponse) throws BeaconClientException {
        Response.Status.Family statusFamily = clientResponse.getClientResponseStatus().getFamily();
        if (statusFamily != Response.Status.Family.SUCCESSFUL && statusFamily != Response.Status.Family.INFORMATIONAL) {
            throw BeaconClientException.fromResponse(clientResponse);
        }
    }

    private void logClientResponse(ClientResponse clientResponse) {
        if (getDebugMode()) {
            LOG.debug(clientResponse.toString());
        }
    }

    private void submitEntity(API operation, String entityName, String filePath) throws BeaconClientException {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation, entityStream);
        getResponse(clientResponse, APIResult.class);
    }

    private void pair(String remoteClusterName,
                           boolean isInternalPairing) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.CLUSTER_PAIR.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_PAIRING, Boolean.toString(isInternalPairing))
                .call(API.CLUSTER_PAIR);
        getResponse(clientResponse, APIResult.class);
    }

    private void unpair(String remoteClusterName,
                             boolean isInternalUnpairing) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.CLUSTER_UNPAIR.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_UNPAIRING, Boolean.toString(isInternalUnpairing))
                .call(API.CLUSTER_UNPAIR);
        getResponse(clientResponse, APIResult.class);
    }

    private void syncEntity(API operation, String entityName, String entityDefinition) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation, entityDefinition);
        getResponse(clientResponse);
    }

    private APIResult getResponse(ClientResponse clientResponse) throws BeaconClientException {
        logClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return new APIResult(clientResponse.getStatus() == 200 ? APIResult.Status.SUCCEEDED : APIResult.Status.FAILED,
            clientResponse.getEntity(String.class));
    }

    private void syncStatus(String policyName, String status,
                                 boolean isInternalStatusSync) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_SYNCSTATUS.path, policyName)
                .addQueryParam(STATUS, status)
                .addQueryParam(IS_INTERNAL_STATUSSYNC, Boolean.toString(isInternalStatusSync))
                .call(API.POLICY_SYNCSTATUS);
        getResponse(clientResponse, APIResult.class);
    }

    private ClusterList getClusterList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                       Integer offset, Integer numResults) throws BeaconClientException {
        ClientResponse response =
                getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        ClusterList result = response.getEntity(ClusterList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private PolicyList getPolicyList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                     Integer offset, Integer numResults) throws BeaconClientException {
        ClientResponse response =
                getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        PolicyList result = response.getEntity(PolicyList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private ClientResponse getEntityListResponse(API operation, String fields, String orderBy, String filterBy,
                                    String sortOrder, Integer offset, Integer numResults)
                                    throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path)
                .addQueryParam(NUM_RESULTS, numResults)
                .addQueryParam(PARAM_FILTERBY, filterBy)
                .addQueryParam(OFFSET, offset).addQueryParam(SORT_ORDER, sortOrder)
                .addQueryParam(ORDER_BY, orderBy).addQueryParam(FIELDS, fields)
                .call(operation);

        logClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse;
    }

    private Entity.EntityStatus getEntityStatus(API operation, String entityName) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(entityName).call(operation);
        StatusResult statusResult = getResponse(clientResponse, StatusResult.class);
        return statusResult.getStatus();
    }

    private <T> T getEntity(API operation, String entityName, Class<T> aClass) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        return getResponse(clientResponse, aClass);
    }

    private void doEntityOperation(API operation, String entityName) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        getResponse(clientResponse, APIResult.class);
    }

    private void deletePolicyInternal(String entityName,
                                   boolean isInternalSyncDelete) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_DELETE.path, entityName)
                .addQueryParam(IS_INTERNAL_DELETE, Boolean.toString(isInternalSyncDelete))
                .call(API.POLICY_DELETE);
        getResponse(clientResponse, APIResult.class);
    }

    private void setAuthToken(AuthenticatedURL.Token token) {
        this.authToken = token;
    }

    private void setSSOToken(String token) {
        this.ssoToken = token;
    }
    @Override
    public String submitCloudCred(CloudCred cloudCred) throws BeaconClientException {
        InputStream stream = getStream(cloudCred);
        ClientResponse clientResponse = new ResourceBuilder().path(API.SUBMIT_CLOUD_CRED.path)
                .call(API.SUBMIT_CLOUD_CRED, stream);
        return getResponse(clientResponse, APIResult.class).getEntityId();
    }

    private InputStream getStream(CloudCred cloudCred) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotBlank(cloudCred.getName())) {
            builder.append("name=").append(cloudCred.getName()).append(NEW_LINE);
        }
        if (cloudCred.getProvider() != null) {
            builder.append("provider=").append(cloudCred.getProvider().name()).append(NEW_LINE);
        }
        if (cloudCred.getAuthType() != null) {
            builder.append("authtype=").append(cloudCred.getAuthType()).append(NEW_LINE);
        }
        Map<Config, String> configs = cloudCred.getConfigs();
        for (Map.Entry<Config, String> entry : configs.entrySet()) {
            builder.append(entry.getKey().getName()).append("=").append(entry.getValue()).append(NEW_LINE);
        }
        return new ByteArrayInputStream(builder.toString().getBytes());
    }

    @Override
    public void updateCloudCred(String cloudCredId, CloudCred cloudCred) throws BeaconClientException {
        InputStream stream = getStream(cloudCred);
        ClientResponse clientResponse = new ResourceBuilder().path(API.UPDATE_CLOUD_CRED.path, cloudCredId)
                .call(API.UPDATE_CLOUD_CRED, stream);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public void deleteCloudCred(String cloudCredId) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.DELETE_CLOUD_CRED.path, cloudCredId)
                .call(API.DELETE_CLOUD_CRED);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public CloudCred getCloudCred(String cloudCredId) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.GET_CLOUD_CRED.path, cloudCredId)
                .call(API.GET_CLOUD_CRED);
        return getResponse(clientResponse, CloudCred.class);
    }

    @Override
    public void validateCloudPath(String cloudCredId, String path) throws BeaconClientException {
        String validatePath = API.VALIDATE_CLOUD_CRED.path + cloudCredId + "/validate";
        ClientResponse clientResponse = new ResourceBuilder().path(validatePath)
                .addQueryParam(PATH, path)
                .call(API.VALIDATE_CLOUD_CRED);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public CloudCredList listCloudCred(String filterBy, String orderBy, String sortOrder, Integer offset,
                                       Integer resultsPerPage) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.LIST_CLOUD_CRED.path)
                .addQueryParam(PARAM_FILTERBY, filterBy)
                .addQueryParam(ORDER_BY, orderBy)
                .addQueryParam(SORT_ORDER, sortOrder)
                .addQueryParam(OFFSET, offset)
                .addQueryParam(NUM_RESULTS, resultsPerPage)
                .call(API.LIST_CLOUD_CRED);
        return getResponse(clientResponse, CloudCredList.class);
    }

    @Override
    public FileListResult listFiles(String path, String cloudCredId) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.LIST_FILES.path)
                .addQueryParam(PATH, path)
                .addQueryParam(CRED_ID, cloudCredId)
                .call(API.LIST_FILES);
        return getResponse(clientResponse, FileListResult.class);
    }

    @Override
    public FileListResult listFiles(String path) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.LIST_FILES.path)
                .addQueryParam(PATH, path)
                .call(API.LIST_FILES);
        return getResponse(clientResponse, FileListResult.class);
    }

    @Override
    public DBListResult listDBs() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.LIST_DBS.path)
                .call(API.LIST_DBS);
        return getResponse(clientResponse, DBListResult.class);
    }

    @Override
    public UserPrivilegesResult getUserPrivileges() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.USER_PRIVILEGES_GET.path)
                .call(API.USER_PRIVILEGES_GET);
        return getResponse(clientResponse, UserPrivilegesResult.class);
    }

    @Override
    public String toString() {
        return service.getURI().toString();
    }
}
