/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.hadoop.hdfs.web.KerberosUgiAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

/**
 * Client API to submit and manage Beacon resources (Cluster, Policies).
 */
public class BeaconWebClient implements BeaconClient {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconWebClient.class);
    private static final String IS_INTERNAL_PAIRING = "isInternalPairing";
    private static final String IS_INTERNAL_DELETE = "isInternalSyncDelete";
    private static final String IS_INTERNAL_STATUSSYNC = "isInternalStatusSync";

    private static final String IS_INTERNAL_UNPAIRING = "isInternalUnpairing";

    public static final String ORDER_BY = "orderBy";
    public static final String SORT_ORDER = "sortOrder";
    public static final String OFFSET = "offset";
    public static final String NUM_RESULTS = "numResults";
    private static final String FIELDS = "fields";

    private static final String PARAM_FILTERBY = "filterBy";

    public static final String REMOTE_CLUSTERNAME = "remoteClusterName";
    public static final String STATUS = "status";

    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    private static final String BEACON_BASIC_AUTH_ENABLED="beacon.basic.authentication.enabled";
    private static final String BEACON_USERNAME = "beacon.username";
    private static final String BEACON_PASSWORD = "beacon.password";

    public static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession sslSession) {
            return true;
        }
    };
    private final WebResource service;

    private AuthenticatedURL.Token authToken;

    /**
     * debugMode=false means no debugging. debugMode=true means debugging on.
     */
    private boolean debugMode = false;

    private final Properties clientProperties;

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl of the server to which client interacts
     * @ - If unable to initialize SSL Props
     */
    public BeaconWebClient(String beaconUrl) throws BeaconClientException {
        this(beaconUrl, new Properties());
    }

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl  of the server to which client interacts
     * @param properties client properties
     * @ - If unable to initialize SSL Props
     */
    public BeaconWebClient(String beaconUrl, Properties properties) throws BeaconClientException {
        try {
            String baseUrl = notEmpty(beaconUrl, "BeaconUrl");
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            this.clientProperties = properties;
            SSLContext sslContext = getSslContext();
            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                    new HTTPSProperties(ALL_TRUSTING_HOSTNAME_VERIFIER, sslContext)
            );
            config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
            boolean isBasicAuthentication = AUTHCONFIG.getBooleanProperty(BEACON_BASIC_AUTH_ENABLED, true);


            Client client =  Client.create(config);

            if (isBasicAuthentication) {
                String username=AUTHCONFIG.getProperty(BEACON_USERNAME);
                LOG.info("Beacon Username: {}", username);
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

    private static SSLContext getSslContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(
                null,
                new TrustManager[]{TrustManagerUtils.getValidateServerCertificateTrustManager()},
                new SecureRandom());
        return sslContext;
    }

    /**
     * @return current debug Mode
     */
    public boolean getDebugMode() {
        return debugMode;
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

    protected enum API {
        //Cluster operations
        SUBMITCLUSTER("api/beacon/cluster/submit/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LISTCLUSTER("api/beacon/cluster/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTERSTATUS("api/beacon/cluster/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETCLUSTER("api/beacon/cluster/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        DELETECLUSTER("api/beacon/cluster/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        PAIRCLUSTERS("api/beacon/cluster/pair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        UNPAIRCLUSTERS("api/beacon/cluster/unpair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        UPDATE_CLUSTER("api/beacon/cluster/", HttpMethod.PUT, MediaType.APPLICATION_JSON),

        //policy operations
        SUBMITANDSCHEDULEPOLICY("api/beacon/policy/submitAndSchedule/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LISTPOLICY("api/beacon/policy/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICYSTATUS("api/beacon/policy/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETPOLICY("api/beacon/policy/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SYNCPOLICY("api/beacon/policy/sync/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SYNCPOLICYSTATUS("api/beacon/policy/syncStatus/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        DELETEPOLICY("api/beacon/policy/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        SUSPENDPOLICY("api/beacon/policy/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RESUMEPOLICY("api/beacon/policy/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON),

        //policy instance operations
        POLICY_INSTANCE_LIST(API_PREFIX + "instance/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        ABORT_POLICY_INSTANCE("api/beacon/policy/instance/abort/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RERUN_POLICY_INSTANCE("api/beacon/policy/instance/rerun/", HttpMethod.POST, MediaType.APPLICATION_JSON),

        //Admin operations
        ADMIN_STATUS(API_PREFIX + "admin/status", HttpMethod.GET, MediaType.APPLICATION_JSON),
        ADMIN_VERSION(API_PREFIX + "admin/version", HttpMethod.GET, MediaType.APPLICATION_JSON);

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
        submitEntity(API.SUBMITCLUSTER, clusterName, filePath);
    }

    @Override
    public void submitAndScheduleReplicationPolicy(String policyName, String filePath) throws BeaconClientException {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(API.SUBMITANDSCHEDULEPOLICY.path, policyName)
                .call(API.SUBMITANDSCHEDULEPOLICY, entityStream);
        getResponse(clientResponse, APIResult.class);
    }

    @Override
    public ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                      Integer offset, Integer numResults) throws BeaconClientException {
        return getClusterList(API.LISTCLUSTER, fields, orderBy, null, sortOrder, offset, numResults);
    }

    @Override
    public PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder,
                                    Integer offset, Integer numResults) throws BeaconClientException {
        return getPolicyList(API.LISTPOLICY, fields, orderBy, filterBy, sortOrder, offset, numResults);
    }

    @Override
    public Entity.EntityStatus getClusterStatus(String clusterName) throws BeaconClientException {
        return getEntityStatus(API.CLUSTERSTATUS, clusterName);
    }

    @Override
    public Entity.EntityStatus getPolicyStatus(String policyName) throws BeaconClientException {
        return getEntityStatus(API.POLICYSTATUS, policyName);
    }

    @Override
    public Cluster getCluster(String clusterName) throws BeaconClientException {
        return getEntity(API.GETCLUSTER, clusterName, Cluster.class);
    }

    @Override
    public ReplicationPolicy getPolicy(String policyName) throws BeaconClientException {
        return getEntity(API.GETPOLICY, policyName, ReplicationPolicy.class);
    }

    @Override
    public void deleteCluster(String clusterName) throws BeaconClientException {
        doEntityOperation(API.DELETECLUSTER, clusterName);
    }

    @Override
    public void deletePolicy(String policyName, boolean isInternalSyncDelete) throws BeaconClientException {
        deletePolicyInternal(policyName, isInternalSyncDelete);
    }

    @Override
    public void suspendPolicy(String policyName) throws BeaconClientException {
        doEntityOperation(API.SUSPENDPOLICY, policyName);
    }

    @Override
    public void resumePolicy(String policyName) throws BeaconClientException {
        doEntityOperation(API.RESUMEPOLICY, policyName);
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
        syncEntity(API.SYNCPOLICY, policyName, policyDefinition);
    }

    @Override
    public void syncPolicyStatus(String policyName, String status,
                                      boolean isInternalStatusSync) throws BeaconClientException {
        syncStatus(policyName, status, isInternalStatusSync);
    }

    @Override
    public String getServiceStatus() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_STATUS.path).call(API.ADMIN_STATUS);
        ServerStatusResult response = getResponse(clientResponse, ServerStatusResult.class);
        return response.getStatus();
    }

    @Override
    public String getServiceVersion() throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_VERSION.path).call(API.ADMIN_VERSION);
        ServerStatusResult response = getResponse(clientResponse, ServerStatusResult.class);
        return response.getVersion();
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
        doEntityOperation(API.ABORT_POLICY_INSTANCE, policyName);
    }

    @Override
    public void updateCluster(String clusterName, String updateDefinition) throws BeaconClientException {
        syncEntity(API.UPDATE_CLUSTER, clusterName, updateDefinition);
    }

    @Override
    public void rerunPolicyInstance(String policyName) throws BeaconClientException {
        doEntityOperation(API.RERUN_POLICY_INSTANCE, policyName);
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
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(entities.mimeType);
            builder.type(MediaType.TEXT_PLAIN);
            if (authToken != null && authToken.isSet()) {
                builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
            }
            try {
                return builder.method(entities.method, ClientResponse.class);
            } catch (ClientHandlerException e) {
                throw new BeaconClientException(e, "Failed to connect to {}", service.getURI());
            }
        }

        public ClientResponse call(API operation, Object entityDefinition) {
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(operation.mimeType);
            builder.type(MediaType.APPLICATION_OCTET_STREAM_TYPE);
            if (authToken != null && authToken.isSet()) {
                builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
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
        ClientResponse clientResponse = new ResourceBuilder().path(API.PAIRCLUSTERS.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_PAIRING, Boolean.toString(isInternalPairing))
                .call(API.PAIRCLUSTERS);
        getResponse(clientResponse, APIResult.class);
    }

    private void unpair(String remoteClusterName,
                             boolean isInternalUnpairing) throws BeaconClientException {
        ClientResponse clientResponse = new ResourceBuilder().path(API.UNPAIRCLUSTERS.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_UNPAIRING, Boolean.toString(isInternalUnpairing))
                .call(API.UNPAIRCLUSTERS);
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
        ClientResponse clientResponse = new ResourceBuilder().path(API.SYNCPOLICYSTATUS.path, policyName)
                .addQueryParam(STATUS, status)
                .addQueryParam(IS_INTERNAL_STATUSSYNC, Boolean.toString(isInternalStatusSync))
                .call(API.SYNCPOLICYSTATUS);
        getResponse(clientResponse, APIResult.class);
    }

    private ClusterList getClusterList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                       Integer offset, Integer numResults) throws BeaconClientException {
        ClientResponse response = getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        ClusterList result = response.getEntity(ClusterList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private PolicyList getPolicyList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                     Integer offset, Integer numResults) throws BeaconClientException {
        ClientResponse response = getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        PolicyList result = response.getEntity(PolicyList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private ClientResponse getEntityListResponse(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                                 Integer offset, Integer numResults) throws BeaconClientException {
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
        ClientResponse clientResponse = new ResourceBuilder().path(API.DELETEPOLICY.path, entityName)
                .addQueryParam(IS_INTERNAL_DELETE, Boolean.toString(isInternalSyncDelete))
                .call(API.DELETEPOLICY);
        getResponse(clientResponse, APIResult.class);
    }

    private void setAuthToken(AuthenticatedURL.Token token) {
        this.authToken = token;
    }

    @Override
    public String toString() {
        return service.getURI().toString();
    }
}
