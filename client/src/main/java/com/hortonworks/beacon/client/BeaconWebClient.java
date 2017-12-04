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

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.StatusResult;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.hadoop.hdfs.web.KerberosUgiAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client API to submit and manage Beacon resources (Cluster, Policies).
 */
public class BeaconWebClient implements BeaconClient {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconWebClient.class);
    private static final String IS_INTERNAL_PAIRING = "isInternalPairing";
    private static final String IS_INTERNAL_DELETE = "isInternalSyncDelete";
    private static final String IS_INTERNAL_STATUSSYNC = "isInternalStatusSync";

    private static final String IS_INTERNAL_UNPAIRING = "isInternalUnpairing";
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<>(System.out);

    public static final String ORDER_BY = "orderBy";
    public static final String SORT_ORDER = "sortOrder";
    public static final String OFFSET = "offset";
    public static final String NUM_RESULTS = "numResults";
    private static final String FIELDS = "fields";

    private static final String PARAM_FILTERBY = "filterBy";

    public static final String REMOTE_BEACON_ENDPOINT = "remoteBeaconEndpoint";
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
    public BeaconWebClient(String beaconUrl) {
        this(beaconUrl, new Properties());
    }

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl  of the server to which client interacts
     * @param properties client properties
     * @ - If unable to initialize SSL Props
     */
    public BeaconWebClient(String beaconUrl, Properties properties) {
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
        SUBMITCLUSTER("api/beacon/cluster/submit/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SUBMITANDSCHEDULEPOLICY("api/beacon/policy/submitAndSchedule/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LISTCLUSTER("api/beacon/cluster/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LISTPOLICY("api/beacon/policy/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTERSTATUS("api/beacon/cluster/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICYSTATUS("api/beacon/policy/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETCLUSTER("api/beacon/cluster/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETPOLICY("api/beacon/policy/getEntity/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        DELETECLUSTER("api/beacon/cluster/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        DELETEPOLICY("api/beacon/policy/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        SUSPENDPOLICY("api/beacon/policy/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RESUMEPOLICY("api/beacon/policy/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        PAIRCLUSTERS("api/beacon/cluster/pair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        UNPAIRCLUSTERS("api/beacon/cluster/unpair/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SYNCPOLICY("api/beacon/policy/sync/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SYNCPOLICYSTATUS("api/beacon/policy/syncStatus/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        POLICY_INSTANCE_LIST(API_PREFIX + "instance/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        ABORT_POLICY_INSTANCE("api/beacon/policy/instance/abort/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        UPDATE_CLUSTER("api/beacon/cluster/", HttpMethod.PUT, MediaType.APPLICATION_JSON),
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
    public APIResult submitCluster(String clusterName, String filePath) {
        return submitEntity(API.SUBMITCLUSTER, clusterName, filePath);
    }

    @Override
    public APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath) {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(API.SUBMITANDSCHEDULEPOLICY.path, policyName)
                .call(API.SUBMITANDSCHEDULEPOLICY, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public ClusterList getClusterList(String fields, String orderBy, String sortOrder,
                                      Integer offset, Integer numResults) {
        return getClusterList(API.LISTCLUSTER, fields, orderBy, null, sortOrder, offset, numResults);
    }

    @Override
    public PolicyList getPolicyList(String fields, String orderBy, String filterBy, String sortOrder,
                                    Integer offset, Integer numResults) {
        return getPolicyList(API.LISTPOLICY, fields, orderBy, filterBy, sortOrder, offset, numResults);
    }

    @Override
    public StatusResult getClusterStatus(String clusterName) {
        return getEntityStatus(API.CLUSTERSTATUS, clusterName);
    }

    @Override
    public StatusResult getPolicyStatus(String policyName) {
        return getEntityStatus(API.POLICYSTATUS, policyName);
    }

    @Override
    public String getCluster(String clusterName) {
        return getEntity(API.GETCLUSTER, clusterName);
    }

    @Override
    public String getPolicy(String policyName) {
        return getEntity(API.GETPOLICY, policyName);
    }

    @Override
    public APIResult deleteCluster(String clusterName) {
        return doEntityOperation(API.DELETECLUSTER, clusterName);
    }

    @Override
    public APIResult deletePolicy(String policyName, boolean isInternalSyncDelete) {
        return policyDelete(policyName, isInternalSyncDelete);
    }

    @Override
    public APIResult suspendPolicy(String policyName) {
        return doEntityOperation(API.SUSPENDPOLICY, policyName);
    }

    @Override
    public APIResult resumePolicy(String policyName) {
        return doEntityOperation(API.RESUMEPOLICY, policyName);
    }

    @Override
    public APIResult pairClusters(String remoteClusterName, boolean isInternalPairing) {
        return pair(remoteClusterName, isInternalPairing);
    }

    @Override
    public APIResult unpairClusters(String remoteClusterName,
                                    boolean isInternalUnpairing) {
        return unpair(remoteClusterName, isInternalUnpairing);
    }

    @Override
    public APIResult syncPolicy(String policyName, String policyDefinition) {
        return syncEntity(API.SYNCPOLICY, policyName, policyDefinition);
    }

    @Override
    public APIResult syncPolicyStatus(String policyName, String status,
                                      boolean isInternalStatusSync) {
        return syncStatus(policyName, status, isInternalStatusSync);
    }

    @Override
    public String getStatus() {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_STATUS.path).call(API.ADMIN_STATUS);
        ServerStatusResult response = getResponse(ServerStatusResult.class, clientResponse);
        return response.getStatus();
    }

    @Override
    public String getVersion() {
        ClientResponse clientResponse = new ResourceBuilder().path(API.ADMIN_VERSION.path).call(API.ADMIN_VERSION);
        ServerStatusResult response = getResponse(ServerStatusResult.class, clientResponse);
        return response.getVersion();
    }

    @Override
    public PolicyInstanceList listPolicyInstances(String policyName) {
        ClientResponse clientResponse = new ResourceBuilder().path(API.POLICY_INSTANCE_LIST.path)
                .addQueryParam(PARAM_FILTERBY, "name:" + policyName)
                .call(API.POLICY_INSTANCE_LIST);
        return getResponse(PolicyInstanceList.class, clientResponse);
    }

    @Override
    public APIResult abortPolicyInstance(String policyName) {
        return doEntityOperation(API.ABORT_POLICY_INSTANCE, policyName);
    }

    @Override
    public APIResult updateCluster(String clusterName, String updateDefinition) {
        return syncEntity(API.UPDATE_CLUSTER, clusterName, updateDefinition);
    }

    @Override
    public APIResult rerunPolicyInstance(String policyName) {
        return doEntityOperation(API.RERUN_POLICY_INSTANCE, policyName);
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     */
    private InputStream getServletInputStreamFromFile(String filePath) {

        if (filePath == null) {
            return null;
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new BeaconClientException(e, "File not found.");
        }
        return stream;
    }

    private <T> T getResponse(Class<T> clazz, ClientResponse clientResponse) {
        printClientResponse(clientResponse);
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

        private ClientResponse call(API entities) {
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(entities.mimeType);
            builder.type(MediaType.TEXT_PLAIN);
            if (authToken != null && authToken.isSet()) {
                builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
            }
            return builder.method(entities.method, ClientResponse.class);
        }

        public ClientResponse call(API operation, InputStream entityStream)  {
            setAuthToken(getToken(service.getURI().toString()));
            WebResource.Builder builder = resource.accept(operation.mimeType);
            builder.type(MediaType.APPLICATION_OCTET_STREAM_TYPE);
            if (authToken != null && authToken.isSet()) {
                builder.cookie(new Cookie(AuthenticatedURL.AUTH_COOKIE, authToken.toString()));
            }
            return builder.method(operation.method, ClientResponse.class, entityStream);
        }

        public ClientResponse call(API operation, String entityDefinition) {
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

    private void checkIfSuccessful(ClientResponse clientResponse) {
        Response.Status.Family statusFamily = clientResponse.getClientResponseStatus().getFamily();
        if (statusFamily != Response.Status.Family.SUCCESSFUL && statusFamily != Response.Status.Family.INFORMATIONAL) {
            throw BeaconClientException.fromResponse(clientResponse);
        }
    }

    private void printClientResponse(ClientResponse clientResponse) {
        if (getDebugMode()) {
            OUT.get().println(clientResponse.toString());
        }
    }

    private APIResult submitEntity(API operation, String entityName, String filePath) {
        InputStream entityStream = getServletInputStreamFromFile(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    private APIResult pair(String remoteClusterName,
                           boolean isInternalPairing) {
        ClientResponse clientResponse = new ResourceBuilder().path(API.PAIRCLUSTERS.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_PAIRING, Boolean.toString(isInternalPairing))
                .call(API.PAIRCLUSTERS);
        return getResponse(APIResult.class, clientResponse);
    }

    private APIResult unpair(String remoteClusterName,
                             boolean isInternalUnpairing) {
        ClientResponse clientResponse = new ResourceBuilder().path(API.UNPAIRCLUSTERS.path)
                .addQueryParam(REMOTE_CLUSTERNAME, remoteClusterName)
                .addQueryParam(IS_INTERNAL_UNPAIRING, Boolean.toString(isInternalUnpairing))
                .call(API.UNPAIRCLUSTERS);
        return getResponse(APIResult.class, clientResponse);
    }

    private APIResult syncEntity(API operation, String entityName, String entityDefinition) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation, entityDefinition);
        return getResponse(clientResponse);
    }

    private APIResult getResponse(ClientResponse clientResponse) {
        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return new APIResult(clientResponse.getStatus() == 200 ? APIResult.Status.SUCCEEDED : APIResult.Status.FAILED,
            clientResponse.getEntity(String.class));
    }

    private APIResult syncStatus(String policyName, String status,
                                 boolean isInternalStatusSync) {
        ClientResponse clientResponse = new ResourceBuilder().path(API.SYNCPOLICYSTATUS.path, policyName)
                .addQueryParam(STATUS, status)
                .addQueryParam(IS_INTERNAL_STATUSSYNC, Boolean.toString(isInternalStatusSync))
                .call(API.SYNCPOLICYSTATUS);
        return getResponse(APIResult.class, clientResponse);
    }

    private ClusterList getClusterList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                       Integer offset, Integer numResults) {
        ClientResponse response = getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        ClusterList result = response.getEntity(ClusterList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private PolicyList getPolicyList(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                     Integer offset, Integer numResults) {


        ClientResponse response = getEntityListResponse(operation, fields, orderBy, filterBy, sortOrder, offset, numResults);
        PolicyList result = response.getEntity(PolicyList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private ClientResponse getEntityListResponse(API operation, String fields, String orderBy, String filterBy, String sortOrder,
                                                 Integer offset, Integer numResults) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path)
                .addQueryParam(NUM_RESULTS, numResults)
                .addQueryParam(PARAM_FILTERBY, filterBy)
                .addQueryParam(OFFSET, offset).addQueryParam(SORT_ORDER, sortOrder)
                .addQueryParam(ORDER_BY, orderBy).addQueryParam(FIELDS, fields)
                .call(operation);

        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse;
    }

    private StatusResult getEntityStatus(API operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(entityName)
                .call(operation);

        return getResponse(StatusResult.class, clientResponse);
    }

    private String getEntity(API operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        return getResponse(String.class, clientResponse);
    }

    private APIResult doEntityOperation(API operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        return getResponse(APIResult.class, clientResponse);
    }

    private APIResult policyDelete(String entityName,
                                   boolean isInternalSyncDelete) {
        ClientResponse clientResponse = new ResourceBuilder().path(API.DELETEPOLICY.path, entityName)
                .addQueryParam(IS_INTERNAL_DELETE, Boolean.toString(isInternalSyncDelete))
                .call(API.DELETEPOLICY);
        return getResponse(APIResult.class, clientResponse);
    }

    private void setAuthToken(AuthenticatedURL.Token token) {
        this.authToken = token;
    }

    @Override
    public String toString() {
        return service.getURI().toString();
    }
}
