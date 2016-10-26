package com.hortonworks.beacon.client;

import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.EntityList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class BeaconClient extends AbstractBeaconClient {

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<>(System.out);

    public static final String USER = System.getProperty("user.name");
    public static final String AUTH_URL = "api/options?" + PseudoAuthenticator.USER_NAME + "=" + USER;

    public static final String ORDER_BY = "orderBy";
    public static final String SORT_ORDER = "sortOrder";
    public static final String OFFSET = "offset";
    public static final String NUM_RESULTS = "numResults";
    private static final String FIELDS = "fields";

    /**
     * Name of the HTTP cookie used for the authentication token between the client and the server.
     */
    public static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";

    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();

    public static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession sslSession) {
            return true;
        }
    };
    private final WebResource service;
    private final AuthenticatedURL.Token authenticationToken;

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
    public BeaconClient(String beaconUrl) {
        this(beaconUrl, new Properties());
    }

    /**
     * Create a Beacon client instance.
     *
     * @param beaconUrl  of the server to which client interacts
     * @param properties client properties
     * @ - If unable to initialize SSL Props
     */
    public BeaconClient(String beaconUrl, Properties properties) {
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
            Client client = Client.create(config);
            client.setConnectTimeout(Integer.parseInt(clientProperties.getProperty("falcon.connect.timeout",
                    "180000")));
            client.setReadTimeout(Integer.parseInt(clientProperties.getProperty("falcon.read.timeout", "180000")));
            service = client.resource(UriBuilder.fromUri(baseUrl).build());
            client.resource(UriBuilder.fromUri(baseUrl).build());
            authenticationToken = getToken(baseUrl);
        } catch (Exception e) {
            throw new BeaconClientException("Unable to initialize Beacon Client object. Cause : " + e.getMessage(), e);
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

    /**
     * Set debug mode.
     *
     * @param debugMode : debugMode=false means no debugging. debugMode=true means debugging on
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public static AuthenticatedURL.Token getToken(String baseUrl) {
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();
        try {
            URL url = new URL(baseUrl + AUTH_URL);
            // using KerberosAuthenticator which falls back to PsuedoAuthenticator
            // instead of passing authentication type from the command line - bad factory
            HttpsURLConnection.setDefaultSSLSocketFactory(getSslContext().getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier(ALL_TRUSTING_HOSTNAME_VERIFIER);
            new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
        } catch (Exception ex) {
            throw new BeaconClientException("Could not authenticate, " + ex.getMessage(), ex);
        }

        return currentToken;
    }


    /**
     * Methods allowed on Entity Resources.
     */
    protected static enum Entities {
        SUBMITCLUSTER("api/beacon/cluster/submit/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SUBMITPOLICY("api/beacon/policy/submit/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SCHEDULEPOLICY("api/beacon/policy/schedule/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SUBMITANDSCHEDULEPOLICY("api/beacon/policy/submitAndSchedule/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LISTCLUSTER("api/beacon/cluster/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LISTPOLICY("api/beacon/policy/list/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        CLUSTERSTATUS("api/beacon/cluster/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        POLICYSTATUS("api/beacon/policy/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETCLUSTER("api/beacon/cluster/get/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        GETPOLICY("api/beacon/policy/get/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        DELETECLUSTER("api/beacon/cluster/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        DELETEPOLICY("api/beacon/policy/delete/", HttpMethod.DELETE, MediaType.APPLICATION_JSON),
        SUSPENDPOLICY("api/beacon/policy/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RESUMEPOLICY("api/beacon/policy/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON);

        private String path;
        private String method;
        private String mimeType;

        Entities(String path, String method, String mimeType) {
            this.path = path;
            this.method = method;
            this.mimeType = mimeType;
        }
    }

    public String notEmpty(String str, String name) {
        if (str == null) {

            throw new IllegalArgumentException(name + " cannot be null");
        }
        if (str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return str;
    }

    @Override
    public APIResult submitCluster(String clusterName, String filePath) {
        return submitEntity(Entities.SUBMITCLUSTER, clusterName, filePath);
    }

    @Override
    public APIResult submitReplicationPolicy(String policyName, String filePath) {
        return submitEntity(Entities.SUBMITPOLICY, policyName, filePath);
    }

    @Override
    public APIResult scheduleReplicationPolicy(String policyName) {
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SCHEDULEPOLICY.path, policyName)
                .call(Entities.SCHEDULEPOLICY);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public APIResult submitAndScheduleReplicationPolicy(String policyName, String filePath) {
        InputStream entityStream = getServletInputStream(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SUBMITANDSCHEDULEPOLICY.path, policyName)
                .call(Entities.SUBMITANDSCHEDULEPOLICY, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public EntityList getClusterList(String fields, String orderBy, String sortOrder, Integer offset, Integer numResults) {
        return getEntityList(Entities.LISTCLUSTER, fields, orderBy, sortOrder, offset, numResults);
    }

    @Override
    public EntityList getPolicyList(String fields, String orderBy, String sortOrder, Integer offset, Integer numResults) {
        return getEntityList(Entities.LISTPOLICY, fields, orderBy, sortOrder, offset, numResults);
    }

    @Override
    public APIResult getClusterStatus(String clusterName) {
        return getEntityStatus(Entities.CLUSTERSTATUS, clusterName);
    }

    @Override
    public APIResult getPolicyStatus(String policyName) {
        return getEntityStatus(Entities.POLICYSTATUS, policyName);
    }

    @Override
    public String getCluster(String clusterName) {
        return getEntity(Entities.GETCLUSTER, clusterName);
    }

    @Override
    public String getPolicy(String policyName) {
        return getEntity(Entities.GETPOLICY, policyName);
    }

    @Override
    public APIResult deleteCluster(String clusterName) {
        return doEntityOperation(Entities.DELETECLUSTER, clusterName);
    }

    @Override
    public APIResult deletePolicy(String policyName) {
        return doEntityOperation(Entities.DELETEPOLICY, policyName);
    }

    @Override
    public APIResult suspendPolicy(String policyName) {
        return doEntityOperation(Entities.SUSPENDPOLICY, policyName);
    }

    @Override
    public APIResult resumePolicy(String policyName) {
        return doEntityOperation(Entities.RESUMEPOLICY, policyName);
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     */
    private InputStream getServletInputStream(String filePath) {

        if (filePath == null) {
            return null;
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new BeaconClientException("File not found:", e);
        }
        return stream;
    }

    private <T> T getResponse(Class<T> clazz, ClientResponse clientResponse) {
        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(clazz);
    }

    private class ResourceBuilder {
        WebResource resource;

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

        private ClientResponse call(Entities entities) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(entities.mimeType).type(MediaType.TEXT_PLAIN)
                    .method(entities.method, ClientResponse.class);
        }

        public ClientResponse call(Entities operation, InputStream entityStream) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(operation.mimeType).type(MediaType.TEXT_PLAIN)
                    .method(operation.method, ClientResponse.class, entityStream);
        }
    }

    private void checkIfSuccessful(ClientResponse clientResponse) {
        Response.Status.Family statusFamily = clientResponse.getClientResponseStatus().getFamily();
        if (statusFamily != Response.Status.Family.SUCCESSFUL && statusFamily != Response.Status.Family.INFORMATIONAL) {
            throw BeaconClientException.fromReponse(clientResponse);
        }
    }

    private void printClientResponse(ClientResponse clientResponse) {
        if (getDebugMode()) {
            OUT.get().println(clientResponse.toString());
        }
    }

    private APIResult submitEntity(Entities operation, String entityName, String filePath) {
        InputStream entityStream = getServletInputStream(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    private EntityList getEntityList(Entities operation, String fields, String orderBy, String sortOrder,
                                     Integer offset, Integer numResults) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path)
                .addQueryParam(NUM_RESULTS, numResults)
                .addQueryParam(OFFSET, offset).addQueryParam(SORT_ORDER, sortOrder)
                .addQueryParam(ORDER_BY, orderBy).addQueryParam(FIELDS, fields)
                .call(operation);

        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);

        EntityList result = clientResponse.getEntity(EntityList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    private APIResult getEntityStatus(Entities operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(entityName)
                .call(operation);

        return getResponse(APIResult.class, clientResponse);
    }

    private String getEntity(Entities operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        return getResponse(String.class, clientResponse);
    }

    private APIResult doEntityOperation(Entities operation, String entityName) {
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityName)
                .call(operation);
        return getResponse(APIResult.class, clientResponse);
    }
}
