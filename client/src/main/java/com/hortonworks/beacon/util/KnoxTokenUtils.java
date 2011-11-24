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

package com.hortonworks.beacon.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.TreeMap;

/**
 * KnoxTokenUtils Class.
 */
public final class KnoxTokenUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KnoxTokenUtils.class);
    public static final String KNOX_GATEWAY_URL = "knox.gateway.url";
    private static final String KNOX_DEF_GATEWAY_PATH = "gateway";
    public static final String KNOX_PREAUTH_USER_HEADER = "BEACON_USER";
    public static final String KNOX_PREAUTH_USER = "beacon";
    public static final String KNOX_RREAUTH_TOKEN_API_PATH = "knoxtoken/api/v1/token";
    public static final String KNOX_TOKEN_ACCESS_TOKEN = "access_token";
    public static final String KNOX_TOKEN_EXPIRES_IN = "expires_in";
    private static final long KNOX_TOKEN_EXPIRY_THRESHOLD =
            BeaconConfig.getInstance().getEngine().getKnoxProxyTokenThreshold();

    private  static TreeMap<String, Pair<String, Long>> tokenMap =
            new TreeMap<String, Pair<String, Long>>();

    private KnoxTokenUtils() {
    }

    public static String getFixedKnoxURL(String knoxBaseURL) throws BeaconException {
        try {
            URI uri = new URI(knoxBaseURL);
            String path = uri.getPath();
            String url = knoxBaseURL;
            if (StringUtils.isBlank(path) || path.equals("/")) {
                if (!url.endsWith("/")) {
                    url += "/";
                }
                url = knoxBaseURL + KNOX_DEF_GATEWAY_PATH;
            }
            LOG.info("Knox gateway " + knoxBaseURL + " fixed to " + url);
            return url;
        } catch (URISyntaxException use) {
            throw new BeaconException("Invalid URL provided " + knoxBaseURL, use);
        }
    }

    public static String getKnoxProxiedURL(String knoxBaseURL, String service) {
        StringBuilder proxyURL = new StringBuilder(knoxBaseURL);
        proxyURL.append('/')
                .append(BeaconConfig.getInstance().getEngine().getKnoxProxyTopology())
                .append('/')
                .append(service);
        return proxyURL.toString();
    }

    public static String getKnoxSSOToken(String knoxBaseURL) throws BeaconException {
        return getKnoxSSOToken(knoxBaseURL, false);
    }

    public static String getKnoxSSOToken(String knoxBaseURL, boolean encode) throws BeaconException {

        Pair<String, Long> token = tokenMap.get(knoxBaseURL);

        if (token != null) {
            long expiry = token.getRight();
            long curTime = System.currentTimeMillis();
            long diff = expiry - curTime;

            if (diff >= (KNOX_TOKEN_EXPIRY_THRESHOLD * 1000L)) {
                LOG.debug("Returning cached token");
                String ssoToken = token.getLeft();
                if (encode) {
                    try {
                        ssoToken = URLEncoder.encode(ssoToken, "UTF-8");
                    } catch (IOException ioe) {
                        throw new BeaconException("Unable to encode token : " + ssoToken, ioe);
                    }
                }
                return ssoToken;
            }
        }

        BeaconConfig conf = BeaconConfig.getInstance();
        Engine engine = conf.getEngine();


        StringBuilder sb = new StringBuilder(knoxBaseURL)
                .append('/').append(engine.getKnoxPreAuthTopology())
                .append('/').append(KNOX_RREAUTH_TOKEN_API_PATH);
        String tokenEndpoint = sb.toString();
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                new HTTPSProperties(SSLUtils.HOSTNAME_VERIFIER, SSLUtils.getSSLContext())
        );
        clientConfig.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        Client client = Client.create(clientConfig);
        client.addFilter(new LoggingFilter(System.out));
        WebResource resource = client.resource(UriBuilder.fromUri(tokenEndpoint).build());

        WebResource.Builder builder = resource.accept(MediaType.APPLICATION_JSON)
                .header(KNOX_PREAUTH_USER_HEADER, KNOX_PREAUTH_USER);
        ClientResponse response = builder.method(HttpMethod.GET, ClientResponse.class);
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.error("Unable to get token from Knox: status : {}", response.getStatus());
            throw new BeaconException("Unable to get token from Knox -status " + response.getStatus());
        }

        String jsonStr = response.getEntity(String.class);

        LOG.debug("ClientResponse = {} ", jsonStr);
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonStr);
        JsonObject jsonObject = element.getAsJsonObject();
        String accessToken = jsonObject.get(KNOX_TOKEN_ACCESS_TOKEN).getAsString();
        long expiry = jsonObject.get(KNOX_TOKEN_EXPIRES_IN).getAsLong();
        tokenMap.put(knoxBaseURL, new ImmutablePair<String, Long>(accessToken, expiry));

        LOG.debug("Access token returned : {} ", accessToken);
        return accessToken;
    }

}
