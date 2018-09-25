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
package com.hortonworks.beacon.plugin.atlas;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.SSLUtils;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.apache.atlas.AtlasClientV2;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.Cookie;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

/**
 * BeaconAtlasClient overides getClient method of AtlasClientV2 for SSL.
 */
public class BeaconAtlasClient extends AtlasClientV2 {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconAtlasClient.class);

    public BeaconAtlasClient(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    public BeaconAtlasClient(String[] baseUrl, Cookie cookie) {
        super(baseUrl, cookie);
    }

    public BeaconAtlasClient(Configuration configuration, String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(configuration, baseUrl, basicAuthUserNamePassword);
    }

    @Override
    protected Client getClient(Configuration configuration, UserGroupInformation ugi, String doAsUser) {
        if (!configuration.getBoolean(TLS_ENABLED)) {
            return super.getClient(configuration, ugi, doAsUser);
        }

        SSLContext sslContext = getSslContext();
        if (sslContext == null) {
            return super.getClient(configuration, ugi, doAsUser);
        }

        LOG.info("BeaconAtlasPlugin: BeaconAtlasClient: initializing client using SSLContext...");
        DefaultClientConfig config = new DefaultClientConfig();
        config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        config.getClasses().add(MultiPartWriter.class);
        config.getClasses().add(JacksonJaxbJsonProvider.class);

        config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                new HTTPSProperties(null, sslContext));
        return Client.create(config);
    }

    private SSLContext getSslContext() {
        try {
            return SSLUtils.getSSLContext();
        } catch (BeaconException e) {
            return null;
        }
    }
}
