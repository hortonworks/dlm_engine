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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Cookie;
import java.io.IOException;

/**
 * MockClientBuilder for AtlasRESTClient.
 */
public class RESTClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(RESTClientBuilder.class);

    private static final String KNOX_HDP_COOKIE_NAME = "hadoop-jwt";

    private static final String BEACON_KERBEROS_AUTH_ENABLED = "beacon.kerberos.authentication.enabled";
    private static final String BEACON_AUTH_TYPE = "beacon.kerberos.authentication.type";
    private static final String BEACON_USER_PRINCIPAL = "beacon.kerberos.principal";
    private static final String BEACON_USER_KEYTAB = "beacon.kerberos.keytab";

    private static final String ATLAS_CLIENT_DEFAULT_USER = "admin";
    private static final String ATLAS_CLIENT_DEFAULT_PASSWORD = "admin";
    private static final String ATLAS_CLIENT_USER_NAME_KEY = "atlas.client.user.name";
    private static final String ATLAS_CLIENT_USER_PASSWORD_KEY = "atlas.client.user.password";
    private static final String URL_SEPERATOR = ",";

    private AuthStrategy authStrategy;
    private String userId;
    private String password;
    private UserGroupInformation userGroupInformation;
    private Cookie hdpCookie;
    private String knoxBaseUrl;

    protected String incomingUrl;
    protected String[] baseUrls;

    enum AuthStrategy {
        USER_NAME_PASSWORD,
        KERBEROS,
        KNOX
    }

    public RESTClientBuilder() {
    }

    public RESTClientBuilder baseUrl(String urls) {
        this.incomingUrl = urls;
        if (urls.contains(URL_SEPERATOR)) {
            this.baseUrls = StringUtils.split(urls, URL_SEPERATOR);
        } else {
            this.baseUrls = new String[]{urls};
        }

        return this;
    }

    public RESTClientBuilder setAuthStrategy() throws BeaconException {
        return inferKnoxAuthStrategy()
                .inferKerberosAuthStrategy()
                .inferUserNamePasswordAuthStrategy();
    }

    private RESTClientBuilder inferUserNamePasswordAuthStrategy() {
        if (authStrategy != null) {
            return this;
        }

        authStrategy = AuthStrategy.USER_NAME_PASSWORD;
        LOG.info("BeaconAtlasPlugin: authStrategy: {} : {}", authStrategy, baseUrls);
        return this;
    }

    private RESTClientBuilder inferKerberosAuthStrategy() throws BeaconException {
        if (authStrategy != null) {
            return this;
        }

        String hostName = BeaconConfig.getInstance().getEngine().getHostName();
        if (!BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()
                && KerberosAuthEnabledChecker.isEnabled(PropertiesUtil.getInstance(), hostName)) {
            try {
                authStrategy = AuthStrategy.KERBEROS;
                this.userGroupInformation = UserGroupInformation.getLoginUser();
                LOG.info("BeaconAtlasPlugin: authStrategy: {} : urls: {}: userGroupInformation: {}",
                        authStrategy, baseUrls, userGroupInformation);
            } catch (Exception e) {
                throw new BeaconException("Error: setAuthStrategy: UserGroupInformation.getLoginUser: failed!", e);
            }
        }

        return this;
    }

    private RESTClientBuilder inferKnoxAuthStrategy() {
        if (authStrategy != null) {
            return this;
        }

        if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            authStrategy = AuthStrategy.KNOX;
            LOG.info("BeaconAtlasPlugin: authStrategy: {}", authStrategy);
        }

        return this;
    }

    public AuthStrategy getAuthStrategy() {
        return authStrategy;
    }

    public RESTClientBuilder userIdPwd(String uid, String pwd) {
        this.userId = uid;
        this.password = pwd;

        return this;
    }

    RESTClientBuilder knoxBaseUrl(String url) {
        this.knoxBaseUrl = url;
        return this;
    }

    private String getSSOToken(String knoxBaseURL) throws BeaconException {
        if (!BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            return null;
        }

        try {
            return KnoxTokenUtils.getKnoxSSOToken(knoxBaseURL);
        } catch (Exception e) {
            LOG.error("Unable to get knox sso token from {} : {} . Cause: {}", knoxBaseURL, e.getMessage(), e);
            throw new BeaconException(e);
        }
    }

    public RESTClient create() throws BeaconException {
        try {
            if (ArrayUtils.isEmpty(baseUrls)) {
                throw new BeaconException("baseUrls is not set.");
            }

            setAuthStrategy();
            AtlasClientV2 clientV2;
            switch (authStrategy) {
                case KNOX:
                    this.hdpCookie = new Cookie(KNOX_HDP_COOKIE_NAME, getSSOToken(knoxBaseUrl));
                    LOG.info("BeaconAtlasPlugin: authStrategy: {} : knox hdpCookie: {}", authStrategy, hdpCookie);
                    clientV2 = new AtlasClientV2(baseUrls, this.hdpCookie);
                    return new AtlasRESTClient(clientV2);

                case KERBEROS:
                    clientV2 = new AtlasClientV2(this.userGroupInformation,
                            this.userGroupInformation.getShortUserName(), baseUrls);

                    return new AtlasRESTClient(clientV2);

                case USER_NAME_PASSWORD:
                    Configuration conf = getDefaultConfiguration();
                    if (conf == null) {
                        throw new BeaconException("Error initializing: " + AtlasPluginInfo.PLUGIN_NAME);
                    }

                    userIdPwd(getUserName(), getPassword());
                    clientV2 = new AtlasClientV2(conf, baseUrls, new String[]{userId, password});
                    return new AtlasRESTClient(clientV2);

                default:
                    throw new BeaconException("Unsupported auth strategy!");
            }
        } catch (Exception ex) {
            LOG.error("Unable to create RESTClient: {}", authStrategy, ex);
            throw new AtlasRestClientException("AtlasRESTClient: Error fetching configuration", ex);
        }
    }

    private String getPassword() throws AtlasException {
        return getDefaultConfiguration().getString(ATLAS_CLIENT_USER_NAME_KEY, ATLAS_CLIENT_DEFAULT_USER);
    }

    private String getUserName() throws AtlasException {
        return getDefaultConfiguration().getString(ATLAS_CLIENT_USER_PASSWORD_KEY, ATLAS_CLIENT_DEFAULT_PASSWORD);
    }


    private Configuration getDefaultConfiguration() throws AtlasException {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            LOG.error("AtlasRESTClient: Error fetching configuration!", e);
            throw e;
        }
    }

    @VisibleForTesting
    static class KerberosAuthEnabledChecker {
        private static final String KERBEROS_TYPE = "kerberos";

        public static boolean isEnabled(PropertiesUtil propertiesUtil, String hostName) throws BeaconException {
            if (fromAuthKey(propertiesUtil) || fromAuthTypeKey(propertiesUtil)) {
                return true;
            }

            return fromPrincipal(propertiesUtil, hostName);
        }

        private static boolean fromPrincipal(PropertiesUtil propertiesUtil, String hostName) throws BeaconException {
            String beaconPrincipal = propertiesUtil.getProperty(BEACON_USER_PRINCIPAL);
            String keytab = propertiesUtil.getProperty(BEACON_USER_KEYTAB);
            String principal = getPrincipalFromSecureLogin(beaconPrincipal, hostName);

            return StringUtils.isNotEmpty(hostName)
                    && StringUtils.isNotEmpty(principal)
                    && StringUtils.isNotEmpty(keytab);
        }

        private static boolean fromAuthTypeKey(PropertiesUtil propertiesUtil) {
            return KERBEROS_TYPE.equalsIgnoreCase(propertiesUtil.getProperty(BEACON_AUTH_TYPE));
        }

        private static boolean fromAuthKey(PropertiesUtil propertiesUtil) {
            return propertiesUtil.getBooleanProperty(BEACON_KERBEROS_AUTH_ENABLED, false);
        }

        private static String getPrincipalFromSecureLogin(String beaconPrincipal,
                                                          String hostName) throws BeaconException {
            try {
                return SecureClientLogin.getPrincipal(beaconPrincipal, hostName);
            } catch (IOException e) {
                LOG.error("Unable to read: Principal: {}: Host: {}", beaconPrincipal, hostName, e);
                throw new BeaconException(e);
            }
        }
    }
}
