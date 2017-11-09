/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.authorize;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.config.PropertiesUtil;

/** This class contains factory implementation for Different Authorizers.
 */

public final class BeaconAuthorizerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconAuthorizerFactory.class);
    private static final String SIMPLE_AUTHORIZER = "com.hortonworks.beacon.authorize.simple.SimpleBeaconAuthorizer";
    private static final String RANGER_AUTHORIZER =
        "org.apache.ranger.authorization.beacon.authorizer.RangerBeaconAuthorizer";
    private static final String BEACON_AUTHORIZER_TYPE= "beacon.authorizer.impl";
    private static volatile BeaconAuthorizer instance = null;
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();

    private BeaconAuthorizerFactory(){
    }
    public static BeaconAuthorizer getBeaconAuthorizer() throws BeaconAuthorizationException {
        BeaconAuthorizer ret = instance;

        if (ret == null) {
            synchronized (BeaconAuthorizerFactory.class) {
                if (instance == null) {
                    String authorizerClass = AUTHCONFIG.getProperty(BEACON_AUTHORIZER_TYPE, "SIMPLE");
                    if (StringUtils.isNotEmpty(authorizerClass)) {
                        if (StringUtils.equalsIgnoreCase(authorizerClass, "SIMPLE")) {
                            authorizerClass = SIMPLE_AUTHORIZER;
                        } else if (StringUtils.equalsIgnoreCase(authorizerClass, "RANGER")) {
                            authorizerClass = RANGER_AUTHORIZER;
                        }
                    } else {
                        authorizerClass = SIMPLE_AUTHORIZER;
                    }

                    if (isDebugEnabled) {
                        LOG.debug("Initializing Authorizer :: {0}", authorizerClass);
                    }
                    try {
                        Class authorizerMetaObject = Class.forName(authorizerClass);
                        if (authorizerMetaObject != null) {
                            instance = (BeaconAuthorizer) authorizerMetaObject.newInstance();
                        }
                    } catch (Exception e) {
                        LOG.error("Error while creating authorizer of type '{}'", authorizerClass, e);
                        throw new BeaconAuthorizationException(
                            "Error while creating authorizer of type '{}'", e, authorizerClass);
                    }
                    ret = instance;
                }
            }
        }
        return ret;
    }

}
