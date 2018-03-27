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
                        LOG.debug("Initializing Authorizer :: {}", authorizerClass);
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
