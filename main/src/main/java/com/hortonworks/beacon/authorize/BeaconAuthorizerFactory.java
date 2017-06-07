/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.authorize;

import org.apache.commons.lang.StringUtils;

import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.log.BeaconLog;

/** This class contains factory implementation for Different Authorizers.
 */

public final class BeaconAuthorizerFactory {
    private static final BeaconLog LOG = BeaconLog.getLog(BeaconAuthorizerFactory.class);
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
                        LOG.error("Error while creating authorizer of type '{}", authorizerClass, e);
                        throw new BeaconAuthorizationException("Error while creating authorizer of type '"
                            + authorizerClass + "'", e);
                    }
                    ret = instance;
                }
            }
        }
        return ret;
    }

}
