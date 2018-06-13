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

package com.hortonworks.beacon.authorize.simple;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.authorize.BeaconAccessRequest;
import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconAuthorizationException;
import com.hortonworks.beacon.authorize.BeaconAuthorizer;
import com.hortonworks.beacon.authorize.BeaconAuthorizerFactory;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;

/**
 * This class contains Authorization utility.
 */

public final class BeaconAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconAuthorizationUtils.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    public static final String BASE_API = "api/beacon/";
    private static final String BASE_URL = "/" + BASE_API;

    private BeaconAuthorizationUtils(){
    }
    public static String getApi(String contextPath) {
        if (isDebugEnabled) {
            LOG.debug("==> getApi({})", contextPath);
        }
        if (contextPath.startsWith(BASE_URL)) {
            contextPath = contextPath.substring(BASE_URL.length());
        } else {
            // strip of leading '/'
            if (contextPath.startsWith("/")) {
                contextPath = contextPath.substring(1);
            }
        }
        String[] split = contextPath.split("/", 3);

        String api = split[0];
        if (Pattern.matches("v\\d", api)) {
            api = split[1];
        }

        if (isDebugEnabled) {
            LOG.debug("<== getApi({}): {}", contextPath, api);
        }

        return api;
    }

    public static BeaconActionTypes getBeaconAction(String method) {
        BeaconActionTypes action = null;

        switch (method.toUpperCase()) {
            case "POST":
                action = BeaconActionTypes.CREATE;
                break;
            case "GET":
                action = BeaconActionTypes.READ;
                break;
            case "PUT":
                action = BeaconActionTypes.UPDATE;
                break;
            case "DELETE":
                action = BeaconActionTypes.DELETE;
                break;
            default:
                if (isDebugEnabled) {
                    LOG.debug("getBeaconAction(): Invalid HTTP method '{}'", method);
                }
                break;
        }

        if (isDebugEnabled) {
            LOG.debug("<== BeaconAuthorizationFilter getBeaconAction HTTP Method {} mapped to BeaconAction : {}",
                    method, action);
        }
        return action;
    }

    public static Set<BeaconResourceTypes> getBeaconResourceType(String contextPath) {
        Set<BeaconResourceTypes> resourceTypes = new HashSet<>();
        if (isDebugEnabled) {
            LOG.debug("==> getBeaconResourceType  for {}", contextPath);
        }
        String api = getApi(contextPath);
        if ((api.startsWith("cluster"))) {
            resourceTypes.add(BeaconResourceTypes.CLUSTER);
        } else if (api.startsWith("policy/schedule")) {
            resourceTypes.add(BeaconResourceTypes.SCHEDULE);
        } else if (api.startsWith("policy")) {
            resourceTypes.add(BeaconResourceTypes.POLICY);
        } else if (api.startsWith("events")) {
            resourceTypes.add(BeaconResourceTypes.EVENT);
        } else if (api.startsWith("logs")) {
            resourceTypes.add(BeaconResourceTypes.LOGS);
        }else {
            LOG.error("Unable to find Beacon Resource corresponding to : {}\nSetting {}", api,
                BeaconResourceTypes.UNKNOWN.name());
            resourceTypes.add(BeaconResourceTypes.UNKNOWN);
        }

        if (isDebugEnabled) {
            LOG.debug("<== Returning BeaconResources {} for api {}", resourceTypes, api);
        }
        return resourceTypes;
    }

    public static boolean isAccessAllowed(BeaconResourceTypes resourcetype, BeaconActionTypes actionType,
            String userName, Set<String> groups, HttpServletRequest request) {
        BeaconAuthorizer authorizer = null;
        boolean isaccessAllowed = false;

        Set<BeaconResourceTypes> resourceTypes = new HashSet<>();
        resourceTypes.add(resourcetype);
        BeaconAccessRequest beaconRequest = new BeaconAccessRequest(resourceTypes, "*", actionType, userName, groups,
                BeaconAuthorizationUtils.getRequestIpAddress(request));
        try {
            authorizer = BeaconAuthorizerFactory.getBeaconAuthorizer();
            if (authorizer != null) {
                isaccessAllowed = authorizer.isAccessAllowed(beaconRequest);
            }
        } catch (BeaconAuthorizationException e) {
            LOG.error("Unable to obtain BeaconAuthorizer.", e);
        }

        return isaccessAllowed;
    }

    public static String getRequestIpAddress(HttpServletRequest httpServletRequest) {
        try {
            InetAddress inetAddr = InetAddress.getByName(httpServletRequest.getRemoteAddr());
            return inetAddr.getHostAddress();
        } catch (UnknownHostException ex) {
            LOG.error("Error occured when retrieving IP address", ex);
            return "";
        }
    }
}
