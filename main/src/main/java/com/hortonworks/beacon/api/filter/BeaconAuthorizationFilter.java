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

package com.hortonworks.beacon.api.filter;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.hortonworks.beacon.authorize.BeaconAccessRequest;
import com.hortonworks.beacon.authorize.BeaconAuthorizationException;
import com.hortonworks.beacon.authorize.BeaconAuthorizer;
import com.hortonworks.beacon.authorize.BeaconAuthorizerFactory;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;
import com.hortonworks.beacon.config.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * This enforces simple authorization on resources if user is authenticated.
 */

public class BeaconAuthorizationFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconAuthorizationFilter.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private BeaconAuthorizer authorizer = null;

    private static final String BASE_URL = "/" + PropertiesUtil.BASE_API;
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    private static final String BEACON_AUTHORIZATION_ENABLED="beacon.authorization.enabled";
    private String coreSiteFile;
    private String hdfsSiteFile;

    public BeaconAuthorizationFilter() {
        try {
            authorizer = BeaconAuthorizerFactory.getBeaconAuthorizer();
            if (authorizer != null) {
                authorizer.init();
            } else {
                LOG.warn("BeaconAuthorizer not initialized properly, please check the application logs"
                                + " and add proper configurations.");
            }
        } catch (BeaconAuthorizationException e) {
            LOG.error("Unable to obtain BeaconAuthorizer.", e);
        }

    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
        coreSiteFile=AUTHCONFIG.getResourceFileName("core-site.xml");
        hdfsSiteFile=AUTHCONFIG.getResourceFileName("hdfs-site.xml");
    }

    @Override
    public void destroy() {
        if (authorizer != null) {
            authorizer.cleanUp();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        boolean isAuthorization = AUTHCONFIG.getBooleanProperty(BEACON_AUTHORIZATION_ENABLED, false);
        if (!isAuthorization) {
            chain.doFilter(req, res);
            return;
        }
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) res;
        String pathInfo = request.getRequestURI();
        Configuration conf = new Configuration();
        if (!Strings.isNullOrEmpty(pathInfo) && pathInfo.startsWith(BASE_URL)) {
            if (isDebugEnabled) {
                LOG.debug("{} is a valid REST API request!!!", pathInfo);
            }
            String userName = null;
            Set<String> groups = new HashSet<>();
            HttpSession session = request.getSession();
            if (session != null) {
                if (session.getAttribute("username") != null) {
                    userName=(String) session.getAttribute("username");
                    LOG.debug("userName: {}", userName);
                    LOG.debug("CORE_SITE_FILE: {}", coreSiteFile);
                    LOG.debug("HDFS_SITE_FILE: {}", hdfsSiteFile);
                    if (!StringUtils.isEmpty(userName)) {
                        if (!StringUtils.isEmpty(coreSiteFile) && !StringUtils.isEmpty(hdfsSiteFile)) {
                            conf.addResource(new Path(coreSiteFile));
                            conf.addResource(new Path(hdfsSiteFile));
                            Groups groupObj=new Groups(conf);
                            List<String> userGroups=null;
                            try{
                                userGroups=groupObj.getGroups(userName);
                            } catch(Exception ex) {
                                LOG.error("No groups found for user: {}", userName, ex.getMessage());
                            }
                            if (userGroups!=null) {
                                for (String groupNames : userGroups) {
                                    LOG.debug("groupNames: {}", groupNames);
                                    groups.add(groupNames);
                                }
                            }
                        }
                    }
                }
            }
            BeaconAccessRequest beaconRequest = new BeaconAccessRequest(request, userName, groups);
            if (isDebugEnabled) {
                LOG.debug(
                        "============================"
                        + "\nUserName :: {}\nGroups :: {}\nURL :: {}\nAction :: {}\nrequest.getServletPath() ::"
                        + " {}\n============================\n",
                        beaconRequest.getUser(), beaconRequest.getUserGroups(), request.getRequestURL(),
                        beaconRequest.getAction(), pathInfo);
            }

            boolean accessAllowed = false;

            Set<BeaconResourceTypes> beaconResourceTypes = beaconRequest.getResourceTypes();
            if (beaconResourceTypes.size() == 1 && beaconResourceTypes.contains(BeaconResourceTypes.UNKNOWN)) {
                // Allowing access to unprotected resource types
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Allowing access to unprotected resource types {}", beaconResourceTypes);
                }
                accessAllowed = true;
            } else {
                try {
                    if (authorizer != null) {
                        accessAllowed = authorizer.isAccessAllowed(beaconRequest);
                    }
                } catch (BeaconAuthorizationException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Access Restricted. Could not process the request :: {}", e);
                    }
                }
                if (isDebugEnabled) {
                    LOG.debug("Authorizer result :: {}", accessAllowed);
                }
            }

            if (accessAllowed) {
                if (isDebugEnabled) {
                    LOG.debug("Access is allowed so forwarding the request!!!");
                }
                chain.doFilter(req, res);
            } else {
                JsonObject json = new JsonObject();
                json.addProperty("AuthorizationError", "You are not authorized for " + beaconRequest.getAction().name()
                        + " on " + beaconResourceTypes + " : " + beaconRequest.getResource());

                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.sendError(HttpServletResponse.SC_FORBIDDEN, json.toString());
                if (isDebugEnabled) {
                    LOG.debug(
                            "You are not authorized for {0} on {1} : {2}\n"
                                    + "Returning 403 since the access is blocked update!!!!",
                            beaconRequest.getAction().name(), beaconResourceTypes, beaconRequest.getResource());
                }
            }

        } else {
            LOG.debug("Unauthorized");
            unauthorized(response, "Unauthorized");
        }
    }

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        response.sendError(403, message);
    }
}
