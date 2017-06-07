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

package com.hortonworks.beacon.api.filter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.StringTokenizer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.log.BeaconLog;

/**
 * This enforces basic authentication as part of the filter before processing the request.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BeaconBasicAuthFilter implements Filter {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconBasicAuthFilter.class);
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    static final String BEACON_BASIC_AUTH_ENABLED="beacon.basic.authentication.enabled";

    /**
     * <p>
     * Initializes the authentication filter and signer secret provider.
     * </p>
     * It instantiates and initializes the specified
     * {@link AuthenticationHandler}.
     *
     * @param filterConfig
     *            filter configuration.
     *
     * @throws ServletException
     *             thrown if the filter or the authentication handler could not
     *             be initialized properly.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        LOG.info("Beacon Session doFilter:");
        boolean isSSOAuthenticated = false;
        boolean isKrbAuthenticated = false;
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        if (httpRequest!=null) {
            HttpSession session = httpRequest.getSession();
            if (session != null) {
                if (session.getAttribute("username") != null) {
                    if (httpRequest.getAttribute("ssoEnabled") != null
                            && httpRequest.getAttribute("ssoEnabled").equals(Boolean.TRUE)) {
                        isSSOAuthenticated = true;
                    }
                    if (httpRequest.getAttribute("kerberosEnabled") != null
                            && httpRequest.getAttribute("kerberosEnabled").equals(Boolean.TRUE)) {
                        isKrbAuthenticated = true;
                    }
                }
                if (isSSOAuthenticated || isKrbAuthenticated) {
                    filterChain.doFilter(request, response);
                    return;
                }
            }
        }
        boolean isBasicAuthentication = AUTHCONFIG.getBooleanProperty(BEACON_BASIC_AUTH_ENABLED, true);
        if (!isSSOAuthenticated && !isKrbAuthenticated && !isBasicAuthentication) {
            unauthorized(httpResponse, "Unauthorized");
            throw BeaconWebException.newAPIException("Invalid Login credentials", Response.Status.UNAUTHORIZED);
        }
        if (isBasicAuthentication) {
            isBasicAuthentication = false;
            String authHeader = httpRequest.getHeader("Authorization");
            if (authHeader != null) {
                StringTokenizer st = new StringTokenizer(authHeader);
                if (st.hasMoreTokens()) {
                    String basic = st.nextToken();
                    if (basic.equalsIgnoreCase("Basic")) {
                        try {
                            String credentials = new String(Base64.decodeBase64(st.nextToken()), "UTF-8");
                            int p = credentials.indexOf(":");
                            if (p != -1) {
                                String inputUsername = credentials.substring(0, p).trim();
                                String inputPassword = credentials.substring(p + 1).trim();
                                if (StringUtils.isNotEmpty(inputUsername)) {
                                    if (isValidCredentials(inputUsername, inputPassword)) {
                                        isBasicAuthentication = true;
                                        LOG.info("Basic auth user: [{}]", inputUsername);
                                        String requestURL = httpRequest.getRequestURL() + "?"
                                                + httpRequest.getQueryString();
                                        LOG.info("Request URI : {} ", requestURL);
                                        HttpSession session = httpRequest.getSession();
                                        if (session != null) {
                                            if (session.getAttribute("username") == null) {
                                                synchronized (session) {
                                                    if (session.getAttribute("username") == null) {
                                                        session.setAttribute("username", inputUsername);
                                                        session.setMaxInactiveInterval(30*60);
                                                    }
                                                }
                                            }
                                            if (session.getAttribute("username") != null) {
                                                request.setAttribute("basicAuthentication", true);
                                            }
                                        }
                                    } else {
                                        unauthorized(httpResponse, "Bad credentials");
                                    }
                                }
                            } else {
                                unauthorized(httpResponse, "Invalid authentication token");
                            }
                        } catch (UnsupportedEncodingException e) {
                            throw new Error("Couldn't retrieve authentication", e);
                        }
                    }
                }
            } else {
                unauthorized(httpResponse, "Unauthorized");
            }
        }
        if (isBasicAuthentication) {
            filterChain.doFilter(request, response);
        } else {
            throw BeaconWebException.newAPIException("Invalid Login credentials", Response.Status.UNAUTHORIZED);
        }
    }

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        // response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm +
        // "\"");
        response.sendError(401, message);
    }

    @Override
    public void destroy() {
    }

    private boolean isValidCredentials(String username, String credentials) {
        if (username == null || username.isEmpty() || credentials == null || credentials.isEmpty()) {
            return false;
        }
        String userdetailsStr = AUTHCONFIG.getProperty(username);
        String encodedPassword = getSha256Hash(credentials);
        if (userdetailsStr == null || userdetailsStr.isEmpty()) {
            return false;
        }
        String password = "";
        //String role = "";
        String []dataArr = userdetailsStr.split("::");
        if (dataArr != null && dataArr.length == 2) {
            //role = dataArr[0];
            password = dataArr[1];
        }
        if (encodedPassword.equals(password)) {
            return true;
        } else {
            LOG.error("Wrong credentials provided for user: " + username);
        }
        return false;
    }

    private static String getSha256Hash(String clearText) {
        StringBuffer hexString = new StringBuffer();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(clearText.getBytes("UTF-8"));
            for (byte aHash : hash) {
                String hex = Integer.toHexString(0xff & aHash);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();

        } catch (Exception ex) {
            LOG.error("Exception: " + ex.getMessage());
        }
        return hexString.toString();
    }
}
