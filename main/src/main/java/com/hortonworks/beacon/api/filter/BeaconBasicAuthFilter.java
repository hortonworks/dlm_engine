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
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.api.exception.BeaconAuthException;
import com.hortonworks.beacon.config.PropertiesUtil;

/**
 * This enforces basic authentication as part of the filter before processing the request.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BeaconBasicAuthFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconBasicAuthFilter.class);
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
        boolean basicAuthEnabled = AUTHCONFIG.getBooleanProperty(BEACON_BASIC_AUTH_ENABLED, true);
        if (!isSSOAuthenticated && !isKrbAuthenticated && !basicAuthEnabled) {
            boolean throwException = false;
            if (httpResponse.getStatus() == HttpServletResponse.SC_UNAUTHORIZED
                  &&  (!httpResponse.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE))) {
                throwException = true;
            }
            unauthorized(httpResponse, "Unauthorized");

            if (throwException) {
                throw BeaconAuthException.newAPIException("Invalid login credentials at basic authentication filter");
            }
        }
        String inputUsername = null;
        boolean authenticated = false;
        if (basicAuthEnabled) {
            String authHeader = httpRequest.getHeader("Authorization");
            if (authHeader != null) {
                StringTokenizer st = new StringTokenizer(authHeader);
                if (st.hasMoreTokens()) {
                    String basic = st.nextToken();
                    if (basic.equalsIgnoreCase("Basic")) {
                        try {
                            if (st.hasMoreTokens()) {
                                String credentials = new String(Base64.decodeBase64(st.nextToken()), "UTF-8");
                                int p = credentials.indexOf(":");
                                if (p != -1) {
                                    inputUsername = credentials.substring(0, p).trim();
                                    String inputPassword = credentials.substring(p + 1).trim();
                                    if (StringUtils.isNotEmpty(inputUsername)) {
                                        if (isValidCredentials(inputUsername, inputPassword)) {
                                            authenticated = true;
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
                            } else {
                                unauthorized(httpResponse, "Bad credentials");
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
        if (authenticated) {
            final String finalInputUsername = inputUsername;
            HttpServletRequestWrapper requestWithUsername = new HttpServletRequestWrapper(httpRequest) {
                public String getRemoteUser() {
                    return finalInputUsername;
                }
            };
            filterChain.doFilter(requestWithUsername, response);
        } else {
            if (httpResponse.getStatus() == HttpServletResponse.SC_UNAUTHORIZED
                    &&   httpResponse.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)) {
                unauthorized(httpResponse, "Unauthorized");
            } else {
                unauthorized(httpResponse, "Unauthorized");
                throw BeaconAuthException.newAPIException("Invalid login credentials at basic authentication filter");
            }
        }
    }

    protected static void unauthorized(HttpServletResponse response, String message) throws IOException {
        if (!response.isCommitted()) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, message);
        }
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
            LOG.error("Wrong credentials provided for user: {}", username);
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
            LOG.error("Exception: {}", ex.getMessage());
        }
        return hexString.toString();
    }
}
