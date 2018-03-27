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

import com.hortonworks.beacon.api.exception.BeaconAuthException;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.beacon.api.filter.BeaconBasicAuthFilter.unauthorized;


/**
 * This enforces authentication as part of the filter before processing the request.
 * Subclass of com.hortonworks.beacon.api.filter.BeaconAuthenticationFilter
 */

public class BeaconKerberosAuthenticationFilter extends BeaconAuthenticationFilter {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconKerberosAuthenticationFilter.class);
    protected static final ServletContext NULL_SERVLETCONTEXT = new NullServletContext();
    private static final String BEACON_KERBEROS_AUTH_ENABLED="beacon.kerberos.authentication.enabled";
    private static final String BEACON_AUTH_TYPE = "beacon.kerberos.authentication.type";
    private static final String NAME_RULES = "beacon.kerberos.namerules.auth_to_local";
    private static final String TOKEN_VALID = "beacon.kerberos.token.valid.seconds";
    private static final String COOKIE_DOMAIN = "beacon.kerberos.cookie.domain";
    private static final String COOKIE_PATH = "beacon.kerberos.cookie.path";
    private static final String PRINCIPAL = "beacon.kerberos.spnego.principal";
    private static final String KEYTAB = "beacon.kerberos.spnego.keytab";

    private static final String AUTH_TYPE = "type";
    private static final String NAME_RULES_PARAM = "kerberos.name.rules";
    private static final String TOKEN_VALID_PARAM = "token.validity";
    private static final String COOKIE_DOMAIN_PARAM = "cookie.domain";
    private static final String COOKIE_PATH_PARAM = "cookie.path";
    private static final String PRINCIPAL_PARAM = "kerberos.principal";
    private static final String KEYTAB_PARAM = "kerberos.keytab";
    private static final String AUTH_COOKIE_NAME = "hadoop.auth";
    private static final String KERBEROS_TYPE = "kerberos";
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();

    /**
     * Initialize the filter.
     *
     * @param filterConfig filter configuration.
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("BeaconKerberosAuthenticationFilter initialization started");
        if (!isSpnegoEnable()) {
            return;
        }
        final FilterConfig globalConf = filterConfig;
        final Map<String, String> params = new HashMap<>();
        params.put(AUTH_TYPE, AUTHCONFIG.getProperty(BEACON_AUTH_TYPE, KERBEROS_TYPE));
        params.put(NAME_RULES_PARAM, AUTHCONFIG.getProperty(NAME_RULES, "DEFAULT"));
        params.put(TOKEN_VALID_PARAM, AUTHCONFIG.getProperty(TOKEN_VALID, "30"));
        params.put(COOKIE_DOMAIN_PARAM,
                AUTHCONFIG.getProperty(COOKIE_DOMAIN, BeaconConfig.getInstance().getEngine().getHostName()));
        params.put(COOKIE_PATH_PARAM, AUTHCONFIG.getProperty(COOKIE_PATH, "/"));
        String principal="*";
        try {
            principal = SecureClientLogin.getPrincipal(AUTHCONFIG.getProperty(PRINCIPAL),
                    BeaconConfig.getInstance().getEngine().getHostName());
        } catch (IOException e) {
            LOG.error("Unable to read principal: {}", e.toString());
        }
        params.put(PRINCIPAL_PARAM, principal);
        params.put(KEYTAB_PARAM, AUTHCONFIG.getProperty(KEYTAB));
        FilterConfig filterConfig1 = new FilterConfig() {
            @Override
            public ServletContext getServletContext() {
                if (globalConf != null) {
                    return globalConf.getServletContext();
                } else {
                    return NULL_SERVLETCONTEXT;
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public Enumeration<String> getInitParameterNames() {
                return new IteratorEnumeration(params.keySet().iterator());
            }

            @Override
            public String getInitParameter(String param) {
                return params.get(param);
            }

            @Override
            public String getFilterName() {
                return "KerberosFilter";
            }
        };

        super.init(filterConfig1);

    }

    @Override
    protected void doFilter(FilterChain filterChain, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {
        final String userName = getUsernameFromResponse(response);
        if ((isSpnegoEnable() && (!StringUtils.isEmpty(userName)))) {
            // --------------------------- To Create Beacon Session
            // --------------------------------------
            request.setAttribute("spnegoEnabled", true);
            request.setAttribute("kerberosEnabled", true);
            LOG.debug("Kerberos user: [{}]", userName);
            String requestURL = request.getRequestURL() + "?" + request.getQueryString();
            LOG.debug("Request URI: {}", requestURL);

            HttpServletRequestWrapper requestWithUsername = new HttpServletRequestWrapper(request) {
                public String getRemoteUser() {
                    return userName;
                }
            };

            super.doFilter(filterChain, requestWithUsername, response);
        } else {
            unauthorized(response, "Unauthorized");
            throw BeaconAuthException.newAPIException("Invalid login credentials at kerberos authentication filter");
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        boolean isSSOAuthenticated = false;
        HttpServletRequest httpRequest = (HttpServletRequest) request;


        if (httpRequest != null) {
            HttpSession session = httpRequest.getSession();
            if (session != null) {
                if (session.getAttribute("username") != null) {
                    if (httpRequest.getAttribute("ssoEnabled") != null
                            && httpRequest.getAttribute("ssoEnabled").equals(Boolean.TRUE)) {
                        isSSOAuthenticated = true;
                    }
                }
            }
        }
        if (isSSOAuthenticated) {
            filterChain.doFilter(request, response);
            return;
        }

        if (isSpnegoEnable()) {
            KerberosName.setRules(AUTHCONFIG.getProperty(NAME_RULES));
            String userName = getUsernameFromRequest(httpRequest);
            if (!StringUtils.isEmpty(userName)) {
                request.setAttribute("spnegoEnabled", true);
                request.setAttribute("kerberosEnabled", true);
                LOG.debug("Login into beacon as = {}", userName);
            } else {
                super.doFilter(request, response, filterChain);
                HttpServletResponse httpResponse = (HttpServletResponse) response;
                if (httpResponse.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                    filterChain.doFilter(request, response);
                    return;
                }
            }
        } else {
            filterChain.doFilter(request, response);
            return;
        }
    }

    private boolean isSpnegoEnable() {
        boolean isKerberos = AUTHCONFIG.getBooleanProperty(BEACON_KERBEROS_AUTH_ENABLED, false);
        if (isKerberos && KERBEROS_TYPE.equalsIgnoreCase(AUTHCONFIG.getProperty(BEACON_AUTH_TYPE))) {
            return isKerberos;
        }
        if (isKerberos) {
            isKerberos = false;
            String keytab = AUTHCONFIG.getProperty(KEYTAB);
            String principal="*";
            try {
                principal = SecureClientLogin.getPrincipal(AUTHCONFIG.getProperty(PRINCIPAL),
                        BeaconConfig.getInstance().getEngine().getHostName());
            } catch (IOException e) {
                LOG.error("Unable to read principal: {}", e.toString());
            }
            String hostname = BeaconConfig.getInstance().getEngine().getHostName();
            if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(principal)
                    && StringUtils.isNotEmpty(hostname)) {
                isKerberos = true;
            }
        }
        return isKerberos;
    }

    private String getUsernameFromRequest(HttpServletRequest httpRequest) {
        String userName = null;
        Cookie[] cookie = httpRequest.getCookies();
        if (cookie != null) {
            for (Cookie c : cookie) {
                String cname = c.getName();
                if (cname != null && "u".equalsIgnoreCase(cname)) {
                    int ustr = cname.indexOf("u=");
                    if (ustr != -1) {
                        int andStr = cname.indexOf("&", ustr);
                        if (andStr != -1) {
                            userName = cname.substring(ustr + 2, andStr);
                        }
                    }
                } else if (cname != null && AUTH_COOKIE_NAME.equalsIgnoreCase(cname)) {
                    String value = c.getValue();
                    int ustr = value.indexOf("u=");
                    if (ustr != -1) {
                        int andStr = value.indexOf("&", ustr);
                        if (andStr != -1) {
                            userName = value.substring(ustr + 2, andStr);
                        }
                    }
                }
            }
        }
        if (userName == null) {
            userName = httpRequest.getRemoteUser();
            if (StringUtils.isEmpty(userName)) {
                userName = httpRequest.getParameter("user.name");
            }
            if (StringUtils.isEmpty(userName)) {
                userName = httpRequest.getHeader("Remote-User");
            }
        }
        LOG.debug("Kerberos username from request >>>>>>>> {}", userName);
        return userName;
    }

    private static String getUsernameFromResponse(HttpServletResponse response1) {
        String userName = null;
        boolean isCookieSet = response1.containsHeader("Set-Cookie");
        if (isCookieSet) {
            String cookie = response1.getHeader("Set-Cookie");
            if (!StringUtils.isEmpty(cookie)) {
                if (cookie.toLowerCase().startsWith(AuthenticatedURL.AUTH_COOKIE.toLowerCase())
                        && cookie.contains("u=")) {
                    String[] split = cookie.split(";");
                    if (split != null) {
                        for (String s : split) {
                            if (!StringUtils.isEmpty(s)
                                    && s.toLowerCase().startsWith(AuthenticatedURL.AUTH_COOKIE.toLowerCase())) {
                                int ustr = s.indexOf("u=");
                                if (ustr != -1) {
                                    int andStr = s.indexOf("&", ustr);
                                    if (andStr != -1) {
                                        try {
                                            userName = s.substring(ustr + 2, andStr);
                                            break;
                                        } catch (Exception e) {
                                            userName = null;
                                            LOG.info("Exception: {}", e.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return userName;
    }
}



