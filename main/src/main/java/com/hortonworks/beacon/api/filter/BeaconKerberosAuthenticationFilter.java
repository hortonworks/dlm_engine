/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.filter;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.util.KerberosName;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;


/**
 * This enforces authentication as part of the filter before processing the request.
 * Subclass of com.hortonworks.beacon.api.filter.BeaconAuthenticationFilter
 */

public class BeaconKerberosAuthenticationFilter extends BeaconAuthenticationFilter {
    private static final BeaconLog LOG = BeaconLog.getLog(BeaconKerberosAuthenticationFilter.class);
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

    private HttpServlet optionsServlet;

    /**
     * Initialize the filter.
     *
     * @param filterConfig filter configuration.
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info(MessageCode.MAIN_000136.name());
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
            LOG.error(MessageCode.MAIN_000132.name(), e.toString());
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

        optionsServlet = new HttpServlet() {
            private static final long serialVersionUID = 1L;
        };
        optionsServlet.init();

    }

    @Override
    protected void doFilter(FilterChain filterChain, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {
        String userName = getUsernameFromResponse(response);
        if ((isSpnegoEnable() && (!StringUtils.isEmpty(userName)))) {
            // --------------------------- To Create Beacon Session
            // --------------------------------------
            request.setAttribute("spnegoEnabled", true);
            LOG.info(MessageCode.MAIN_000130.name(), userName);
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            String requestURL = httpRequest.getRequestURL() + "?" + httpRequest.getQueryString();
            LOG.info(MessageCode.MAIN_000109.name(), requestURL);
            super.doFilter(filterChain, request, response);
        } else {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000105.name(), Response.Status.UNAUTHORIZED);
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
                LOG.info(MessageCode.MAIN_000131.name(), userName);
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
                LOG.error(MessageCode.MAIN_000132.name(), e.toString());
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
        LOG.info(MessageCode.MAIN_000133.name(), userName);
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
                                            LOG.info(MessageCode.MAIN_000134.name(), e.getMessage());
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



