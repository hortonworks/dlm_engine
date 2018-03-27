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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Date;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
/**
 * This enforces knox sso authentication as part of the filter before processing the request.
 */

public class BeaconKnoxSSOAuthenticationFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconKnoxSSOAuthenticationFilter.class);
    public static final String BEACON_SSO_ENABLED="beacon.sso.knox.authentication.enabled";
    public static final String BROWSER_USERAGENT = "beacon.sso.knox.browser.useragent";
    public static final String JWT_AUTH_PROVIDER_URL = "beacon.sso.knox.providerurl";
    public static final String JWT_PUBLIC_KEY = "beacon.sso.knox.publicKey";
    public static final String JWT_COOKIE_NAME = "beacon.sso.knox.cookiename";
    public static final String JWT_ORIGINAL_URL_QUERY_PARAM = "beacon.sso.knox.query.param.originalurl";
    public static final String JWT_COOKIE_NAME_DEFAULT = "hadoop-jwt";
    public static final String JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT = "originalUrl";
    public static final String DEFAULT_BROWSER_USERAGENT = "beacon.sso.knox.default.browser-useragents";
    public static final String LOCAL_LOGIN_URL = "locallogin";
    private String originalUrlQueryParam = "originalUrl";
    private String cookieName = "hadoop-jwt";
    private SSOAuthenticationProperties jwtProperties;
    private String authenticationProviderUrl = null;
    private RSAPublicKey publicKey = null;
    private JWSVerifier verifier = null;
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();

    public BeaconKnoxSSOAuthenticationFilter() {
        try {
            LOG.debug("Security Config:"+PropertiesUtil.getPropertiesMap().toString());
            jwtProperties = loadJwtProperties();
            setJwtProperties();
        } catch (Exception e) {
            LOG.error("Error while getting application properties.", e);
        }
    }

    public BeaconKnoxSSOAuthenticationFilter(SSOAuthenticationProperties jwtProperties) {
        this.jwtProperties = jwtProperties;
        setJwtProperties();
    }

    @Override
    public void init(FilterConfig filterConfig) {
    }

    /*
     * doFilter of BeaconKnoxSSOAuthenticationFilter is the first in the filter list so in this it check for the request
     * if the request is from browser and sso is enabled then it process the request against knox sso
     * else if it's ssoenable and the request is with local login string then it show's the appropriate msg
     * else if ssoenable is false then it continues with further filters as it was before sso
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        boolean ssoEnabled=isSSOEnabled();
        if (!ssoEnabled) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }
        HttpServletRequest httpRequest = (HttpServletRequest)servletRequest;
        if (httpRequest.getRequestedSessionId() != null && !httpRequest.isRequestedSessionIdValid()){
            synchronized(httpRequest.getServletContext()){
                if (httpRequest.getServletContext().getAttribute(httpRequest.getRequestedSessionId()) != null
                        && "locallogin".equals(httpRequest.getServletContext()
                                .getAttribute(httpRequest.getRequestedSessionId()).toString())) {
                    ssoEnabled = false;
                    httpRequest.getSession().setAttribute("locallogin", "true");
                    httpRequest.getServletContext().removeAttribute(httpRequest.getRequestedSessionId());
                }
            }
        }

        String userAgent = httpRequest.getHeader("User-Agent");
        if (httpRequest.getSession() != null){
            if (httpRequest.getSession().getAttribute("locallogin") != null){
                servletRequest.setAttribute("ssoEnabled", false);
                filterChain.doFilter(servletRequest, servletResponse);
                return;
            }
        }

        //If sso is enable and request is not for local login and is from browser then
        //it will go inside and try for knox sso authentication
        if (ssoEnabled && !httpRequest.getRequestURI().contains(LOCAL_LOGIN_URL)) {
            //if jwt properties are loaded and is current not authenticated then it will go for sso authentication
            //Note : Need to remove !isAuthenticated() after knoxsso solve the bug from cross-origin script
            if (jwtProperties != null && !isAuthenticated(httpRequest)) {
                HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
                String serializedJWT = getJWTFromCookie(httpRequest);
                // if we get the hadoop-jwt token from the cookies then will process it further
                if (serializedJWT != null) {
                    SignedJWT jwtToken = null;
                    try {
                        jwtToken = SignedJWT.parse(serializedJWT);
                        boolean valid = validateToken(jwtToken);
                        //if the public key provide is correct and also token is not expired the process token
                        if (valid) {
                            final String userName = jwtToken.getJWTClaimsSet().getSubject();
                            String requestURL = httpRequest.getRequestURL()+"?"+httpRequest.getQueryString();
                            LOG.debug("Knox SSO user: [{}]", userName);
                            LOG.debug("Request URI: {}", requestURL);
                            if (StringUtils.isNotEmpty(userName)) {
                                HttpSession session = httpRequest.getSession();
                                if (session != null) {
                                    if (session.getAttribute("username") == null) {
                                        synchronized (session) {
                                            if (session.getAttribute("username") == null) {
                                                session.setAttribute("username", userName);
                                                session.setMaxInactiveInterval(30*60);
                                            }
                                        }
                                    }
                                    if (session.getAttribute("username") != null) {
                                        servletRequest.setAttribute("ssoEnabled", true);
                                    }
                                }
                            }

                            HttpServletRequestWrapper requestWithUsername = new HttpServletRequestWrapper(httpRequest) {
                                public String getRemoteUser() {
                                    return userName;
                                }
                            };
                            filterChain.doFilter(requestWithUsername, httpServletResponse);
                        } else {
                            // if the token is not valid then redirect to knox sso
                            if (isWebUserAgent(userAgent)) {
                                redirectToKnox(httpRequest, httpServletResponse);
                            } else {
                                filterChain.doFilter(servletRequest, httpServletResponse);
                            }
                        }
                    } catch (ParseException e) {
                        LOG.warn("Unable to parse the JWT token", e);
                    }
                } else {
                    // if the jwt token is not available then redirect it to knox sso
                    if (isWebUserAgent(userAgent)) {
                        redirectToKnox(httpRequest, httpServletResponse);
                    } else {
                        filterChain.doFilter(servletRequest, httpServletResponse);
                    }
                }
            } else {
                //if property is not loaded or is already authenticated then proceed further with next filter
                filterChain.doFilter(servletRequest, servletResponse);
            }
        } else if (ssoEnabled && ((HttpServletRequest) servletRequest).getRequestURI().contains(LOCAL_LOGIN_URL)
                && isWebUserAgent(userAgent) && isAuthenticated(httpRequest)) {
            //If already there's an active session with sso and user want's to switch to local login(i.e without sso)
            //then it won't be navigated to local login
            // In this scenario the user as to use separate browser
            String url = ((HttpServletRequest) servletRequest).getRequestURI().replace(LOCAL_LOGIN_URL+"/", "");
            url = url.replace(LOCAL_LOGIN_URL, "");
            LOG.warn(
                "There is an active session and if you want local login to beacon, try this on a separate browser");
            ((HttpServletResponse)servletResponse).sendRedirect(url);
        } else {
            //if sso is not enable or the request is not from browser then proceed further with next filter
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    private void redirectToKnox(HttpServletRequest httpRequest, HttpServletResponse httpServletResponse)
            throws IOException {
        String ajaxRequestHeader = httpRequest.getHeader("X-Requested-With");
        if ("XMLHttpRequest".equals(ajaxRequestHeader)) {
            String ssourl = constructLoginURL(httpRequest, true);
            JsonObject json = new JsonObject();
            json.addProperty("knoxssoredirectURL", URLEncoder.encode(ssourl, "UTF-8"));
            httpServletResponse.setContentType("application/json");
            httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, json.toString());
        } else {
            String ssourl = constructLoginURL(httpRequest, false);
            httpServletResponse.sendRedirect(ssourl);
            LOG.debug("After sendRedirect: {}");
        }
    }

    private boolean isWebUserAgent(String userAgent) {
        boolean isWeb = false;
        if (jwtProperties != null) {
            String []userAgentList = jwtProperties.getUserAgentList();
            if (userAgentList != null && userAgentList.length > 0) {
                for (String ua : userAgentList) {
                    if (StringUtils.startsWithIgnoreCase(userAgent, ua)) {
                        isWeb = true;
                        break;
                    }
                }
            }
        }
        return isWeb;
    }


    private void setJwtProperties() {
        if (jwtProperties != null) {
            authenticationProviderUrl = jwtProperties.getAuthenticationProviderUrl();
            publicKey = jwtProperties.getPublicKey();
            cookieName = jwtProperties.getCookieName();
            originalUrlQueryParam = jwtProperties.getOriginalUrlQueryParam();
            if (publicKey != null) {
                verifier = new RSASSAVerifier(publicKey);
            }
        }
    }

    /**
     * Do not try to validate JWT if user already authenticated via other provider.
     *
     * @return true, if JWT validation required
     */
    private boolean isAuthenticated(HttpServletRequest httpRequest) {
        boolean isSSOAuthenticated=false;
        if (httpRequest!=null){
            HttpSession session =httpRequest.getSession();
            if (session!=null){
                if (session.getAttribute("username") != null) {
                    if (httpRequest.getAttribute("ssoEnabled") != null
                            && httpRequest.getAttribute("ssoEnabled").equals(Boolean.TRUE)) {
                        isSSOAuthenticated=true;
                    }
                }
            }
        }
        return isSSOAuthenticated;
    }

    /**
     * Encapsulate the acquisition of the JWT token from HTTP cookies within the
     * request.
     *
     * @param req servlet request to get the JWT token from
     * @return serialized JWT token
     */
    protected String getJWTFromCookie(HttpServletRequest req) {
        String serializedJWT = null;
        Cookie[] cookies = req.getCookies();
        if (cookieName != null && cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookieName.equals(cookie.getName())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} cookie has been found and is being processed", cookieName);
                    }
                    serializedJWT = cookie.getValue();
                    break;
                }
            }
        }
        return serializedJWT;
    }

    /**
     * Create the URL to be used for authentication of the user in the absence
     * of a JWT token within the incoming request.
     *
     * @param request for getting the original request URL
     * @return url to use as login url for redirect
     */
    protected String constructLoginURL(HttpServletRequest request, boolean isXMLRequest) {
        String delimiter = "?";
        if (authenticationProviderUrl.contains("?")) {
            delimiter = "&";
        }
        StringBuilder loginURL = new StringBuilder();
        if (isXMLRequest) {
            String beaconApplicationURL = "";
            String referalURL = request.getHeader("referer");
            if (referalURL == null) {
                beaconApplicationURL = request.getScheme() + "://" + request.getServerName() + ":"
                        + request.getServerPort() + request.getContextPath();
            } else {
                beaconApplicationURL = referalURL;
            }
            loginURL.append(authenticationProviderUrl).append(delimiter).append(originalUrlQueryParam).append("=")
                    .append(beaconApplicationURL);
        } else {
            loginURL.append(authenticationProviderUrl).append(delimiter).append(originalUrlQueryParam).append("=")
                    .append(request.getRequestURL().append(getOriginalQueryString(request)));
        }
        return loginURL.toString();
    }

    private String getOriginalQueryString(HttpServletRequest request) {
        String originalQueryString = request.getQueryString();
        return (originalQueryString == null) ? "" : "?" + originalQueryString;
    }

    /**
     * This method provides a single method for validating the JWT for use in
     * request processing. It provides for the override of specific aspects of
     * this implementation through submethods used within but also allows for
     * the override of the entire token validation algorithm.
     *
     * @param jwtToken the token to validate
     * @return true if valid
     */
    protected boolean validateToken(SignedJWT jwtToken) {
        boolean isValid = validateSignature(jwtToken);
        if (isValid) {
            isValid = validateExpiration(jwtToken);
            if (!isValid) {
                LOG.warn("Expiration time validation of JWT token failed.");
            }
        } else {
            LOG.warn("Signature of JWT token could not be verified. Please check the public key");
        }
        return isValid;
    }

    /**
     * Verify the signature of the JWT token in this method. This method depends
     * on the public key that was established during init based upon the
     * provisioned public key. Override this method in subclasses in order to
     * customize the signature verification behavior.
     *
     * @param jwtToken the token that contains the signature to be validated
     * @return valid true if signature verifies successfully; false otherwise
     */
    protected boolean validateSignature(SignedJWT jwtToken) {
        boolean valid = false;
        if (JWSObject.State.SIGNED == jwtToken.getState()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SSO token is in a SIGNED state");
            }
            if (jwtToken.getSignature() != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSO token signature is not null");
                }
                try {
                    if (verifier != null && jwtToken.verify(verifier)) {
                        valid = true;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("SSO token has been successfully verified");
                        }
                    } else {
                        LOG.warn("SSO signature verification failed.Please check the public key");
                    }
                } catch (JOSEException je) {
                    LOG.warn("Error while validating signature: {}", je);
                } catch (Exception e) {
                    LOG.warn("Error while validating signature: {}", e);
                }
            }
        }
        return valid;
    }

    /**
     * Validate that the expiration time of the JWT token has not been violated.
     * If it has then throw an AuthenticationException. Override this method in
     * subclasses in order to customize the expiration validation behavior.
     *
     * @param jwtToken the token that contains the expiration date to validate
     * @return valid true if the token has not expired; false otherwise
     */
    protected boolean validateExpiration(SignedJWT jwtToken) {
        boolean valid = false;
        try {
            Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
            LOG.debug("Expires: {}", expires);
            if (expires == null || new Date().before(expires)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSO token expiration date has been successfully validated");
                }
                valid = true;
            } else {
                LOG.warn("SSO expiration date validation failed.");
            }
        } catch (ParseException pe) {
            LOG.warn("SSO expiration date validation failed.", pe);
        }
        return valid;
    }

    @Override
    public void destroy() {
    }

    public SSOAuthenticationProperties loadJwtProperties() {
        String providerUrl =AUTHCONFIG.getProperty(JWT_AUTH_PROVIDER_URL);
        if (providerUrl != null && isSSOEnabled()) {
            SSOAuthenticationProperties ssoAuthenticationProperties = new SSOAuthenticationProperties();
            String publicKeyPathStr =AUTHCONFIG.getProperty(JWT_PUBLIC_KEY);
            if (publicKeyPathStr == null) {
                LOG.error("Public key pem not specified for SSO auth provider. SSO auth will be disabled");
                return null;
            }
            ssoAuthenticationProperties.setAuthenticationProviderUrl(providerUrl);
            ssoAuthenticationProperties.setCookieName(
                    AUTHCONFIG.getProperty(JWT_COOKIE_NAME, JWT_COOKIE_NAME_DEFAULT));
            ssoAuthenticationProperties.setOriginalUrlQueryParam(
                    AUTHCONFIG.getProperty(JWT_ORIGINAL_URL_QUERY_PARAM, JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT));
            String userAgent = AUTHCONFIG.getProperty(BROWSER_USERAGENT);
            if (StringUtils.isEmpty(userAgent)){
                userAgent = AUTHCONFIG.getProperty(DEFAULT_BROWSER_USERAGENT);
            }
            if (userAgent != null && !userAgent.isEmpty()) {
                ssoAuthenticationProperties.setUserAgentList(userAgent.split(","));
            }
            try {
                RSAPublicKey rsaPublicKey = parseRSAPublicKey(publicKeyPathStr);
                ssoAuthenticationProperties.setPublicKey(rsaPublicKey);
            } catch (ServletException e) {
                LOG.error("ServletException while processing the properties", e);
            }
            return ssoAuthenticationProperties;
        } else {
            return null;
        }
    }

    public static RSAPublicKey parseRSAPublicKey(String pem)
            throws ServletException {
        String pemHeader = "-----BEGIN CERTIFICATE-----\n";
        String pemFooter = "\n-----END CERTIFICATE-----";
        String fullPem = pemHeader + pem + pemFooter;
        PublicKey key = null;
        try {
            CertificateFactory fact = CertificateFactory.getInstance("X.509");
            ByteArrayInputStream is = new ByteArrayInputStream(fullPem.getBytes("UTF8"));
            X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
            key = cer.getPublicKey();
        } catch (CertificateException ce) {
            LOG.error("CertificateException: {}", ce);
            String message = null;
            if (pem.startsWith(pemHeader)) {
                message = "CertificateException - be sure not to include PEM header "
                        + "and footer in the PEM configuration element.";
            } else {
                message = "CertificateException - PEM may be corrupt";
            }
            throw new ServletException(message, ce);
        } catch (UnsupportedEncodingException uee) {
            LOG.error("CertificateException: {}", uee);
            throw new ServletException(uee);
        }
        return (RSAPublicKey) key;
    }

    private boolean isSSOEnabled(){
        return AUTHCONFIG.getBooleanProperty(BEACON_SSO_ENABLED, false);
    }

}
