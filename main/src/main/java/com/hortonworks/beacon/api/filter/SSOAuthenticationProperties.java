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

import java.security.interfaces.RSAPublicKey;

/**
 * This knox sso authentication config properties pojo class.
 */
public class SSOAuthenticationProperties {

    private String authenticationProviderUrl = null;
    private RSAPublicKey publicKey = null;
    private String cookieName = "hadoop-jwt";
    private String originalUrlQueryParam = null;
    private String[] userAgentList = null;

    public String getAuthenticationProviderUrl() {
        return authenticationProviderUrl;
    }

    public void setAuthenticationProviderUrl(String authenticationProviderUrl) {
        this.authenticationProviderUrl = authenticationProviderUrl;
    }

    public RSAPublicKey getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(RSAPublicKey publicKey) {
        this.publicKey = publicKey;
    }

    public String getCookieName() {
        return cookieName;
    }

    public void setCookieName(String cookieName) {
        this.cookieName = cookieName;
    }

    public String getOriginalUrlQueryParam() {
        return originalUrlQueryParam;
    }

    public void setOriginalUrlQueryParam(String originalUrlQueryParam) {
        this.originalUrlQueryParam = originalUrlQueryParam;
    }

    /**
     * @return the userAgentList
     */
    public String[] getUserAgentList() {
        return (String[]) userAgentList.clone();
    }

    /**
     * @param userAgentList the userAgentList to set
     */
    public void setUserAgentList(String[] userAgentList) {
        this.userAgentList = (String[])userAgentList.clone();
    }
}

