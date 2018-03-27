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

import java.util.Date;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.authorize.simple.BeaconAuthorizationUtils;

/**
 * This class contains details related to request received at beacon end.
 */

public class BeaconAccessRequest {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconAccessRequest.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private Set<BeaconResourceTypes> resourceType = null;
    private String resource = null;
    private BeaconActionTypes action = null;
    private String user = null;
    private Set<String> userGroups = null;
    private Date accessTime = null;
    private String clientIPAddress = null;

    public BeaconAccessRequest(HttpServletRequest request, String user, Set<String> userGroups) {
        this(BeaconAuthorizationUtils.getBeaconResourceType(request.getRequestURI()), "*", BeaconAuthorizationUtils
            .getBeaconAction(request.getMethod()), user, userGroups,
            BeaconAuthorizationUtils.getRequestIpAddress(request));
    }

    public BeaconAccessRequest(Set<BeaconResourceTypes> resourceType, String resource, BeaconActionTypes action,
        String user, Set<String> userGroups, String clientIPAddress) {
        if (isDebugEnabled) {
            LOG.debug("==> BeaconAccessRequestImpl-- Initializing BeaconAccessRequest");
        }
        setResource(resource);
        setAction(action);
        setUser(user);
        setUserGroups(userGroups);
        setResourceType(resourceType);

        // set remaining fields to default value
        setAccessTime(new Date());
        setClientIPAddress(clientIPAddress);
    }

    public Set<BeaconResourceTypes> getResourceTypes() {
        return resourceType;
    }

    public void setResourceType(Set<BeaconResourceTypes> resourceType) {
        this.resourceType = resourceType;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public BeaconActionTypes getAction() {
        return action;
    }

    public void setAction(BeaconActionTypes action) {
        this.action = action;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUserGroups(Set<String> userGroups) {
        this.userGroups = userGroups;
    }

    public Set<String> getUserGroups() {
        return userGroups;
    }

    public Date getAccessTime() {
        return new Date(accessTime.getTime());
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = new Date(accessTime.getTime());
    }

    public String getClientIPAddress() {
        return clientIPAddress;
    }

    public void setClientIPAddress(String clientIPAddress) {
        this.clientIPAddress = clientIPAddress;
    }

    @Override
    public String toString() {
        return "BeaconAccessRequest [resourceType=" + resourceType + ", resource=" + resource + ", action=" + action
            + ", user=" + user + ", userGroups=" + userGroups + ", accessTime=" + accessTime + ", clientIPAddress="
            + clientIPAddress + "]";
    }

}
