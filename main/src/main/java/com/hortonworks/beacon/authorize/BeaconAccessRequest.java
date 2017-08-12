/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.authorize;

import java.util.Date;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import com.hortonworks.beacon.authorize.simple.BeaconAuthorizationUtils;
import com.hortonworks.beacon.log.BeaconLog;

/**
 * This class contains details related to request received at beacon end.
 */

public class BeaconAccessRequest {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconAccessRequest.class);
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
