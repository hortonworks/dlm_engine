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
