/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.resource;

import com.hortonworks.beacon.RequestContext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * API result that contains user priviliges.
 */
@XmlRootElement(name = "user-privileges")
@XmlAccessorType(XmlAccessType.FIELD)
public class UserPrivilegesResult {
    private String requestId;
    private boolean hdfsSuperUser;
    private String userName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public UserPrivilegesResult() {
        this.requestId = RequestContext.get().getRequestId();
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public boolean isHdfsSuperUser() {
        return hdfsSuperUser;
    }

    public void setHdfsSuperUser(boolean hdfsSuperUser) {
        this.hdfsSuperUser = hdfsSuperUser;
    }
}
