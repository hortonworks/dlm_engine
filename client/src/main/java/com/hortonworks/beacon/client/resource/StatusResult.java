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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.hortonworks.beacon.client.entity.Entity;

/**
 * Cluster/Policy status API result.
 */
@XmlRootElement(name = "status")
@XmlAccessorType(XmlAccessType.FIELD)
public class StatusResult {

    @XmlElement
    private String requestId;

    @XmlElement
    private String name;

    @XmlElement
    private Entity.EntityStatus status;

    public StatusResult(String policyName, String status) {
        this(policyName, Entity.EntityStatus.valueOf(status));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Entity.EntityStatus getStatus() {
        return status;
    }

    public void setStatus(Entity.EntityStatus status) {
        this.status = status;
    }

    public StatusResult(String name, Entity.EntityStatus status) {
        this.name = name;
        this.status = status;
        this.requestId = RequestContext.get().getRequestId();
    }
}
