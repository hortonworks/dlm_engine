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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Policy report element into policy list response.
 */
@XmlRootElement(name = "report")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyReport {

    @XmlElement
    private PolicyInstanceList.InstanceElement lastSucceededInstance;

    @XmlElement
    private PolicyInstanceList.InstanceElement lastFailedInstance;


    public PolicyInstanceList.InstanceElement getLastSucceededInstance() {
        return lastSucceededInstance;
    }

    public void setLastSucceededInstance(PolicyInstanceList.InstanceElement lastSucceededInstance) {
        this.lastSucceededInstance = lastSucceededInstance;
    }

    public PolicyInstanceList.InstanceElement getLastFailedInstance() {
        return lastFailedInstance;
    }

    public void setLastFailedInstance(PolicyInstanceList.InstanceElement lastFailedInstance) {
        this.lastFailedInstance = lastFailedInstance;
    }
}
