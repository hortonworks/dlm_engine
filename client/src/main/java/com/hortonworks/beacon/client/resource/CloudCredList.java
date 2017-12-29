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
import com.hortonworks.beacon.client.entity.CloudCred;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Cloud cred list for REST API response.
 */
@XmlRootElement(name = "cloudCreds")
@XmlAccessorType(XmlAccessType.FIELD)
public class CloudCredList {

    @XmlElement
    private String requestId;

    @XmlElement
    private long totalResults;

    @XmlElement(name = "cloudCred")
    private CloudCred[] cloudCreds;

    @XmlElement
    private int results;

    public CloudCredList(long totalResults, List<CloudCred> elements) {
        this.totalResults = totalResults;
        this.cloudCreds = elements.toArray(new CloudCred[elements.size()]);
        this.results = elements.size();
        this.requestId = RequestContext.get().getRequestId();
    }

    public CloudCredList() {
        this.requestId = RequestContext.get().getRequestId();
    }

    public String getRequestId() {
        return requestId;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public int getResults() {
        return results;
    }

    public CloudCred[] getCloudCreds() {
        CloudCred[] credElements = new CloudCred[cloudCreds.length];
        System.arraycopy(cloudCreds, 0, credElements, 0, cloudCreds.length);
        return credElements;
    }
}
