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
import java.util.Arrays;
import java.util.List;

/**
 * Instance list of an beacon policy used for marshalling / unmarshalling with REST calls.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement(name = "instances")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyInstanceList {

    @XmlElement
    private final long totalResults;

    @XmlElement(name = "instance")
    private final InstanceElement[] elements;

    @XmlElement
    private int results;

    /**
     * Summary of an Policy Instance.
     */
    public static class InstanceElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
        @XmlElement
        public String id;
        @XmlElement
        public String policyId;
        @XmlElement
        public String name;
        @XmlElement
        public String type;
        @XmlElement
        public String executionType;
        @XmlElement
        public String user;
        @XmlElement
        public String status;
        @XmlElement
        public String trackingInfo;
        @XmlElement
        public String startTime;
        @XmlElement
        public String endTime;
        @XmlElement
        public String retryAttempted;
        @XmlElement
        public String message;
        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck
    }

    public PolicyInstanceList(List<InstanceElement> elements, long totalCount) {
        this.totalResults = totalCount;
        this.elements = elements.toArray(new InstanceElement[elements.size()]);
        this.results = elements.size();
    }


    public long getTotalResults() {
        return totalResults;
    }

    public long getResults() {
        return results;
    }

    public InstanceElement[] getElements() {
        return Arrays.copyOf(elements, elements.length);
    }
}
