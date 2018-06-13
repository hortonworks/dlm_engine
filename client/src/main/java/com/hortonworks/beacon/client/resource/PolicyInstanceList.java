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

    // For JAX-B
    public PolicyInstanceList() {
        this.totalResults = 0;
        this.elements = new InstanceElement[0];
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
