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
import java.util.Properties;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "policies")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyList {

    @XmlElement
    private long totalResults;

    @XmlElement
    private long results;

    public long getTotalResults() {
        return totalResults;
    }

    public long getResults() {
        return results;
    }

    @XmlElement(name = "policy")
    private final PolicyElement[] elements;

    /**
     * List of fields returned by RestAPI.
     * Policy-name and type are default.
     */
    public enum PolicyFieldList {
        TYPE, NAME, STATUS, TAGS, CLUSTERS, FREQUENCY, STARTTIME, ENDTIME, DATASETS, INSTANCES, EXECUTIONTYPE,
        CUSTOMPROPERTIES, REPORT
    }

    /**
     * Element within an entity.
     */
    public static class PolicyElement {
        //SUSPEND CHECKSTYLE CHEC VisibilityModifierCheck
        @XmlElement
        public String policyId;

        @XmlElement
        public String type;

        @XmlElement
        public String name;

        @XmlElement
        public String description;

        @XmlElement
        public String status;

        @XmlElement
        public String executionType;

        @XmlElement
        public String sourceDataset;

        @XmlElement
        public String targetDataset;

        @XmlElement
        public String sourceCluster;

        @XmlElement
        public String targetCluster;

        @XmlElement
        public String creationTime;

        @XmlElement
        public String startTime;

        @XmlElement
        public String endTime;

        @XmlElement
        public String retirementTime;

        @XmlElement
        public Integer frequencyInSec;

        @XmlElement
        public List<String> tags;

        @XmlElement
        public Properties customProperties;

        @XmlElement
        public String user;

        @XmlElement
        public Integer retryAttempts;

        @XmlElement
        public Long retryDelay;

        @XmlElement
        public String notificationType;

        @XmlElement
        public String notificationTo;

        @XmlElement
        public PolicyInstanceList.InstanceElement[] instances;

        @XmlElement(name = "report")
        public PolicyReport policyReport;
        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck
    }

    //For JAXB
    public PolicyList() {
        this.elements = null;
        this.totalResults = 0;
        this.results = 0;
    }

    public PolicyList(PolicyElement[] elements, long totalResults) {
        this.totalResults = totalResults;
        this.elements = elements != null ? Arrays.copyOf(elements, elements.length) : null;
        this.results = elements != null ? elements.length : 0;
    }

    public PolicyElement[] getElements() {
        return elements != null ? Arrays.copyOf(elements, elements.length) : null;
    }
}
