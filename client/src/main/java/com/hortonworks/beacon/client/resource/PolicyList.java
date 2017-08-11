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
        TYPE, NAME, STATUS, TAGS, CLUSTERS, FREQUENCY, STARTTIME, ENDTIME, DATASETS, INSTANCES
    }

    /**
     * Element within an entity.
     */
    public static class PolicyElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
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
        public String startTime;

        @XmlElement
        public String endTime;

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
