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
        TYPE, NAME, STATUS, TAGS, CLUSTERS, FREQUENCY, STARTTIME, ENDTIME, DATASETS
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
        public int frequencyInSec;

        @XmlElement
        public List<String> tags;

        @XmlElement
        public Properties customProperties;

        @XmlElement
        public String user;

        @XmlElement
        public int retryAttempts;

        @XmlElement
        public long retryDelay;

        @XmlElement
        public String notificationType;

        @XmlElement
        public String notificationTo;
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
