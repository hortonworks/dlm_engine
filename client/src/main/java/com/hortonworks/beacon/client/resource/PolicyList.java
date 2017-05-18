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


import com.hortonworks.beacon.client.entity.Entity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "policies")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolicyList {
    public int getTotalResults() {
        return totalResults;
    }

    @XmlElement
    private int totalResults;

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
        public String type;
        @XmlElement
        public String name;
        @XmlElement
        public String status;
        @XmlElementWrapper(name = "frequencyInSec")
        public Integer frequency;
        @XmlElement
        public String startTime;
        @XmlElement
        public String endTime;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;
        @XmlElementWrapper(name = "sourceCluster")
        public String sourceCluster;
        @XmlElementWrapper(name = "targetCluster")
        public String targetCluster;
        @XmlElement
        public String sourceDataset;
        @XmlElement
        public String targetDataset;

        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            return "PolicyElement {"
                    + "type='" + type + '\''
                    + ", name='" + name + '\''
                    + ", status='" + status + '\''
                    + ", frequency=" + frequency
                    + ", startTime='" + startTime + '\''
                    + ", endTime='" + endTime + '\''
                    + ", tag=" + tag
                    + ", sourceCluster='" + sourceCluster + '\''
                    + ", targetCluster='" + targetCluster + '\''
                    + ", sourceDataset='" + sourceDataset + '\''
                    + ", targetDataset='" + targetDataset + '\''
                    + '}';
        }
    }

    //For JAXB
    public PolicyList() {
        this.elements = null;
        this.totalResults = 0;
    }

    public PolicyList(PolicyElement[] elements, int totalResults) {
        this.totalResults = totalResults;
        this.elements = elements != null ? Arrays.copyOf(elements, elements.length) : null;
    }

    public PolicyList(Entity[] elements, int totalResults) {
        this.totalResults = totalResults;
        int len = elements.length;
        PolicyElement[] items = new PolicyElement[len];
        for (int i = 0; i < len; i++) {
            items[i] = createPolicyElement(elements[i]);
        }
        this.elements = items;
    }

    private PolicyElement createPolicyElement(Entity e) {
        PolicyElement element = new PolicyElement();
        element.type = e.getEntityType().name().toLowerCase();
        element.name = e.getName();
        element.tag = new ArrayList<>();
        return element;
    }

    public PolicyElement[] getElements() {
        return elements != null ? Arrays.copyOf(elements, elements.length) : null;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(totalResults + "\n");
        for (PolicyElement element : elements) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }
}
