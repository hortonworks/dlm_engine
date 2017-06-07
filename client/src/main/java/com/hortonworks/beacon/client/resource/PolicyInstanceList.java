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
