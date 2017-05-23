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

package com.hortonworks.beacon.store.result;

import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.DateUtil;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Date;
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
    @SuppressFBWarnings("URF_UNREAD_FIELD")
    public static class InstanceElement {
        @XmlElement
        private String id;
        @XmlElement
        private String policyId;
        @XmlElement
        private String name;
        @XmlElement
        private String type;
        @XmlElement
        private String executionType;
        @XmlElement
        private String user;
        @XmlElement
        private String status;
        @XmlElement
        private String trackingInfo;
        @XmlElement
        private String startTime;
        @XmlElement
        private String endTime;
        @XmlElement
        private String message;
    }

    public PolicyInstanceList(List<InstanceElement> elements, long totalCount) {
        this.totalResults = totalCount;
        this.elements = elements.toArray(new InstanceElement[elements.size()]);
        this.results = elements.size();
    }

    public static InstanceElement createInstanceElement(String name, String type, String executionType, String user,
                                                        PolicyInstanceBean bean) {
        InstanceElement element = new InstanceElement();
        element.id = bean.getInstanceId();
        element.policyId = bean.getPolicyId();
        element.name = name;
        element.type = type;
        element.executionType = executionType;
        element.user = user;
        element.status = bean.getStatus();
        element.trackingInfo = bean.getTrackingInfo();
        element.startTime = DateUtil.formatDate(new Date(bean.getStartTime().getTime()));
        element.endTime = DateUtil.formatDate(bean.getEndTime() != null ? new Date(bean.getEndTime().getTime()) : null);
        element.message = bean.getMessage();
        return element;
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
