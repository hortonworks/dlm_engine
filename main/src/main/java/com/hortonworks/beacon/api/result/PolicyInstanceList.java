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

package com.hortonworks.beacon.api.result;

import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.DateUtil;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.sql.Timestamp;
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
    private final int totalResults;

    @XmlElement(name = "instance")
    private final InstanceElement[] elements;

    /**
     * Summary of an Policy Instance.
     */
    public static class InstanceElement {
        @XmlElement
        public String id;
        @XmlElement
        public String name;
        @XmlElement
        public String type;
        @XmlElement
        public String executionType;
        @XmlElement
        public String status;
        @XmlElement
        public String startTime;
        @XmlElement
        public String endTime;
        @XmlElement
        public long duration;
        @XmlElement
        public String message;
    }

    public PolicyInstanceList() {
        this.elements = null;
        this.totalResults = 0;
    }

    public PolicyInstanceList(int totalResults, InstanceElement[] elements) {
        this.totalResults = totalResults;
        this.elements = elements;
    }

    public PolicyInstanceList(List<PolicyInstanceBean> beanList) {
        this.totalResults = beanList.size();
        this.elements = new InstanceElement[totalResults];
        for (int i = 0; i < beanList.size(); i++) {
            PolicyInstanceBean bean = beanList.get(i);
            InstanceElement element = createInstanceElement(bean);
            elements[i] = element;
        }
    }

    private InstanceElement createInstanceElement(PolicyInstanceBean bean) {
        InstanceElement element = new InstanceElement();
        element.id = bean.getId();
        element.name = bean.getName();
        element.executionType = bean.getJobExecutionType();
        element.type = bean.getType();
        element.status = bean.getStatus();
        element.startTime = DateUtil.formatDate(new Date(bean.getStartTime().getTime()));
        Timestamp endTime = bean.getEndTime();
        element.endTime = DateUtil.formatDate(endTime != null ? new Date(endTime.getTime()) : null);
        element.duration = bean.getDuration();
        element.message = bean.getMessage();
        return element;
    }

    public int getTotalResults() {
        return totalResults;
    }

    public InstanceElement[] getElements() {
        return elements;
    }
}
