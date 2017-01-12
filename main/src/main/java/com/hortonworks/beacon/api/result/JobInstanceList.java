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

import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.util.DateUtil;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.List;

@XmlRootElement(name = "instances")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInstanceList {

    @XmlElement
    private final int totalResults;

    @XmlElement(name = "instance")
    private final InstanceElement[] elements;

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

    public JobInstanceList() {
        this.elements = null;
        this.totalResults = 0;
    }

    public JobInstanceList(int totalResults, InstanceElement[] elements) {
        this.totalResults = totalResults;
        this.elements = elements;
    }

    public JobInstanceList(List<JobInstanceBean> beanList) {
        this.totalResults = beanList.size();
        this.elements = new InstanceElement[totalResults];
        for (int i = 0; i < beanList.size(); i++) {
            JobInstanceBean bean = beanList.get(i);
            InstanceElement element = createInstanceElement(bean);
            elements[i] = element;
        }
    }

    private InstanceElement createInstanceElement(JobInstanceBean bean) {
        InstanceElement element = new InstanceElement();
        element.id = bean.getId();
        element.name = bean.getName();
        element.executionType = bean.getJobExecutionType();
        element.type = bean.getType();
        element.status = bean.getStatus();
        element.startTime = DateUtil.formatDate(new Date(bean.getStartTime().getTime()));
        element.endTime = DateUtil.formatDate(new Date(bean.getEndTime().getTime()));
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
