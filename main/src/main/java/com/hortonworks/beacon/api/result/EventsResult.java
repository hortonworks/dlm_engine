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

import com.hortonworks.beacon.client.resource.APIResult;
import org.apache.commons.lang3.StringUtils;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * EventResult List is the output returned by all the Events related APIs.
 */

//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class EventsResult extends APIResult {

    @XmlElement
    private int totalCount;

    @XmlElement
    private EventInstance[] events;

    public EventsResult() {
    }

    public EventsResult(Status status, String message) {
        super(status, message);
    }

    public EventInstance[] getEvents() {
        return events;
    }

    public void setEventsCollection(EventInstance[] events, int size) {
        this.events = events;
        this.totalCount = size;
    }

    public int getTotalCount() {
        return totalCount;
    }

    @Override
    public Object[] getCollection() {
        return getEvents();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setEventsCollection(new EventInstance[0], 0);
        } else {
            EventInstance[] newInstances = new EventInstance[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (EventInstance)items[index];
            }
            setEventsCollection(newInstances, newInstances.length);
        }
    }

    /**
     * Event object for EventList Result.
     */
    @XmlRootElement(name = "eventinstance")
    public static class EventInstance {

        @XmlElement
        public String policyId;

        @XmlElement
        public String instanceId;

        @XmlElement
        public String event;

        @XmlElement
        public String eventType;

        @XmlElement
        public String policyReplType;

        @XmlElement
        public String severity;

        @XmlElement
        public Boolean syncEvent;

        @XmlElement
        public String timestamp;

        @XmlElement
        public String message;

        @Override
        public String toString() {
            return "Events{"
                    + "policyId='" + (StringUtils.isNotBlank(policyId) ? policyId : "") + '\''
                    + ", instanceId='" + (StringUtils.isNotBlank(instanceId) ? instanceId : "") + '\''
                    + ", event='" + event + '\''
                    + ", eventType='" + eventType + '\''
                    + ", policyReplType='" + policyReplType + (StringUtils.isNotBlank(policyReplType)
                                                            ? policyReplType : "") +'\''
                    + ", severity='" + severity + '\''
                    + ", syncEvent='" + syncEvent + '\''
                    + ", timestamp='" + timestamp + '\''
                    + ", message='" + message + '\''
                    + '}';
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck
