/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
    private long totalResults;

    @XmlElement
    private int results;

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

    private void setEventsCollection(EventInstance[] events, long totalResults, int size) {
        this.events = events;
        this.totalResults = totalResults;
        this.results = size;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public int getResults() {
        return results;
    }

    @Override
    public Object[] getCollection() {
        return getEvents();
    }

    public void setCollection(Object[] items) {
        if (items == null) {
            setCollection(null, 0);
        } else {
            setCollection(items, items.length);
        }
    }

    public void setCollection(Object[] items, long totalResults) {
        if (items == null || totalResults == 0) {
            setEventsCollection(new EventInstance[0], 0, 0);
        } else {
            EventInstance[] newInstances = new EventInstance[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (EventInstance) items[index];
            }
            setEventsCollection(newInstances, totalResults, newInstances.length);
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
                    + ", severity='" + severity + '\''
                    + ", syncEvent='" + syncEvent + '\''
                    + ", timestamp='" + timestamp + '\''
                    + ", message='" + message + '\''
                    + '}';
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck
