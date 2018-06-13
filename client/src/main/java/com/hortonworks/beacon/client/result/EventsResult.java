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

package com.hortonworks.beacon.client.result;

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
    private long numSyncEvents;

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

    private void setEventsCollection(EventInstance[] events, long totalResults, int size, long numSyncEvents) {
        this.events = events;
        this.totalResults = totalResults;
        this.results = size;
        this.numSyncEvents = numSyncEvents;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public int getResults() {
        return results;
    }

    public long getNumSyncEvents() {
        return numSyncEvents;
    }

    @Override
    public Object[] getCollection() {
        return getEvents();
    }

    public void setCollection(Object[] items) {
        if (items == null) {
            setCollection(null, 0, 0);
        } else {
            setCollection(items, items.length, 0);
        }
    }

    public void setCollection(Object[] items, long totalResults, long numSyncEvents) {
        if (items == null || totalResults == 0) {
            setEventsCollection(new EventInstance[0], 0, 0, 0);
        } else {
            EventInstance[] newInstances = new EventInstance[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (EventInstance) items[index];
            }
            setEventsCollection(newInstances, totalResults, newInstances.length, numSyncEvents);
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
