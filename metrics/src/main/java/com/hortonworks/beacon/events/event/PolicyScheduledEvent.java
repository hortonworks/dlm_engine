/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.events.event;

import com.hortonworks.beacon.events.BeaconEvent;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.EventInfo;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyBean;

import java.sql.Timestamp;

/**
 * Policy Scheduled Event class.
 */
public class PolicyScheduledEvent extends BeaconEvent {
    private static final String EVENT_MESSAGE = "replication policy scheduled";
    private String policyId;
    private String eventMessage;
    private EventInfo eventInfo;

    public PolicyScheduledEvent(Events event, PolicyBean bean, EventInfo eventInfo) {
        super(event);
        this.policyId = bean.getId();
        this.eventMessage = EVENT_MESSAGE;
        this.eventInfo = eventInfo;
    }

    public EventBean getEventBean() {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId(policyId);
        eventBean.setEventEntityType(EventEntityType.POLICY.getName());
        eventBean.setEventId(getEventId());
        eventBean.setEventSeverity(getEventSeverity());
        eventBean.setEventTimeStamp(new Timestamp(getTime()));
        eventBean.setEventMessage(eventMessage);
        eventBean.setEventInfo(eventInfo.toJsonString());
        return  eventBean;
    }
}
