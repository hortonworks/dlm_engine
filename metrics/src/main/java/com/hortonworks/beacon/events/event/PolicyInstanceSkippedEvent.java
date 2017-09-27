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
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;

import java.sql.Timestamp;

/**
 * Policy Instance Ignored Event class.
 */
public class PolicyInstanceSkippedEvent extends BeaconEvent {
    private static final String EVENT_MESSAGE = "policy instance skipped";
    private String policyId;
    private String instanceId;
    private String eventMessage;

    public PolicyInstanceSkippedEvent(Events event, PolicyInstanceBean bean) {
        super(event);
        this.policyId = bean.getPolicyId();
        this.instanceId = bean.getInstanceId();
        this.eventMessage = EVENT_MESSAGE;
    }

    public EventBean getEventBean() {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId(policyId);
        eventBean.setInstanceId(instanceId);
        eventBean.setEventEntityType(EventEntityType.POLICYINSTANCE.getName());
        eventBean.setEventId(getEventId());
        eventBean.setEventSeverity(getEventSeverity());
        eventBean.setEventTimeStamp(new Timestamp(getTime()));
        eventBean.setEventMessage(eventMessage);
        return eventBean;
    }
}
