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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.events.BeaconEvent;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.store.bean.EventBean;

import java.sql.Timestamp;

/**
 * Cluster Entity Paired Event class.
 */
public class ClusterEntityPairedEvent extends BeaconEvent {
    private static final String EVENT_MESSAGE = "cluster paired";
    private String eventMessage;

    public ClusterEntityPairedEvent(Events events, Cluster cluster) {
        super(events);
        this.eventMessage = cluster.getName()+" "+ EVENT_MESSAGE +" with "+cluster.getPeers();
    }

    public EventBean getEventBean() {
        EventBean eventBean = new EventBean();
        eventBean.setEventEntityType(EventEntityType.CLUSTER.getName());
        eventBean.setEventId(getEventId());
        eventBean.setEventSeverity(getEventSeverity());
        eventBean.setEventTimeStamp(new Timestamp(getTime()));
        eventBean.setEventMessage(eventMessage);
        return eventBean;
    }
}
