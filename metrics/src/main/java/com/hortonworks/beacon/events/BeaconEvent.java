/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.events;

import com.hortonworks.beacon.store.bean.EventBean;

/**
 * Beacon Event Detail.
 */
public abstract class BeaconEvent {
    private int eventId;
    private String eventSeverity;
    private long time;

    public BeaconEvent() {
    }

    public BeaconEvent(Events event) {
        eventSeverity = event.getEventSeverity().getName();
        time = System.currentTimeMillis();
        eventId = event.getId();
    }

    public int getEventId() {
        return eventId;
    }

    public String getEventSeverity() {
        return eventSeverity;
    }

    public long getTime() {
        return time;
    }


    public abstract EventBean getEventBean();
}
