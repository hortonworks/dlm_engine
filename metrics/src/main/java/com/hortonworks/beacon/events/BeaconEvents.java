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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.EventsExecutor;

/**
 * Create a method for events and invoke the method from beacon components.
 */

public final class BeaconEvents {

    private BeaconEvents() {
    }

    public static void createEvents(Events event, EventEntityType entityType) {
        persistEvents(createEventsBean(event, entityType));
    }

    public static void createEvents(Events event, EventEntityType entityType, Cluster cluster) {
        persistEvents(createEventsBean(event, entityType, cluster));
    }

    public static void createEvents(Events event, EventEntityType entityType, PolicyBean bean, EventInfo eventInfo) {
        persistEvents(createEventsBean(event, entityType, bean, eventInfo));
    }

    public static void createEvents(Events event, EventEntityType entityType, PolicyInstanceBean bean) {
        persistEvents(createEventsBean(event, entityType, bean));
    }

    static EventBean createEventsBean(Events event, EventEntityType entityType) {
        BeaconEvent beaconEvent = EventHandler.getEvents(event, entityType);
        return beaconEvent.getEventBean();
    }

    static EventBean createEventsBean(Events event, EventEntityType entityType, Cluster cluster) {
        BeaconEvent beaconEvent = EventHandler.getEvents(event, entityType, cluster);
        return beaconEvent.getEventBean();
    }

    static EventBean createEventsBean(Events event, EventEntityType entityType, PolicyBean bean, EventInfo eventInfo) {
        BeaconEvent beaconEvent = EventHandler.getEvents(event, entityType, bean, eventInfo);
        return beaconEvent.getEventBean();
    }

    static EventBean createEventsBean(Events event, EventEntityType entityType, PolicyInstanceBean bean) {
        BeaconEvent beaconEvent = EventHandler.getEvents(event, entityType, bean);
        return beaconEvent.getEventBean();
    }

    private static void persistEvents(EventBean eventBean) {
        EventsExecutor eventsExecutor = new EventsExecutor();
        eventsExecutor.persistEvents(eventBean);
    }
}
