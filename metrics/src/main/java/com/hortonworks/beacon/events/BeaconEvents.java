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
