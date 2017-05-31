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

package com.hortonworks.beacon.events;

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
