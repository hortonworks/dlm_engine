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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.EventsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Create a method for events and invoke the method from beacon components.
 */

public final class BeaconEvents {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconEvents.class);

    private BeaconEvents() {
    }


    public static void createSystemEvents(int eventId, long time, EventStatus status,
                                          String eventMessage) {
        String hostname = BeaconConfig.getInstance().getEngine().getHostName();
        persistEvents(createSystemEventsBean(eventId, time, status, hostname, eventMessage));
    }

    public static void createClusterEvents(int eventId, long time, EventStatus status,
                                           String eventMessage) {
        persistEvents(createClusterEventsBean(eventId, time, status, eventMessage));
    }

    public static void createPolicyEvents(int eventId, long time, EventStatus status,
                                          String eventMessage, PolicyBean policyBean) {
        persistEvents(createPolicyEventsBean(eventId, time, status, eventMessage, policyBean));
    }

    public static void createPolicyInstanceEvents(int eventId, long time, EventStatus status,
                                                  String eventMessage, PolicyInstanceBean instanceBean) {
        persistEvents(createPolicyInstanceEventsBean(eventId, time, status, eventMessage, instanceBean));
    }

    public static EventBean createSystemEventsBean(int eventId, long time, EventStatus status,
                                                   String hostname, String eventMessage) {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId("ID");
        eventBean.setInstanceId(hostname);
        eventBean.setEventEntityType(EventEntityType.SYSTEM.getName());
        eventBean.setEventId(eventId);
        eventBean.setEventTimeStamp(new Timestamp(time));
        eventBean.setEventStatus(status.getName());
        eventBean.setEventMessage(eventMessage);

        return eventBean;
    }


    public static EventBean createClusterEventsBean(int eventId, long time, EventStatus status,
                                                    String eventMessage) {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId("ID");
        eventBean.setInstanceId("cluster");
        eventBean.setEventEntityType(EventEntityType.CLUSTER.getName());
        eventBean.setEventId(eventId);
        eventBean.setEventTimeStamp(new Timestamp(time));
        eventBean.setEventStatus(status.getName());
        eventBean.setEventMessage(eventMessage);

        return eventBean;
    }

    public static EventBean createPolicyInstanceEventsBean(int eventId, long time, EventStatus status,
                                                           String eventMessage, PolicyInstanceBean bean) {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId(bean.getPolicyId());
        eventBean.setInstanceId(bean.getInstanceId());
        eventBean.setEventEntityType(EventEntityType.POLICY.getName());
        eventBean.setEventId(eventId);
        eventBean.setEventTimeStamp(new Timestamp(time));
        eventBean.setEventStatus(status.getName());
        eventBean.setEventMessage(bean.getMessage());

        return eventBean;
    }


    public static EventBean createPolicyEventsBean(int eventId, long time, EventStatus status,
                                                   String eventMessage, PolicyBean bean) {
        EventBean eventBean = new EventBean();
        eventBean.setPolicyId(bean.getId());
        eventBean.setInstanceId(bean.getId());
        eventBean.setEventEntityType(EventEntityType.POLICY.getName());
        eventBean.setEventId(eventId);
        eventBean.setEventTimeStamp(new Timestamp(time));
        eventBean.setEventStatus(status.getName());
        eventBean.setEventMessage(eventMessage);

        return eventBean;
    }


    public static void persistEvents(EventBean eventBean) {
        EventsExecutor eventsExecutor = new EventsExecutor();
        eventsExecutor.persistEvents(eventBean);
    }
}
