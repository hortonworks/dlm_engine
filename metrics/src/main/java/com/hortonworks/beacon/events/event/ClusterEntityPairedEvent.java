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

package com.hortonworks.beacon.events.event;

import com.hortonworks.beacon.events.BeaconEvent;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.store.bean.EventBean;

import java.sql.Timestamp;

/**
 * Cluster Entity Paired Event class.
 */
public class ClusterEntityPairedEvent extends BeaconEvent {
    private static final String EVENT_MESSAGE = "cluster entity paired";
    private String eventMessage;

    public ClusterEntityPairedEvent(Events events) {
        super(events);

        this.eventMessage = EVENT_MESSAGE;
    }

    public EventBean getEventBean() {
        EventBean eventBean = new EventBean();
        eventBean.setEventEntityType(EventEntityType.CLUSTER.getName());
        eventBean.setEventId(getEventId());
        eventBean.setEventTimeStamp(new Timestamp(getTime()));
        eventBean.setEventMessage(eventMessage);
        return eventBean;
    }
}
