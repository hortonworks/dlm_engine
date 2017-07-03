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

import com.hortonworks.beacon.events.event.BeaconStartedEvent;
import com.hortonworks.beacon.events.event.BeaconStoppedEvent;
import com.hortonworks.beacon.events.event.ClusterEntityDeletedEvent;
import com.hortonworks.beacon.events.event.ClusterEntityPairedEvent;
import com.hortonworks.beacon.events.event.ClusterEntitySubmittedEvent;
import com.hortonworks.beacon.events.event.PolicyDeletedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceDeletedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceFailedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceIgnoredEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceKilledEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceSucceededEvent;
import com.hortonworks.beacon.events.event.PolicyScheduledEvent;
import com.hortonworks.beacon.events.event.PolicySubmittedEvent;
import com.hortonworks.beacon.events.event.PolicySyncedEvent;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;

/**
 * Event Handler class for Events Initialization.
 */
final class EventHandler {
    private EventHandler(){
    }

    static BeaconEvent getEvents(Events event, EventEntityType entityType) {
        BeaconEvent beaconEvent = null;
        if (entityType == EventEntityType.SYSTEM) {
            beaconEvent = getSystemEvent(event);
        } else if (entityType == EventEntityType.CLUSTER) {
            beaconEvent = getClusterEvent(event);
        }
        return beaconEvent;
    }

    static BeaconEvent getEvents(Events event, EventEntityType entityType, PolicyBean bean, EventInfo eventInfo) {
        BeaconEvent beaconEvent = null;
        if (entityType == EventEntityType.POLICY) {
            beaconEvent = getPolicyEvent(event, bean, eventInfo);
        }
        return beaconEvent;
    }

    static BeaconEvent getEvents(Events event, EventEntityType entityType, PolicyInstanceBean bean) {
        BeaconEvent beaconEvent = null;
        if (entityType == EventEntityType.POLICYINSTANCE) {
            beaconEvent = getPolicyInstanceEvent(event, bean);
        }
        return beaconEvent;
    }


    private static BeaconEvent getSystemEvent(Events event) {
        BeaconEvent beaconEvent;
        switch (event) {
            case STARTED:
                beaconEvent = new BeaconStartedEvent(event);
                break;
            case STOPPED:
                beaconEvent = new BeaconStoppedEvent(event);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Event", event.name()));
        }
        return beaconEvent;
    }

    private static BeaconEvent getClusterEvent(Events event) {
        BeaconEvent beaconEvent;
        switch (event) {
            case SUBMITTED:
                beaconEvent = new ClusterEntitySubmittedEvent(event);
                break;
            case DELETED:
                beaconEvent = new ClusterEntityDeletedEvent(event);
                break;
            case PAIRED:
                beaconEvent = new ClusterEntityPairedEvent(event);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Event", event.name()));
        }
        return beaconEvent;
    }

    private static BeaconEvent getPolicyInstanceEvent(Events event, PolicyInstanceBean bean) {
        BeaconEvent beaconEvent;
        switch (event) {
            case SUCCEEDED:
                beaconEvent = new PolicyInstanceSucceededEvent(event, bean);
                break;
            case FAILED:
                beaconEvent = new PolicyInstanceFailedEvent(event, bean);
                break;
            case IGNORED:
                beaconEvent = new PolicyInstanceIgnoredEvent(event, bean);
                break;
            case DELETED:
                beaconEvent = new PolicyInstanceDeletedEvent(event, bean);
                break;
            case KILLED:
                beaconEvent = new PolicyInstanceKilledEvent(event, bean);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Event", event.name()));

        }
        return beaconEvent;
    }

    private static BeaconEvent getPolicyEvent(Events event, PolicyBean bean, EventInfo eventInfo) {
        BeaconEvent beaconEvent;
        switch (event) {
            case SUBMITTED:
                beaconEvent = new PolicySubmittedEvent(event, bean, eventInfo);
                break;
            case SYNCED:
                beaconEvent = new PolicySyncedEvent(event, bean, eventInfo);
                break;
            case SCHEDULED:
                beaconEvent = new PolicyScheduledEvent(event, bean, eventInfo);
                break;
            case DELETED:
                beaconEvent = new PolicyDeletedEvent(event, bean, eventInfo);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Event", event.name()));
        }
        return beaconEvent;
    }
}
