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
import com.hortonworks.beacon.events.event.BeaconStartedEvent;
import com.hortonworks.beacon.events.event.BeaconStoppedEvent;
import com.hortonworks.beacon.events.event.ClusterEntityDeletedEvent;
import com.hortonworks.beacon.events.event.ClusterEntityPairedEvent;
import com.hortonworks.beacon.events.event.ClusterEntitySubmittedEvent;
import com.hortonworks.beacon.events.event.ClusterEntityUnPairedEvent;
import com.hortonworks.beacon.events.event.PolicyDeletedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceDeletedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceFailedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceSkippedEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceKilledEvent;
import com.hortonworks.beacon.events.event.PolicyInstanceSucceededEvent;
import com.hortonworks.beacon.events.event.PolicyResumedEvent;
import com.hortonworks.beacon.events.event.PolicyScheduledEvent;
import com.hortonworks.beacon.events.event.PolicySubmittedEvent;
import com.hortonworks.beacon.events.event.PolicySuspendedEvent;
import com.hortonworks.beacon.events.event.PolicySyncedEvent;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.StringFormat;

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
        }

        return beaconEvent;
    }

    static BeaconEvent getEvents(Events event, EventEntityType entityType, Cluster cluster) {
        BeaconEvent beaconEvent = null;
        if (entityType == EventEntityType.CLUSTER) {
            beaconEvent = getClusterEvent(event, cluster);
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
                throw new IllegalArgumentException(
                    StringFormat.format("Event type: {} is not supported", event.name()));
        }
        return beaconEvent;
    }

    private static BeaconEvent getClusterEvent(Events event, Cluster cluster) {
        BeaconEvent beaconEvent;
        switch (event) {
            case SUBMITTED:
                beaconEvent = new ClusterEntitySubmittedEvent(event, cluster);
                break;
            case DELETED:
                beaconEvent = new ClusterEntityDeletedEvent(event, cluster);
                break;
            case PAIRED:
                beaconEvent = new ClusterEntityPairedEvent(event, cluster);
                break;
            case UNPAIRED:
                beaconEvent = new ClusterEntityUnPairedEvent(event, cluster);
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Event type: {} is not supported", event.name()));
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
            case SKIPPED:
                beaconEvent = new PolicyInstanceSkippedEvent(event, bean);
                break;
            case DELETED:
                beaconEvent = new PolicyInstanceDeletedEvent(event, bean);
                break;
            case KILLED:
                beaconEvent = new PolicyInstanceKilledEvent(event, bean);
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Event type: {} is not supported", event.name()));

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
            case SUSPENDED:
                beaconEvent = new PolicySuspendedEvent(event, bean, eventInfo);
                break;
            case RESUMED:
                beaconEvent = new PolicyResumedEvent(event, bean, eventInfo);
                break;
            case DELETED:
                beaconEvent = new PolicyDeletedEvent(event, bean, eventInfo);
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Event type: {} is not supported", event.name()));
        }
        return beaconEvent;
    }
}
