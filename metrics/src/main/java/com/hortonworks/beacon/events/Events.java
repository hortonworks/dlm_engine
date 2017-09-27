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

import java.util.HashMap;
import java.util.Map;

/**
 * List of the Events in Beacon system.
 * IMPORTANT : Append new events at end. Don't add in between.
 */
public enum Events {
    STARTED(0, "started", EventSeverity.INFO),
    STOPPED(1, "stopped", EventSeverity.INFO),
    SUBMITTED(2, "submitted", EventSeverity.INFO),
    DELETED(3, "deleted", EventSeverity.WARN),
    PAIRED(4, "paired", EventSeverity.INFO),
    UNPAIRED(5, "unpaired", EventSeverity.INFO),
    SYNCED(6, "synced", EventSeverity.INFO),
    SCHEDULED(7, "scheduled", EventSeverity.INFO),
    SUCCEEDED(8, "succeeded", EventSeverity.INFO),
    SUSPENDED(9, "suspend", EventSeverity.INFO),
    RESUMED(10, "resumed", EventSeverity.INFO),
    FAILED(11, "failed", EventSeverity.ERROR),
    SKIPPED(12, "skipped", EventSeverity.INFO),
    KILLED(13, "killed", EventSeverity.ERROR);

    private static final Map<Integer, Events> EVENTS_MAP = new HashMap<>();

    private final int id;
    private final String name;
    private EventSeverity eventSeverity;

    static {
        for (Events events : Events.values()) {
            EVENTS_MAP.put(events.id, events);
        }
    }

    Events(int id, String name, EventSeverity eventSeverity) {
        this.id = id;
        this.name = name;
        this.eventSeverity = eventSeverity;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public EventSeverity getEventSeverity() {
        return eventSeverity;
    }

    public static Events getEvent(Integer id) {
        return EVENTS_MAP.get(id);
    }
}
