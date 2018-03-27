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
