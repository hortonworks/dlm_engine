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
    SYNCED(6, "synced", EventSeverity.INFO),
    SCHEDULED(7, "scheduled", EventSeverity.INFO),
    SUCCEEDED(9, "succeeded", EventSeverity.INFO),
    FAILED(10, "failed", EventSeverity.ERROR),
    IGNORED(11, "ignored", EventSeverity.INFO),
    KILLED(12, "killed", EventSeverity.ERROR);

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
