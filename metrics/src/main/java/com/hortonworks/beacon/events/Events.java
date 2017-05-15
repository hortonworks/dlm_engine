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
    STARTED(0, "started"),
    STOPPED(1, "stopped"),
    SUBMITTED(2, "submitted"),
    DELETED(3, "deleted"),
    PAIRED(4, "paired"),
    SYNCED(6, "synced"),
    SCHEDULED(7, "scheduled"),
    SUCCEEDED(9, "succeeded"),
    FAILED(10, "failed"),
    IGNORED(11, "ignored"),
    KILLED(12, "killed");

    private static final Map<Integer, Events> EVENTS_MAP = new HashMap<>();

    private final int id;
    private final String name;

    static {
        for (Events events : Events.values()) {
            EVENTS_MAP.put(events.id, events);
        }
    }

    Events(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public static Events getEvent(Integer id) {
        return EVENTS_MAP.get(id);
    }
}
