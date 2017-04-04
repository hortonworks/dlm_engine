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
    BEACON_STARTED("beacon-started", 0),
    BEACON_STOPPED("beacon-stopped", 1),
    CLUSTER_ENTITY_SUBMITTED("cluster-entity-submitted", 2),
    CLUSTER_ENTITY_DELETED("cluster-entity-deleted", 3),
    CLUSTER_ENTITY_PAIRED("cluster-entity-paired", 4),
    POLICY_SUBMITTED("policy-submitted", 5),
    POLICY_SCHEDULED("policy-scheduled", 6),
    POLICY_DELETED("policy-deleted", 7),
    POLICY_INSTANCE_SUCCEEDED("policy-instance-succeeded", 8),
    POLICY_INSTANCE_FAILED("policy-instance-failed", 9),
    POLICY_INSTANCE_IGNORED("policy-instance-ignored", 10),
    POLICY_INSTANCE_KILLED("policy-instance-killed", 11),
    POLICY_INSTANCE_DELETED("policy-instance-deleted", 12);

    private static final Map<Integer, Events> EVENTS_MAP = new HashMap<>();

    private final String name;
    private final int id;

    static {
        for (Events events : Events.values()) {
            EVENTS_MAP.put(events.id, events);
        }
    }

    Events(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public static Events getEvent(Integer id) {
        return EVENTS_MAP.get(id);
    }
}
