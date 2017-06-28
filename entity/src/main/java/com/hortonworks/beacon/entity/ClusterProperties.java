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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;

import java.util.HashSet;
import java.util.Set;

/**
 * Properties for Beacon Cluster resource which specifies optional and required properties.
 */
public enum ClusterProperties {
    NAME(ClusterFields.NAME.getName(), "Name of the cluster"),
    DESCRIPTION(ClusterFields.DESCRIPTION.getName(), "Description of cluster", false),
    FS_ENDPOINT(ClusterFields.FSENDPOINT.getName(), "HDFS Write endpoint"),
    HS_ENDPOINT(ClusterFields.HSENDPOINT.getName(), "Hive server2 uri", false),
    BEACON_ENDPOINT(ClusterFields.BEACONENDPOINT.getName(), "Beacon server endpoint"),
    ATLAS_ENDPOINT(ClusterFields.ATLASENDPOINT.getName(), "Atlas server endpoint", false),
    RANGER_ENDPOINT(ClusterFields.RANGERENDPOINT.getName(), "Ranger server endpoint", false),
    PEERS(ClusterFields.PEERS.getName(), "Clusters paired", false),
    TAGS(ClusterFields.TAGS.getName(), "Cluster tags", false),
    USER(ClusterFields.USER.getName(), "User", false);

    private final String name;
    private final String description;
    private final boolean isRequired;


    private static Set<String> elements = new HashSet<>();
    static {
        for (ClusterProperties c : ClusterProperties.values()) {
            elements.add(c.getName().toLowerCase());
        }
    }

    public static Set<String> getClusterElements() {
        return elements;
    }

    ClusterProperties(String name, String description) {
        this(name, description, true);
    }

    ClusterProperties(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Override
    public String toString() {
        return getName();
    }
}
