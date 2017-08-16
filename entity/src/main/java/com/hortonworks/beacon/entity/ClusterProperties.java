/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
    LOCAL(ClusterFields.LOCAL.getName(), "Local cluster flag", false),
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
