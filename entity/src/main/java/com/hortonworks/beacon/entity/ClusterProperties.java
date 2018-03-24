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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Properties for Beacon Cluster resource which specifies optional and required properties.
 */
public enum ClusterProperties {
    NAME(ClusterFields.NAME.getName(), "Name of the cluster"),
    DESCRIPTION(ClusterFields.DESCRIPTION.getName(), "Description of cluster", false),
    FS_ENDPOINT(ClusterFields.FSENDPOINT.getName(), "HDFS Write endpoint", false),
    HS_ENDPOINT(ClusterFields.HSENDPOINT.getName(), "Hive server2 uri", false),
    BEACON_ENDPOINT(ClusterFields.BEACONENDPOINT.getName(), "Beacon server endpoint"),
    ATLAS_ENDPOINT(ClusterFields.ATLASENDPOINT.getName(), "Atlas server endpoint", false),
    RANGER_ENDPOINT(ClusterFields.RANGERENDPOINT.getName(), "Ranger server endpoint", false),
    LOCAL(ClusterFields.LOCAL.getName(), "Local cluster flag", false),
    PEERS(ClusterFields.PEERS.getName(), "Clusters paired", false),
    PEERSINFO(ClusterFields.PEERSINFO.getName(), "Clusters peers Info", false),
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

    public static List<String> updateExclusionProps() {
        List<String> exclusionProps = new ArrayList<>();
        exclusionProps.add(NAME.getName());
        exclusionProps.add(LOCAL.getName());
        exclusionProps.add(PEERS.getName());
        exclusionProps.add(PEERSINFO.getName());
        return exclusionProps;
    }

    @Override
    public String toString() {
        return getName();
    }
}
