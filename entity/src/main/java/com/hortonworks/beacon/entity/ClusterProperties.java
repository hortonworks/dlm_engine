package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;

import java.util.HashSet;
import java.util.Set;

public enum ClusterProperties {
    NAME(ClusterFields.NAME.getName(), "Name of the cluster"),
    DESCRIPTION(ClusterFields.DECRIPTION.getName(), "Description of cluster"),
    DATACENTER(ClusterFields.DATACENTER.getName(), "Data center of cluster", false),
    FS_URI(ClusterFields.FSENDPOINT.getName(), "HDFS Write endpoint"),
    HS_URI(ClusterFields.HSENDPOINT.getName(), "Hive server2 uri", false),
    BEACON_URI(ClusterFields.BEACONENDPOINT.getName(), "Beacon server endpoint"),
    PEERS(ClusterFields.PEERS.getName(), "Clusters paired", false),
    TAGS(ClusterFields.TAGS.getName(), "Cluster tags", false),
    ACL_OWNER(ClusterFields.ACLOWNER.getName(), "Acl owner", false),
    ACL_GROUP(ClusterFields.ACLGROUP.getName(), "Acl group", false),
    ACL_PERMISSION(ClusterFields.ACLPERMISSION.getName(), "Acl permission", false);

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
