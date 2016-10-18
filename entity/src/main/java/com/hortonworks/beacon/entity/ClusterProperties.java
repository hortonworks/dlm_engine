package com.hortonworks.beacon.entity;

import java.util.HashSet;
import java.util.Set;

public enum ClusterProperties {
    NAME("name", "Name of the cluster"),
    DESCRIPTION("description", "Description of cluster"),
    DATACENTER("dataCenter", "Data center of cluster", false),
    FS_URI("fsEndpoint", "HDFS Write endpoint"),
    HS_URI("hsEndpoint", "Hive server2 uri", false),
    PEERS("peers", "Clusters paired", false),
    TAGS("tags", "Cluster tags", false),
    ACL_OWNER("aclOwner", "Acl owner", false),
    ACL_GROUP("aclGroup", "Acl group", false),
    ACL_PERMISSION("aclPermission", "Acl permission", false);

    private final String name;
    private final String description;
    private final boolean isRequired;


    private static Set<String> elements = new HashSet<>();
    static {
        for (ClusterProperties c : ClusterProperties.values()) {
            elements.add(c.getName());
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
