package com.hortonworks.beacon.entity;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sramesh on 9/29/16.
 */
public enum ClusterProperties {
    NAME("name", "Name of the cluster"),
    DESCRIPTION("description", "Description of cluster"),
    COLO("colo", "Colo of cluster", false),
    NAMENODE_URI("writeEndpoint", "HDFS Write endpoint"),
    EXECUTE_URI("executeEndpoint", "Execute uri"),
    WF_ENGINE_URI("workflowEngineEndpoint", "WF engine uri"),
    MESSAGING_URI("messagingEndpoint", "Messaging uri", false),
    HS2_URI("hiveserverEndpoint", "HS2 endpoint", false),
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
