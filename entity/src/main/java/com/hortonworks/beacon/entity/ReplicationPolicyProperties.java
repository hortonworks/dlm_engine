package com.hortonworks.beacon.entity;

import java.util.HashSet;
import java.util.Set;

public enum ReplicationPolicyProperties {
    NAME("name", "Name of the replication policy"),
    SOURCELUSTER("sourceCluster", "Source cluster"),
    TARGETCLUSTER("targetCluster", "Target cluster"),
    FREQUENCY("frequencyInSec", "Frequency of job run"),
    TAGS("tags", "Policy tags", false),
    RETRY_DELAY("retryDelay", "Retry delay", false),
    RETRY_ATTEMPTS("retryAttempts", "Retry attempts", false),
    ACL_OWNER("aclOwner", "Job acl owner", false),
    ACL_GROUP("aclGroup", "Job acl group", false),
    ACL_PERMISSION("aclPermission", "Job acl permission", false),
    NOTIFICATION_TYPE("notificationType", "Notification Type", false),
    NOTIFICATION_ADDRESS("notificationReceivers", "Email Notification receivers", false);

    private final String name;
    private final String description;
    private final boolean isRequired;


    private static Set<String> elements = new HashSet<>();
    static {
        for (ReplicationPolicyProperties c : ReplicationPolicyProperties.values()) {
            elements.add(c.getName());
        }
    }

    public static Set<String> getPolicyElements() {
        return elements;
    }

    ReplicationPolicyProperties(String name, String description) {
        this(name, description, true);
    }

    ReplicationPolicyProperties(String name, String description, boolean isRequired) {
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
