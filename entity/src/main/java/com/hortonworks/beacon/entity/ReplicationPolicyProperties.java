package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;

import java.util.HashSet;
import java.util.Set;

public enum ReplicationPolicyProperties {
    NAME(ReplicationPolicyFields.NAME.getName(), "Name of the replication policy"),
    TYPE(ReplicationPolicyFields.TYPE.getName(), "Type of replication policy"),
    DATASET(ReplicationPolicyFields.DATASET.getName(), "Dataset to replicate"),
    SOURCELUSTER(ReplicationPolicyFields.SOURCECLUSTER.getName(), "Source cluster"),
    TARGETCLUSTER(ReplicationPolicyFields.TARGETCLUSTER.getName(), "Target cluster"),
    STARTTIME(ReplicationPolicyFields.STARTTIME.getName(), "Start time of the job", false),
    ENDTIME(ReplicationPolicyFields.ENDTIME.getName(), "End time for the job", false),
    FREQUENCY(ReplicationPolicyFields.FREQUENCYINSEC.getName(), "Frequency of job run"),
    TAGS(ReplicationPolicyFields.TAGS.getName(), "Policy tags", false),
    RETRY_DELAY(ReplicationPolicyFields.RETRYDELAY.getName(), "Retry delay", false),
    RETRY_ATTEMPTS(ReplicationPolicyFields.RETRYATTEMPTS.getName(), "Retry attempts", false),
    ACL_OWNER(ReplicationPolicyFields.ACLOWNER.getName(), "Job acl owner", false),
    ACL_GROUP(ReplicationPolicyFields.ACLGROUP.getName(), "Job acl group", false),
    ACL_PERMISSION(ReplicationPolicyFields.ACLPERMISSION.getName(), "Job acl permission", false),
    NOTIFICATION_TYPE(ReplicationPolicyFields.NOTIFICATIONTYPE.getName(), "Notification Type", false),
    NOTIFICATION_ADDRESS(ReplicationPolicyFields.NOTIFICATIONTO.getName(), "Email Notification receivers", false);

    private final String name;
    private final String description;
    private final boolean isRequired;


    private static Set<String> elements = new HashSet<>();
    static {
        for (ReplicationPolicyProperties c : ReplicationPolicyProperties.values()) {
            elements.add(c.getName().toLowerCase());
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
