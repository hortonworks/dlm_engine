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

import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;

import java.util.HashSet;
import java.util.Set;

/**
 * Properties for Beacon ReplicationPolicy resource which specifies optional and required properties.
 */
public enum ReplicationPolicyProperties {
    NAME(ReplicationPolicyFields.NAME.getName(), "Name of the replication policy"),
    TYPE(ReplicationPolicyFields.TYPE.getName(), "Type of replication policy"),
    DESCRIPTION(ReplicationPolicyFields.DESCRIPTION.getName(), "Description of the replication policy", false),
    SOURCEDATASET(ReplicationPolicyFields.SOURCEDATASET.getName(), "Dataset to replicate"),
    TARGETDATASET(ReplicationPolicyFields.TARGETDATASET.getName(), "Dataset to replicate", false),
    SOURCELUSTER(ReplicationPolicyFields.SOURCECLUSTER.getName(), "Source cluster", false),
    TARGETCLUSTER(ReplicationPolicyFields.TARGETCLUSTER.getName(), "Target cluster", false),
    STARTTIME(ReplicationPolicyFields.STARTTIME.getName(), "Start time of the job", false),
    ENDTIME(ReplicationPolicyFields.ENDTIME.getName(), "End time for the job", false),
    FREQUENCY(ReplicationPolicyFields.FREQUENCYINSEC.getName(), "Frequency of job run"),
    TAGS(ReplicationPolicyFields.TAGS.getName(), "Policy tags", false),
    RETRY_DELAY(ReplicationPolicyFields.RETRYDELAY.getName(), "Retry delay", false),
    RETRY_ATTEMPTS(ReplicationPolicyFields.RETRYATTEMPTS.getName(), "Retry attempts", false),
    USER(ReplicationPolicyFields.USER.getName(), "User name", false),
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
