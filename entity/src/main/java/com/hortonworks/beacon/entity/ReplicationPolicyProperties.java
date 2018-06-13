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
    SOURCECLUSTER(ReplicationPolicyFields.SOURCECLUSTER.getName(), "Source cluster", false),
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
