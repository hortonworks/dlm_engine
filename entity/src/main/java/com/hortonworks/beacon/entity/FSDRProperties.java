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

/**
 * FileSystem Replication properties.
 */

public enum FSDRProperties {
    JOB_NAME("name", "Name of the replication policy"),
    JOB_FREQUENCY("frequencyInSec", "Frequency of job run"),
    JOB_TYPE("type", "Type of replication policy"),
    START_TIME("startTime", "job start time", false),
    END_TIME("endTime", "job end time", false),
    SOURCE_NN("sourceNN", "Source cluster Namenode", false),
    SOURCE_DATASET("sourceDataset", "Location of source path"),
    SOURCE_CLUSTER_NAME("sourceClusterName", "Source cluster name", false),
    RETRY_ATTEMPTS("retryAttempts", "retry count", false),
    RETRY_DELAY("retryDelay", "retry delay", false),
    QUEUE_NAME("queueName", "queue name", false),

    TARGET_NN("targetNN", "Target cluster Namenode", false),
    TARGET_DATASET("targetDataset", "Location of target path", false),
    TARGET_CLUSTER_NAME("targetClusterName", "Target cluster name", false),

    CLOUD_CRED("cloudCred", "Cloud cred entity", false),
    EXECUTION_TYPE("executionType", "replication execution type"),

    DISTCP_MAX_MAPS("distcpMaxMaps", "Maximum number of maps used during distcp", false),
    DISTCP_MAP_BANDWIDTH_IN_MB("distcpMapBandwidth", "Bandwidth in MB/s used by each mapper during replication", false),

    SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT("sourceSnapshotRetentionAgeLimit",
            "Delete source snapshots older than this age", false),
    SOURCE_SNAPSHOT_RETENTION_NUMBER("sourceSnapshotRetentionNumber",
            "Number of latest source snapshots to retain on source", false),
    TARGET_SNAPSHOT_RETENTION_AGE_LIMIT("targetSnapshotRetentionAgeLimit",
            "Delete target snapshots older than this age", false),
    TARGET_SNAPSHOT_RETENTION_NUMBER("targetSnapshotRetentionNumber",
            "Number of latest target snapshots to retain on source", false),

    SOURCE_SETSNAPSHOTTABLE("source.setSnapshottable", "Source dataset to be marked as snapshottable", false),

    TDE_ENCRYPTION_ENABLED("tde.enabled", "Set to true if TDE encryption is enabled", false),
    TDE_SAMEKEY("tde.sameKey", "Set to true to avoid encryption/decryption of data during replication, "
            + "if same encryption key is used between clusters", false),

    CLOUD_ENCRYPTIONALGORITHM("cloud.encryptionAlgorithm", "Algorithm to encrypt the data on cloud", false),
    CLOUD_ENCRYPTIONKEY("cloud.encryptionKey", "KMS encryption key", false),

    PRESERVE_META("preserve.meta", "Preserve the ACLs/File Permissions to cloud", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    FSDRProperties(String name, String description) {
        this(name, description, true);
    }

    FSDRProperties(String name, String description, boolean isRequired) {
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
