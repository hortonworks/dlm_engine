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
    SOURCE_CLUSTER_NAME("sourceClusterName", "Source cluster name"),
    RETRY_ATTEMPTS("retryAttempts", "retry count", false),
    RETRY_DELAY("retryDelay", "retry delay", false),
    QUEUE_NAME("queueName", "queue name", false),

    TARGET_NN("targetNN", "Target cluster Namenode", false),
    TARGET_DATASET("targetDataset", "Location of target path", false),
    TARGET_CLUSTER_NAME("targetClusterName", "Target cluster name"),

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

    TDE_ENCRYPTION_ENABLED("tde.enabled", "Set to true if TDE encryption is enabled", false),
    TDE_SAMEKEY("tde.sameKey", "Set to true to avoid encryption/decryption of data during replication, "
            + "if same encryption key is used between clusters", false);


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
