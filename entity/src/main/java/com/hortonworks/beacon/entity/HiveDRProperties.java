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
 * Hive Replication properties.
 */

public enum HiveDRProperties {
    JOB_NAME("name", "Name of the replication policy"),
    JOB_FREQUENCY("frequencyInSec", "Frequency of job run"),
    JOB_TYPE("type", "Type of replication policy"),
    JOB_ACTION_TYPE("actionType", "Action Type for Hive Replication", false),
    START_TIME("startTime", "job start time", false),
    END_TIME("endTime", "job end time", false),
    SOURCE_CLUSTER_NAME("sourceClusterName", "Source cluster name"),
    TARGET_CLUSTER_NAME("targetClusterName", "Target cluster name"),
    SOURCE_HS2_URI("sourceHiveServer2Uri", "source HS2 uri", false),
    SOURCE_DATASET("sourceDataset", "Hive Database as source dataset"),
    RETRY_ATTEMPTS("retryAttempts", "retry count", false),
    RETRY_DELAY("retryDelay", "retry delay", false),

    // source hadoop endpoints
    SOURCE_NN("sourceNN", "Source cluster Namenode", false),

    // source security kerberos principals
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal", "Source hiveserver2 kerberos principal", false),
    TARGET_HS2_URI("targetHiveServer2Uri", "target HS2 uri", false),

    // target hadoop endpoints
    TARGET_NN("targetNN", "Target cluster Namenode", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal", "Target hiveserver2 kerberos principal", false),

    // Set to true if TDE is enabled
    TDE_ENCRYPTION_ENABLED("tde.enabled", "Set to true if TDE encryption is enabled", false),

    TDE_SAMEKEY("tde.sameKey", "Set to true to avoid encryption/decryption of data during replication, "
            + "if same encryption key is used between clusters", false),

    // Queue name
    QUEUE_NAME("queueName", "queue name", false),

    // num events
    MAX_EVENTS("maxEvents", "number of events to process in this run", false),

    // number of maps
    DISTCP_MAX_MAPS("distcpMaxMaps", "Maximum number of maps used during distcp", false),

    // Map Bandwidth
    DISTCP_MAP_BANDWIDTH_IN_MB("distcpMapBandwidth", "Bandwidth in MB/s used by each mapper during replication", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    HiveDRProperties(String name, String description) {
        this(name, description, true);
    }

    HiveDRProperties(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

}
