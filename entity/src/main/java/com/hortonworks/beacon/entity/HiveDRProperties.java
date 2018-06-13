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
    TARGET_DATASET("targetDataset", "Hive Database as target dataset"),
    RETRY_ATTEMPTS("retryAttempts", "retry count", false),
    RETRY_DELAY("retryDelay", "retry delay", false),

    // source hadoop endpoints
    SOURCE_NN("sourceNN", "Source cluster Namenode", false),

    // source security kerberos principals
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal", "Source hiveserver2 kerberos principal", false),
    SOURCE_HMS_KERBEROS_PRINCIPAL("source.hive.metastore.kerberos.principal", "Source HMS kerberos principal", false),
    SOURCE_HIVE_SERVER_AUTHENTICATION("source.hive.server.authentication", "Source hiveserver2  authentication",
            false),

    TARGET_HS2_URI("targetHiveServer2Uri", "target HS2 uri", false),

    // target hadoop endpoints
    TARGET_NN("targetNN", "Target cluster Namenode", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal", "Target hiveserver2 kerberos principal", false),
    TARGET_HIVE_SERVER_AUTHENTICATION("target.hive.server.authentication", "Target hiveserver2  authentication",
            false),

    TARGET_HMS_KERBEROS_PRINCIPAL("target.hive.metastore.kerberos.principal", "Target HMS kerberos principal", false),

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
