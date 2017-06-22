/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.replication.hive;

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
    SOURCE_HS2_URI("sourceHiveServer2Uri", "source HS2 uri"),
    SOURCE_DATASET("sourceDataset", "Hive Database as source dataset"),
    RETRY_ATTEMPTS("retryAttempts", "retry count", false),
    RETRY_DELAY("retryDelay", "retry delay", false),

    // source hadoop endpoints
    SOURCE_NN("sourceNN", "Source cluster Namenode", false),

    // source security kerberos principals
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal", "Source hiveserver2 kerberos principal", false),
    TARGET_HS2_URI("targetHiveServer2Uri", "target HS2 uri"),

    // target hadoop endpoints
    TARGET_NN("targetNN", "Target cluster Namenode", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal", "Target hiveserver2 kerberos principal", false),

    // Set to true if TDE is enabled
    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Set to true if TDE encryption is enabled", false),

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
