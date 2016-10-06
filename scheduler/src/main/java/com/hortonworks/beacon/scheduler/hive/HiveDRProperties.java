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

package com.hortonworks.beacon.scheduler.hive;


public enum HiveDRProperties {
    SOURCE_HS2_URI("sourceHiveServer2Uri", "source HS2 uri"),
    SOURCE_DATABASE("sourceDatabase", "First source database"),
    SOURCE_TABLES("sourceTables", "comma source tables", false),
    STAGING_PATH("stagingPath", "source staging path for data"),

    // source hadoop endpoints
    SOURCE_NN("sourceNN", "source name node", false),

    // source security kerberos principals
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal", "Source hiveserver2 kerberos principal", false),
    TARGET_HS2_URI("targetHiveServer2Uri", "source meta store uri"),

    // target hadoop endpoints
    TARGET_NN("targetNN", "target name node", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal", "Target hiveserver2 kerberos principal", false),

    // num events
    MAX_EVENTS("maxEvents", "number of events to process in this run",false),

    // tuning params
    REPLICATION_MAX_MAPS("replicationMaxMaps", "number of maps", false),
    DISTCP_MAX_MAPS("distcpMaxMaps", "number of maps", false),

    // Set to true if TDE is enabled
    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Set to true if TDE encryption is enabled", false),

    // Map Bandwidth
    DISTCP_MAP_BANDWIDTH("distcpMapBandwidth", "map bandwidth in mb", false),

    JOB_NAME("jobName", "unique job name"),

    JOB_FREQUENCY("jobFrequency","job frequency schedule");

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
