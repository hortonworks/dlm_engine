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

package com.hortonworks.beacon.replication.fs;

public enum FSDRProperties {
    JOB_NAME("jobName", "unique job name", true),
    JOB_FREQUENCY("jobFrequency","job frequency schedule", true),
    JOB_TYPE("type", "type of job"),
    START_TIME("startTime", "job start time", false),
    END_TIME("endTime", "job end time", false),
    SOURCE_NN("sourceNN", "Snapshot replication source cluster namenode", true),
    SOURCE_EXEC_URL("sourceExecUrl", "Snapshot replication source execute endpoint", false),
    SOURCE_NN_KERBEROS_PRINCIPAL("sourceNNKerberosPrincipal",
            "Snapshot replication source kerberos principal", false),

    SOURCE_DIR("sourceDir", "Location of source snapshot path", true),

    TARGET_NN("targetNN", "Snapshot replication target cluster namenode", true),
    TARGET_EXEC_URL("targetExecUrl", "Snapshot replication target execute endpoint", false),
    TARGET_NN_KERBEROS_PRINCIPAL("targetNNKerberosPrincipal",
            "Snapshot replication target kerberos principal", false),

    TARGET_DIR("targetDir", "Target Hive metastore uri", true),

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

    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Is TDE encryption enabled on source and target", false);


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