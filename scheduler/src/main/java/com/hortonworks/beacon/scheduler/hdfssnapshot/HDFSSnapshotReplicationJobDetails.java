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

package com.hortonworks.beacon.scheduler.hdfssnapshot;

import com.hortonworks.beacon.scheduler.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.ReplicationType;

import java.io.IOException;
import java.util.Properties;

public class HDFSSnapshotReplicationJobDetails extends ReplicationJobDetails {

    String sourceNN;
    String sourceSnapshotDir;
    String targetNN;
    String targetSnapshotDir;
    int maxMaps;
    int mapBandwidth;
    boolean tdeEncryptionEnabled;


    public String getSourceNN() {
        return sourceNN;
    }

    public void setSourceNN(String sourceNN) {
        this.sourceNN = sourceNN;
    }

    public String getSourceSnapshotDir() {
        return sourceSnapshotDir;
    }

    public void setSourceSnapshotDir(String sourceSnapshotDir) {
        this.sourceSnapshotDir = sourceSnapshotDir;
    }

    public String getTargetNN() {
        return targetNN;
    }

    public void setTargetNN(String targetNN) {
        this.targetNN = targetNN;
    }

    public String getTargetSnapshotDir() {
        return targetSnapshotDir;
    }

    public void setTargetSnapshotDir(String targetSnapshotDir) {
        this.targetSnapshotDir = targetSnapshotDir;
    }

    public int getMaxMaps() {
        return maxMaps;
    }

    public void setMaxMaps(int maxMaps) {
        this.maxMaps = maxMaps;
    }

    public int getMapBandwidth() {
        return mapBandwidth;
    }

    public void setMapBandwidth(int mapBandwidth) {
        this.mapBandwidth = mapBandwidth;
    }

    public boolean isTdeEncryptionEnabled() {
        return tdeEncryptionEnabled;
    }

    public void setTdeEncryptionEnabled(boolean tdeEncryptionEnabled) {
        this.tdeEncryptionEnabled = tdeEncryptionEnabled;
    }

    public HDFSSnapshotReplicationJobDetails() {
    }

    public HDFSSnapshotReplicationJobDetails(String name, String type, int frequency, String sourceNN,
                                             String sourceSnapshotDir, String targetNN, String targetSnapshotDir,
                                             int maxMaps, int mapBandwidth, boolean tdeEncryptionEnabled) {
        super(name, type, frequency);
        this.sourceNN = sourceNN;
        this.sourceSnapshotDir = sourceSnapshotDir;
        this.targetNN = targetNN;
        this.targetSnapshotDir = targetSnapshotDir;
        this.maxMaps = maxMaps;
        this.mapBandwidth = mapBandwidth;
        this.tdeEncryptionEnabled = tdeEncryptionEnabled;
    }

    @Override
    public HDFSSnapshotReplicationJobDetails setReplicationJobDetails(Properties properties) {
        return new HDFSSnapshotReplicationJobDetails(
                properties.getProperty(HDFSSnapshotDRProperties.JOB_NAME.getName()),
                ReplicationType.HDFSSNAPSHOT.getName(),
                Integer.parseInt(properties.getProperty(HDFSSnapshotDRProperties.JOB_FREQUENCY.getName())),
                properties.getProperty(HDFSSnapshotDRProperties.SOURCE_NN.getName()),
                properties.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName()),
                properties.getProperty(HDFSSnapshotDRProperties.TARGET_NN.getName()),
                properties.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName()),
                Integer.parseInt(properties.getProperty(HDFSSnapshotDRProperties.MAX_MAPS.getName())),
                Integer.parseInt(properties.getProperty(HDFSSnapshotDRProperties.MAP_BANDWIDTH_IN_MB.getName())),
                Boolean.parseBoolean(properties.getProperty(HDFSSnapshotDRProperties.TDE_ENCRYPTION_ENABLED.getName()))
        );

    }

    @Override
    public void validateReplicationProperties(Properties properties) throws IOException {
        for (HDFSSnapshotDRProperties option : HDFSSnapshotDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IOException("Missing DR property for HDFS Replication : " + option.getName());
            }
        }
    }


    @Override
    public String toString() {
        return "Running replication job with HDFSReplicationJobDetails{" +
                "name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", frequency='" + getFrequency() + '\'' +
                ", sourceDir='" + sourceSnapshotDir + '\'' +
                ", targetDir='" + targetSnapshotDir + '\'' +
                ", sourceClusterFS='" + sourceNN + '\'' +
                ", targetClusterFS='" + targetNN + '\'' +
                ", distcpMaxMaps=" + maxMaps +
                ", distcpMapBandwidth=" + mapBandwidth +
                ", tdeEncryptionEnabled=" + tdeEncryptionEnabled +
                '}';
    }
}
