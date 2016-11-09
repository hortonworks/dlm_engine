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

package com.hortonworks.beacon.replication.hdfs;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class HDFSReplicationJobDetails extends ReplicationJobDetails {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSReplicationJobDetails.class);

    String sourceDir;
    String targetDir;
    String sourceClusterFS;
    String targetClusterFS;
    int distcpMaxMaps;
    int distcpMapBandwidth;
    boolean tdeEncryptionEnabled;
    Properties properties;


    public HDFSReplicationJobDetails() {
    }

    public String getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(String sourceDir) {
        this.sourceDir = sourceDir;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public String getSourceClusterFS() {
        return sourceClusterFS;
    }

    public void setSourceClusterFS(String sourceClusterFS) {
        this.sourceClusterFS = sourceClusterFS;
    }

    public String getTargetClusterFS() {
        return targetClusterFS;
    }

    public void setTargetClusterFS(String targetClusterFS) {
        this.targetClusterFS = targetClusterFS;
    }

    public int getDistcpMaxMaps() {
        return distcpMaxMaps;
    }

    public void setDistcpMaxMaps(int distcpMaxMaps) {
        this.distcpMaxMaps = distcpMaxMaps;
    }

    public int getDistcpMapBandwidth() {
        return distcpMapBandwidth;
    }

    public void setDistcpMapBandwidth(int distcpMapBandwidth) {
        this.distcpMapBandwidth = distcpMapBandwidth;
    }

    public boolean isTdeEncryptionEnabled() {
        return tdeEncryptionEnabled;
    }

    public void setTdeEncryptionEnabled(boolean tdeEncryptionEnabled) {
        this.tdeEncryptionEnabled = tdeEncryptionEnabled;
    }

    public HDFSReplicationJobDetails(String name, String type, int frequency, Date startTime, Date endTime, Properties properties,
                                     String sourceClusterFS, String sourceDir,
                                     String targetClusterFS, String targetDir,
                                     int distcpMapBandwidth, int distcpMaxMaps, boolean tdeEncryptionEnabled) {
        super(name, type, frequency, startTime, endTime);
        this.sourceDir = sourceDir;
        this.targetDir = targetDir;
        this.sourceClusterFS = sourceClusterFS;
        this.targetClusterFS = targetClusterFS;
        this.distcpMapBandwidth = distcpMapBandwidth;
        this.distcpMaxMaps = distcpMaxMaps;
        this.tdeEncryptionEnabled = tdeEncryptionEnabled;
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public HDFSReplicationJobDetails setReplicationJobDetails(Properties properties) {
        return new HDFSReplicationJobDetails(
                properties.getProperty(HDFSDRProperties.JOB_NAME.getName()),
                ReplicationType.HDFS.getName(),
                Integer.parseInt(properties.getProperty(HDFSDRProperties.JOB_FREQUENCY.getName())),
                DateUtil.parseDate((String) properties.get(HDFSDRProperties.START_TIME.getName())),
                DateUtil.parseDate((String) properties.get(HDFSDRProperties.END_TIME.getName())),
                properties,
                properties.getProperty(HDFSDRProperties.SOURCE_CLUSTER_FS_READ_ENDPOINT.getName()),
                properties.getProperty(HDFSDRProperties.SOURCE_DIR.getName()),
                properties.getProperty(HDFSDRProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName()),
                properties.getProperty(HDFSDRProperties.TARGET_DIR.getName()),
                Integer.parseInt(properties.getProperty(HDFSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())),
                Integer.parseInt(properties.getProperty(HDFSDRProperties.DISTCP_MAX_MAPS.getName())),
                Boolean.parseBoolean(properties.getProperty(HDFSDRProperties.TDE_ENCRYPTION_ENABLED.getName()))

        );
    }

    @Override
    public void validateReplicationProperties(Properties properties) {
        for (HDFSDRProperties option : HDFSDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing DR property for HDFS Replication : " + option.getName());
            }
        }
    }

    @Override
    public String toString() {
        return "Running replication job with HDFSReplicationJobDetails{" +
                "name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", frequency='" + getFrequency() + '\'' +
                ", sourceDir='" + sourceDir + '\'' +
                ", targetDir='" + targetDir + '\'' +
                ", sourceClusterFS='" + sourceClusterFS + '\'' +
                ", targetClusterFS='" + targetClusterFS + '\'' +
                ", distcpMaxMaps=" + distcpMaxMaps +
                ", distcpMapBandwidth=" + distcpMapBandwidth +
                ", tdeEncryptionEnabled=" + tdeEncryptionEnabled +
                '}';
    }
}
