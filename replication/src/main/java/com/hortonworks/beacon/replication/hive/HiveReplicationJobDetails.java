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

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class HiveReplicationJobDetails extends ReplicationJobDetails {

    private static final Logger LOG = LoggerFactory.getLogger(HiveReplicationJobDetails.class);

    private String sourceHiveServer2Uri;
    private String targetHiveServer2Uri;
    private String dataBase;
    int maxMaps;
    int mapBandwidth;
    boolean tdeEncryptionEnabled;
    Properties properties;

    public String getSourceHiveServer2Uri() {
        return sourceHiveServer2Uri;
    }

    public void setSourceHiveServer2Uri(String sourceHiveServer2Uri) {
        this.sourceHiveServer2Uri = sourceHiveServer2Uri;
    }
    public String getTargetHiveServer2Uri() {
        return targetHiveServer2Uri;
    }

    public void setTargetHiveServer2Uri(String targetHiveServer2Uri) {
        this.targetHiveServer2Uri = targetHiveServer2Uri;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
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

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public HiveReplicationJobDetails() {
    }

    public HiveReplicationJobDetails(String name, String type, int frequency, Date startTime, Date endTime,
                                     Properties properties, String sourceHiveServer2Uri, String targetHiveServer2Uri,
                                     String dataBase, int maxMaps, int mapBandwidth, boolean tdeEncryptionEnabled) {
        super(name, type, frequency, startTime, endTime);
        this.sourceHiveServer2Uri = sourceHiveServer2Uri;
        this.targetHiveServer2Uri = targetHiveServer2Uri;
        this.dataBase = dataBase;
        this.maxMaps = maxMaps;
        this.mapBandwidth = mapBandwidth;
        this.tdeEncryptionEnabled = tdeEncryptionEnabled;
        this.properties = properties;
    }

    public void validateReplicationProperties(final Properties properties) {
        for (HiveDRProperties option : HiveDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing DR property for Hive Replication : " + option.getName());
            }
        }
    }

    public HiveReplicationJobDetails setReplicationJobDetails(final Properties properties) {
        System.out.println("invoking HiveReplicationJobDetails:");
        System.out.println("name:"+properties.getProperty(HiveDRProperties.JOB_NAME.getName()));
        System.out.println("frequency:"+properties.getProperty(HiveDRProperties.JOB_FREQUENCY.getName()));
        return new HiveReplicationJobDetails(
                properties.getProperty(HiveDRProperties.JOB_NAME.getName()),
                ReplicationType.HIVE.getName(),
                Integer.parseInt(properties.getProperty(HiveDRProperties.JOB_FREQUENCY.getName())),
                DateUtil.parseDate((String) properties.get(HiveDRProperties.START_TIME.getName())),
                DateUtil.parseDate((String) properties.get(HiveDRProperties.END_TIME.getName())),
                properties,
                properties.getProperty(HiveDRProperties.SOURCE_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.TARGET_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()),
                Integer.parseInt(properties.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName())),
                Integer.parseInt(properties.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())),
                Boolean.parseBoolean(properties.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()))
        );
    }

    @Override
    public String toString() {
        return "HiveReplicationJobDetails{" +
                "sourceHiveServer2Uri='" + sourceHiveServer2Uri + '\'' +
                ", targetHiveServer2Uri='" + targetHiveServer2Uri + '\'' +
                ", dataBase='" + dataBase + '\'' +
                ", maxMaps=" + maxMaps +
                ", mapBandwidth=" + mapBandwidth +
                ", tdeEncryptionEnabled=" + tdeEncryptionEnabled +
                ", properties=" + properties +
                '}';
    }
}
