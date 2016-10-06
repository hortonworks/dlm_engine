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

import com.hortonworks.beacon.scheduler.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.ReplicationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HiveReplicationJobDetails extends ReplicationJobDetails {

    private static final Logger LOG = LoggerFactory.getLogger(HiveReplicationJobDetails.class);

    private String sourceHS2URL;
    private String targetHS2URL;
    private String dataBase;
    private String stagingDir;

    public String getSourceHS2URL() {
        return sourceHS2URL;
    }

    public void setSourceHS2URL(String srcHS2URL) {
        this.sourceHS2URL = sourceHS2URL;
    }
    public String getTargetHS2URL() {
        return targetHS2URL;
    }

    public void setTargetHS2URL(String targetHSURL) {
        this.targetHS2URL = targetHS2URL;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public HiveReplicationJobDetails() {
    }

    public HiveReplicationJobDetails(String name, String type, int frequency, String sourceHS2URL, String targetHS2URL,
                                     String dataBase, String stagingDir) {
        super(name, type, frequency);
        this.sourceHS2URL = sourceHS2URL;
        this.targetHS2URL = targetHS2URL;
        this.dataBase = dataBase;
        this.stagingDir = stagingDir;
    }

    public void validateReplicationProperties(final Properties properties) throws IOException {
        for (HiveDRProperties option : HiveDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IOException("Missing DR property for Hive Replication : " + option.getName());
            }
        }
    }

    public ReplicationJobDetails setReplicationJobDetails(Properties properties) {
        return new HiveReplicationJobDetails(
                properties.getProperty(HiveDRProperties.JOB_NAME.getName()),
                ReplicationType.HIVE.getName(),
                Integer.parseInt(properties.getProperty(HiveDRProperties.JOB_FREQUENCY.getName())),
                properties.getProperty(HiveDRProperties.SOURCE_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.TARGET_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()),
                properties.getProperty(HiveDRProperties.STAGING_PATH.getName())
        );
    }

    @Override
    public String toString() {
        return "Running replication job with HiveReplicationJobDetails {" +
                "name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", frequency='" + getFrequency() + '\'' +
                ", sourceHS2URL='" + sourceHS2URL + '\'' +
                ", targetHS2URL='" + targetHS2URL + '\'' +
                ", dataBase='" + dataBase + '\'' +
                ", stagingDir='" + stagingDir + '\'' +
                '}';
    }
}
