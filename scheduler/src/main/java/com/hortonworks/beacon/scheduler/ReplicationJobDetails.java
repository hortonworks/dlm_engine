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

package com.hortonworks.beacon.scheduler;


import com.hortonworks.beacon.scheduler.hive.HiveDRArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ReplicationJobDetails {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationJobDetails.class);

    private String name;
    private int frequency;
    private String srcHS2URL;
    private String targetHS2URL;
    private String dataBase;
    private String stagingDir;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getSrcHS2URL() {
        return srcHS2URL;
    }

    public void setSrcHS2URL(String srcHS2URL) {
        this.srcHS2URL = srcHS2URL;
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

    public ReplicationJobDetails() {
    }

    public ReplicationJobDetails(String name, int frequency, String srcHS2URL, String targetHS2URL,
                                 String dataBase, String stagingDir) {
        this.name = name;
        this.frequency = frequency;
        this.srcHS2URL = srcHS2URL;
        this.targetHS2URL = targetHS2URL;
        this.dataBase = dataBase;
        this.stagingDir = stagingDir;
    }


    public ReplicationJobDetails setReplicationJobDetails(Properties properties) {
        return new ReplicationJobDetails(
                properties.getProperty(HiveDRArgs.JOB_NAME.getName()),
                Integer.parseInt(properties.getProperty(HiveDRArgs.JOB_FREQUENCY.getName())),
                properties.getProperty(HiveDRArgs.SOURCE_HS2_URI.getName()),
                properties.getProperty(HiveDRArgs.TARGET_HS2_URI.getName()),
                properties.getProperty(HiveDRArgs.SOURCE_DATABASE.getName()),
                properties.getProperty(HiveDRArgs.SOURCE_STAGING_PATH.getName())
        );
    }
}
