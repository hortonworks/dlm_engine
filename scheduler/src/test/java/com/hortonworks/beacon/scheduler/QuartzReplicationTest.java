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

import com.hortonworks.beacon.replication.hive.HiveDRProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class QuartzReplicationTest {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzReplicationTest.class);

    private static Properties setHiveDRProperties() {
        final String [][]props = {
                { "type", "hive"},
                { HiveDRProperties.JOB_NAME.getName(), "test" },
                { HiveDRProperties.JOB_FREQUENCY.getName(), "60" },
                { HiveDRProperties.SOURCE_HS2_URI.getName(),"hive2://machine-1-1:10000" },
                { HiveDRProperties.TARGET_HS2_URI.getName(),"hive2://machine-2-1:10000" },
                { HiveDRProperties.SOURCE_DATABASE.getName(),"default" },
                { HiveDRProperties.STAGING_PATH.getName(),"/tmp/dr/staging" }
        };


        Properties properties = new Properties();
        for (String[] prop : props) {
            properties.setProperty(prop[0], prop[1]);
        }

        return properties;
    }

    public static void main(String args[]) throws Exception {
        BeaconClient beaconClient = new BeaconClient();
        beaconClient.scheduleReplicationJob(setHiveDRProperties());

    }
}
