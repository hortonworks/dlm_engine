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

import java.util.Properties;

public class QuartzReplicationTest {

    private static Properties setHiveDRProperties() {
        String [][]props = {
                { HiveDRArgs.JOB_NAME.getName(), "test" },
                { HiveDRArgs.JOB_FREQUENCY.getName(), "60" },
                { HiveDRArgs.SOURCE_HS2_URI.getName(),"hive2://machine-1-1:10000" },
                { HiveDRArgs.TARGET_HS2_URI.getName(),"hive2://machine-2-1:10000" },
                { HiveDRArgs.SOURCE_DATABASE.getName(),"default" },
                { HiveDRArgs.SOURCE_STAGING_PATH.getName(),"/tmp/dr/staging" }
        };


        Properties properties = new Properties();
        for (String[] prop : props) {
            properties.setProperty(prop[0], prop[1]);
        }

        return properties;
    }

    public static void main(String args[]) throws Exception {
        QuartzReplication repl = new QuartzReplication();
        repl.createScheduler();

        ReplicationJobDetails details = new ReplicationJobDetails().setReplicationJobDetails(setHiveDRProperties());

        repl.createReplicationJob("test", details);
        repl.scheduleJob(details);
        repl.startScheduler();
        Thread.sleep(10 * 1000);
        repl.stopScheduler();
    }
}
