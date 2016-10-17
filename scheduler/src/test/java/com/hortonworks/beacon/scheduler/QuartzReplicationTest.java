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

import com.hortonworks.beacon.replication.hdfs.HDFSDRImpl;
import com.hortonworks.beacon.replication.hdfs.HDFSDRProperties;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class QuartzReplicationTest {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzReplicationTest.class);

    private static Properties setHDFSDRProperties() {
        final String [][]props = {
                { "type", "hdfs"},
                { HDFSDRProperties.JOB_NAME.getName(), "test" },
                { HDFSDRProperties.JOB_FREQUENCY.getName(), "60" },
                { HDFSDRProperties.SOURCE_CLUSTER_FS_READ_ENDPOINT.getName(),"hive2://machine-1-1:10000" },
                { HDFSDRProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName(),"hive2://machine-2-1:10000" },
                { HDFSDRProperties.SOURCE_DIR.getName(),"/tmp/dr/staging-target" },
                { HDFSDRProperties.TARGET_DIR.getName(),"/tmp/dr/staging-source" },
                { "tdeEncryptionEnabled", "false"},
                { "distcpMaxMaps", "1"},
                { "distcpMapBandwidth", "10"},
                { "tdeEncryptionEnabled", "false"}
        };


        Properties properties = new Properties();
        for (String[] prop : props) {
            properties.setProperty(prop[0], prop[1]);
        }

        return properties;
    }

    public static void main(String args[]) throws Exception {
        //BeaconClient beaconClient = new BeaconClient();
        //beaconClient.scheduleReplicationJob(setHDFSDRProperties());

        HDFSDRImpl hdimpl = new HDFSDRImpl();
        CommandLine cmd = ReplicationOptionsUtils.getCommand(setHDFSDRProperties());
        System.out.println("Getting distcpmax maps");
        System.out.println(cmd.getArgList());

        for (String o : cmd.getArgs()) {
            System.out.println(o);
        }

        String scfs = cmd.getOptionValue("sourceClusterFS");
        System.out.println(scfs);

    }
}
