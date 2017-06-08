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

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.main.Main;

import java.util.Properties;

/**
 * Embedded beacon server.
 */
public class EmbeddedBeaconServer {

    private void startBeaconServer(int port, String localCluster) throws Exception {
        BeaconTestUtil.createDBSchema();
        Main.main(new String[] {"-port", String.valueOf(port), "-localcluster", localCluster, });
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("properties file missing for embedded beacon server.");
        }

        Properties prop = BeaconTestUtil.getProperties(args[0]);
        EmbeddedBeaconServer embeddedBeaconServer = new EmbeddedBeaconServer();
        embeddedBeaconServer.startBeaconServer(
                Integer.parseInt(prop.getProperty("beacon.port")),
                prop.getProperty("beacon.local.cluster"));
    }
}
