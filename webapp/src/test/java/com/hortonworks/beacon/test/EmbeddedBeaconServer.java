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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.store.BeaconStore;
import org.apache.commons.io.FileUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.File;
import java.util.Properties;

public class EmbeddedBeaconServer {

    private static Server server;

    private void startBeaconServer(String configStore, short port, String hostname, String localCluster,
                                   boolean inTestMode) throws Exception {
        FileUtils.deleteDirectory(new File(configStore));
        BeaconConfig config = BeaconConfig.getInstance();
        Engine engine = config.getEngine();
        engine.setConfigStoreUri(configStore);
        engine.setPort(port);
        engine.setHostName(hostname);
        engine.setLocalClusterName(localCluster);
        engine.setInTestMode(inTestMode);

        Connector connector = new SocketConnector();
        connector.setPort(engine.getPort());
        connector.setHost(engine.getHostName());
        connector.setHeaderBufferSize(engine.getSocketBufferSize());
        connector.setRequestBufferSize(engine.getSocketBufferSize());

        server = new Server();
        server.addConnector(connector);
        WebAppContext application = new WebAppContext("../" + engine.getAppPath(), "/");
        application.setParentLoaderPriority(true);
        server.setHandler(application);
        server.start();
        ConfigurationStore.getInstance().init();
        BeaconTestUtil.createDBSchema();
        BeaconQuartzScheduler.get().startScheduler();
        BeaconStore.getInstance().init();
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("properties file missing for embedded beacon server.");
        }

        Properties prop = BeaconTestUtil.getProperties(args[0]);
        EmbeddedBeaconServer server = new EmbeddedBeaconServer();
        server.startBeaconServer(prop.getProperty("beacon.config.store"),
                Short.parseShort(prop.getProperty("beacon.port")),
                prop.getProperty("beacon.host"),
                prop.getProperty("beacon.local.cluster"),
                Boolean.valueOf(prop.getProperty("beacon.test.mode", "false")));
    }
}