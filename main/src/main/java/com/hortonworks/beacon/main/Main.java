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

package com.hortonworks.beacon.main;

import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.scheduler.BeaconQuartzScheduler;
import com.hortonworks.beacon.util.config.BeaconConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Prevent users from constructing this.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    protected  static Server server;

    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";


    private Main() {
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option opt;

        opt = new Option(APP_PATH, true, "Application Path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(APP_PORT, true, "Application Port");
        opt.setRequired(false);
        options.addOption(opt);


        return new GnuParser().parse(options, args);
    }

    static class ShutDown extends Thread {
        public void run() {
            try {
                LOG.info("calling shutdown hook");
                if (server != null) {
                    server.stop();
                }
                BeaconQuartzScheduler.get().stopScheduler();
                LOG.info("Shutdown Complete.");
            } catch (Exception e) {
                LOG.error("Server shutdown failed with ", e);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Runtime.getRuntime().addShutdownHook(new ShutDown());
        CommandLine cmd = parseArgs(args);
        BeaconConfig conf = new BeaconConfig();

        if (cmd.hasOption(APP_PATH)) {
            conf.setAppPath(cmd.getOptionValue(APP_PATH));
        }

        LOG.info("App path: {}", conf.getAppPath());

        final boolean tlsEnabled = conf.getTlsEnabled();
        final int port = tlsEnabled ? conf.getTlsPort() : conf.getPort();
        Connector connector = new SocketConnector();
        connector.setPort(port);
        connector.setHost(conf.getHostName());
        connector.setHeaderBufferSize(32768);
        connector.setRequestBufferSize(32768);

        server = new Server();

        server.addConnector(connector);
        WebAppContext application = new WebAppContext(conf.getAppPath(), "/");
        application.setParentLoaderPriority(true);
        server.setHandler(application);
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", tlsEnabled, port);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        server.start();
        /* TODO remove */
        ConfigurationStore.get().init();
        BeaconQuartzScheduler.get().startScheduler();

    }

}
