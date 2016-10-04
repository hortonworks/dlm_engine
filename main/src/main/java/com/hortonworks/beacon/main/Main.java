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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.util.config.BeaconConfig;


/**
 * Prevent users from constructing this.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    protected  static Server server;

    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";
    protected static final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);


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

        context.setContextPath("/");

        final boolean tlsEnabled = conf.getTlsEnabled();
        final int port = tlsEnabled ? 25493 : 25000;

        server = new Server(port);

        server.setHandler(context);
        ServletHolder jerseyServlet = context.addServlet(
                org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.classnames",
                ReplicationResource.class.getCanonicalName() +
                "," + EntityResource.class.getCanonicalName());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", tlsEnabled, port);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        server.start();

        /* TODO remove */
        ConfigurationStore.get().init();
    }

}