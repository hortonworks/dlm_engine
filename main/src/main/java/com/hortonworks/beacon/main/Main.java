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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.SchedulerStartService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.store.BeaconStoreService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.security.PrivilegedAction;
import javax.security.auth.Subject;


/**
 * Driver for running Beacon as a standalone server.
 */
public final class Main {

    private static final BeaconLog LOG = BeaconLog.getLog(Main.class);

    private static Server server;
    private static final List<String> DEFAULT_SERVICES = new ArrayList<String>() {
        {
            add(SchedulerInitService.SERVICE_NAME);
            add(BeaconStoreService.SERVICE_NAME);
            // ResourceBundleService is to access the resourceBundle which is
            // accessed by all modules for i18n. This should be the last entry.
            add(ResourceBundleService.SERVICE_NAME);
        }
    };

    private static final List<String> DEPENDENT_SERVICES = new ArrayList<String>() {
        {
            add(SchedulerStartService.SERVICE_NAME);
        }
    };

    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";
    private static final String LOCAL_CLUSTER = "localcluster";

    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    private static final String BEACON_KERBEROS_AUTH_ENABLED="beacon.kerberos.authentication.enabled";
    private static final String BEACON_AUTH_TYPE = "beacon.kerberos.authentication.type";
    private static final String NAME_RULES = "beacon.kerberos.namerules.auth_to_local";
    private static final String PRINCIPAL = "beacon.kerberos.spnego.principal";
    private static final String KEYTAB = "beacon.kerberos.spnego.keytab";
    private static final String KERBEROS_TYPE = "kerberos";
    private static final String BEACON_USER_PRINCIPAL = "beacon.kerberos.principal";
    private static final String BEACON_USER_KEYTAB = "beacon.kerberos.keytab";
    private static final String DEFAULT_NAME_RULE = "DEFAULT";
    private static final String AUTH_TYPE_KERBEROS = "kerberos";

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

        opt = new Option(LOCAL_CLUSTER, true, "Cluster to run");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    static class ShutDown extends Thread {
        public void run() {
            try {
                LOG.info(MessageCode.MAIN_000076.name());
                if (server != null) {
                    BeaconEvents.createEvents(Events.STOPPED, EventEntityType.SYSTEM);
                    server.stop();
                }
                ServiceManager.getInstance().destroy();
                LOG.info(MessageCode.MAIN_000077.name());
            } catch (Exception e) {
                LOG.error(MessageCode.MAIN_000078.name(), e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new ShutDown());
        CommandLine cmd = parseArgs(args);
        BeaconConfig conf = BeaconConfig.getInstance();
        Engine engine = conf.getEngine();
        if (cmd.hasOption(APP_PATH)) {
            engine.setAppPath(cmd.getOptionValue(APP_PATH));
        }
        if (cmd.hasOption(LOCAL_CLUSTER)) {
            engine.setLocalClusterName(cmd.getOptionValue(LOCAL_CLUSTER));
        }

        if (cmd.hasOption(APP_PORT)) {
            engine.setPort(Integer.parseInt(cmd.getOptionValue(APP_PORT)));
        }
        BeaconLogUtils.setLogInfo(System.getProperty("user.name"), engine.getLocalClusterName());
        LOG.info(MessageCode.MAIN_000079.name(), engine.getAppPath());
        LOG.info(MessageCode.MAIN_000080.name(), engine.getLocalClusterName());

        final boolean tlsEnabled = engine.getTlsEnabled();
        final int port = tlsEnabled ? engine.getTlsPort() : engine.getPort();
        Connector connector = new SocketConnector();
        connector.setPort(port);
        connector.setHost(engine.getHostName());
        connector.setHeaderBufferSize(engine.getSocketBufferSize());
        connector.setRequestBufferSize(engine.getSocketBufferSize());

        server = new Server();

        server.addConnector(connector);
        WebAppContext application = new WebAppContext(engine.getAppPath(), "/");
        application.setParentLoaderPriority(true);
        server.setHandler(application);
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info(MessageCode.MAIN_000081.name(), tlsEnabled, port);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        ServiceManager.getInstance().initialize(DEFAULT_SERVICES, DEPENDENT_SERVICES);
        if (isSpnegoEnable()) {
            String keytab = AUTHCONFIG.getProperty(BEACON_USER_KEYTAB);
            String principal = null;
            try {
                principal = SecureClientLogin.getPrincipal(AUTHCONFIG.getProperty(BEACON_USER_PRINCIPAL),
                        BeaconConfig.getInstance().getEngine().getHostName());
            } catch (IOException ignored) {
                LOG.warn("Failed to get beacon.kerberos.principal. Reason: " + ignored.toString());
            }
            String nameRules = AUTHCONFIG.getProperty(NAME_RULES);
            if (StringUtils.isBlank(nameRules)) {
                LOG.info("Name is empty. Setting Name Rule as 'DEFAULT'");
                nameRules = DEFAULT_NAME_RULE;
            }
            if (AUTHCONFIG.getProperty(BEACON_AUTH_TYPE) != null
                    && AUTHCONFIG.getProperty(BEACON_AUTH_TYPE).trim().equalsIgnoreCase(AUTH_TYPE_KERBEROS)
                    && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
                try{
                    LOG.info("Provided Kerberos Credential : Principal = "
                            + principal + " and Keytab = " + keytab);
                    Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
                    Subject.doAs(sub, new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            LOG.info("Starting Jetty Server using kerberos credential");
                            try {
                                server.start();
                            } catch (Exception e) {
                                LOG.error("Jetty Server failed to start:" + e.toString());
                            }
                            return null;
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Jetty Server failed to start:" + e.toString(), e);
                }
            } else {
                server.start();
            }
        } else {
            server.start();
        }

        BeaconEvents.createEvents(Events.STARTED, EventEntityType.SYSTEM);
    }

    private static boolean isSpnegoEnable() {
        boolean isKerberos = AUTHCONFIG.getBooleanProperty(BEACON_KERBEROS_AUTH_ENABLED, false);
        if (isKerberos && KERBEROS_TYPE.equalsIgnoreCase(AUTHCONFIG.getProperty(BEACON_AUTH_TYPE))) {
            return isKerberos;
        }
        if (isKerberos) {
            isKerberos = false;
            String keytab = AUTHCONFIG.getProperty(KEYTAB);
            String principal="*";
            try {
                principal = SecureClientLogin.getPrincipal(AUTHCONFIG.getProperty(PRINCIPAL),
                        BeaconConfig.getInstance().getEngine().getHostName());
            } catch (IOException e) {
                LOG.error("Unable to read principal:" + e.toString());
            }
            String hostname = BeaconConfig.getInstance().getEngine().getHostName();
            if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(principal)
                    && StringUtils.isNotEmpty(hostname)) {
                isKerberos = true;
            }
        }
        return isKerberos;
    }
}
