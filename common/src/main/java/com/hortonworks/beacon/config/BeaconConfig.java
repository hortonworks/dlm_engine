/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.config;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

public class BeaconConfig {

    private String hostName;
    private Short tlsPort;
    private Short port;
    private String principal;
    private Boolean tlsEnabled;
    private String quartzPrefix;
    private String configStoreUri;
    private String appPath;
    private static final String BUILD_PROPS = "beacon-buildinfo.properties";
    private static final String DEF_VERSION = "1.0-SNAPSHOT";
    Logger LOG = org.slf4j.LoggerFactory.getLogger(BeaconConfig.class);
    Properties buildInfo = new Properties();
    Properties beaconProps = new Properties();
    private static final String BEACON_PROPS_FILE = "beacon.properties";
    private static final String BEACON_HOME_ENV = "BEACON_HOME";
    private static final String BEACON_HOME_PROP = "beacon.home";

    private static final String BEACON_CONF_ENV = "BEACON_CONF";
    private static final String BEACON_CONF_PROP = "beacon.conf";


    private String beaconHome;
    private String confDir;


    private String storeJdbcDriver;
    private String storeJdbcUrl;
    private String storeJdbcUser;
    private String storeJdbcPassword;
    private int storeJdbcMaxConnections;


    private static final class Holder {
        private static BeaconConfig _instance = new BeaconConfig();
    }

    public static BeaconConfig getInstance() {
        return Holder._instance;
    }

    private String getParamFromEnvOrProps(String env, String prop, String def) {
        String val = System.getenv(env);
        if (StringUtils.isBlank(val)) {
            val = System.getProperty(prop, def);
        }
        return val;
    }

    private BeaconConfig() {
        String currentDir = System.getProperty("user.dir");
        beaconHome = getParamFromEnvOrProps(BEACON_HOME_ENV, BEACON_HOME_PROP, currentDir);
        File f = new File(beaconHome);
        LOG.info("Beacon home set to " + beaconProps);
        String defConf = beaconHome + File.pathSeparator + "conf";
        confDir = getParamFromEnvOrProps(BEACON_CONF_ENV, BEACON_CONF_PROP, defConf);
        File dir = new File(confDir);
        LOG.info("Beacon conf set to " + confDir);

        File propsFile = new File(confDir, BEACON_PROPS_FILE);
        InputStream resourceAsStream = null;
        try {

            if (!propsFile.exists()) {
                LOG.warn("beacon properties file " + BEACON_PROPS_FILE + " does not exist in " + confDir);
                URL resource = BeaconConfig.class.getResource("/" + BEACON_PROPS_FILE);
                if (resource != null) {
                    LOG.info("Fallback to classpath for: {}", resource);
                    resourceAsStream = BeaconConfig.class.getResourceAsStream("/" + BEACON_PROPS_FILE);
                } else {
                    resource = BeaconConfig.class.getResource(BEACON_PROPS_FILE);
                    if (resource != null) {
                        LOG.info("Fallback to classpath for: {}", resource);
                        resourceAsStream = BeaconConfig.class.getResourceAsStream(BEACON_PROPS_FILE);
                    }
                }
            } else {
                resourceAsStream = new FileInputStream(propsFile);

            }
            if (resourceAsStream != null) {
                beaconProps.load(resourceAsStream);
            } else {
                LOG.warn("No properties file loaded - will use defaults");
            }

            setHostName(beaconProps.getProperty("engine.host.name", "0.0.0.0"));
            setPort(Short.parseShort(beaconProps.getProperty("engine.port", "25000")));
            setTlsPort(Short.parseShort(beaconProps.getProperty("engine.tlsport", "25443")));
            setPrincipal(beaconProps.getProperty("engine.principal", ""));
            setTlsEnabled(Boolean.getBoolean(beaconProps.getProperty("engine.tls.enabled", "false")));
            setConfigStoreUri(beaconProps.getProperty("beacon.configstore.uri", "/tmp/config-store/"));
            setStoreJdbcDriver(beaconProps.getProperty("beacon.store.jdbc.driver", "org.hsqldb.jdbcDriver"));
            setStoreJdbcUrl(beaconProps.getProperty("beacon.store.jdbc.url", "jdbc:hsqldb:mem:quartz"));
            setStoreJdbcUser(beaconProps.getProperty("beacon.store.jdbc.user", "quartz"));
            setStoreJdbcPassword(beaconProps.getProperty("beacon.store.jdbc.password", "quartz"));
            setStoreJdbcMaxConnections(Integer.parseInt(
                    beaconProps.getProperty("beacon.store.jdbc.max.connectionss", "4")));

            Class cl = BeaconConfig.class;
            URL resource = cl.getResource("/" + BUILD_PROPS);
            if (resource != null) {
                resourceAsStream = cl.getResourceAsStream("/" + BUILD_PROPS);
            } else {
                resource = cl.getResource(BUILD_PROPS);
                if (resource != null) {
                    resourceAsStream = cl.getResourceAsStream(BUILD_PROPS);
                }
            }
            if (resourceAsStream != null) {

                try {
                    buildInfo.load(resourceAsStream);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            String version = (String) buildInfo.get("build.version");
            if (version == null) {
                version = "1.0-SNAPSHOT";
            }
            setAppPath(beaconProps.getProperty("engine.app.path", "webapp/target/beacon-webapp-" + version));
        } catch (Exception ioe) {
            LOG.error("Unable to read : {}", BEACON_PROPS_FILE);
            throw new RuntimeException("Error processing properties file : " + BEACON_PROPS_FILE, ioe);
        }
    }


    public static Properties get() {
        /* TODO : Add logic to read from config file */
        return new Properties();
    }


    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }


    public Short getPort() {
        return port;
    }

    public Short getTlsPort() {
        return tlsPort;
    }


    public void setPort(Short port) {
        this.port = port;
    }

    public void setTlsPort(Short port) {
        this.tlsPort = port;
    }


    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public Boolean getTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(Boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getQuartzPrefix() {
        return quartzPrefix;
    }

    public void setQuartzPrefix(String quartzPrefix) {
        this.quartzPrefix = quartzPrefix;
    }


    public String getConfigStoreUri() {
        return configStoreUri;
    }

    public void setConfigStoreUri(String configStoreUri) {
        this.configStoreUri = configStoreUri;
    }

    public String getAppPath() {
        return appPath;
    }

    public void setAppPath(String appPath) {
        this.appPath = appPath;
    }
    public String getStoreJdbcDriver() {
        return storeJdbcDriver;
    }

    public void setStoreJdbcDriver(String storeJdbcDriver) {
        this.storeJdbcDriver = storeJdbcDriver;
    }

    public String getStoreJdbcUrl() {
        return storeJdbcUrl;
    }

    public void setStoreJdbcUrl(String storeJdbcUrl) {
        this.storeJdbcUrl = storeJdbcUrl;
    }

    public String getStoreJdbcUser() {
        return storeJdbcUser;
    }

    public void setStoreJdbcUser(String storeJdbcUser) {
        this.storeJdbcUser = storeJdbcUser;
    }

    public String getStoreJdbcPassword() {
        return storeJdbcPassword;
    }

    public void setStoreJdbcPassword(String storeJdbcPassword) {
        this.storeJdbcPassword = storeJdbcPassword;
    }

    public int getStoreJdbcMaxConnections() {
        return storeJdbcMaxConnections;
    }

    public void setStoreJdbcMaxConnections(int storeJdbcMaxConnections) {
        this.storeJdbcMaxConnections = storeJdbcMaxConnections;
    }

}
