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

package com.hortonworks.beacon.config;


import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;

/**
 * Configuration management class for Beacon.   Responsible for loading and maintaining the beacon
 * configuration from the beacon.yml file.
 */
public final class BeaconConfig {
    private Logger logger = LoggerFactory.getLogger(BeaconConfig.class);
    private static final String BEACON_YML_FILE = "beacon.yml";
    private static final String BEACON_HOME_ENV = "BEACON_HOME";
    private static final String BEACON_HOME_PROP = "beacon.home";

    private static final String BEACON_CONF_ENV = "BEACON_CONF";
    private static final String BEACON_CONF_PROP = "beacon.conf";


    private String beaconHome;
    private String confDir;

    private Engine engine;
    private Store store;

    private BeaconConfig() {
        engine = new Engine();
        store = new Store();
    }

    static {
        BeaconConfig.getInstance().init();
    }

    private static final class Holder {
        private static final BeaconConfig INSTANCE = new BeaconConfig();
    }

    public static BeaconConfig getInstance() {
        return Holder.INSTANCE;
    }

    private static String getParamFromEnvOrProps(String env, String prop, String def) {
        String val = System.getenv(env);
        if (StringUtils.isBlank(val)) {
            val = System.getProperty(prop, def);
        }
        return val;
    }

    private void init() {
        beaconHome = getBeaconHome();
        logger.info("Beacon home set to " + beaconHome);
        confDir = getBeaconConfDir(beaconHome);
        logger.info("Beacon conf set to " + confDir);
        File ymlFile = new File(confDir, BEACON_YML_FILE);
        InputStream resourceAsStream = null;
        Yaml yaml = new Yaml();
        try {

            URL resource = null;
            if (!ymlFile.exists()) {
                logger.warn("beacon properties file " + BEACON_YML_FILE + " does not exist in " + confDir);
                resource = BeaconConfig.class.getResource("/" + BEACON_YML_FILE);
                if (resource != null) {
                    logger.info("Fallback to classpath for: {}", resource);
                    resourceAsStream = BeaconConfig.class.getResourceAsStream("/" + BEACON_YML_FILE);
                } else {
                    resource = BeaconConfig.class.getResource(BEACON_YML_FILE);
                    if (resource != null) {
                        logger.info("Fallback to classpath for: {}", resource);
                        resourceAsStream = BeaconConfig.class.getResourceAsStream(BEACON_YML_FILE);
                    }
                }
            } else {
                resourceAsStream = new FileInputStream(ymlFile);

            }
            BeaconConfig config = null;
            if (resourceAsStream != null) {
                config = yaml.loadAs(resourceAsStream, BeaconConfig.class);
                String localClusterName = null;
                if (config.getEngine() != null) {
                    localClusterName = config.getEngine().getLocalClusterName();
                }
                if (StringUtils.isBlank(localClusterName)) {
                    throw new BeaconException("localClusterName not set for engine in beacon yml file");
                }
                this.getEngine().copy(config.getEngine());
                this.getStore().copy(config.getStore());
            } else {
                logger.warn("No properties file loaded - will use defaults");
            }

        } catch (Exception ioe) {
            logger.warn("Unable to load yaml configuration  : {}", BEACON_YML_FILE, ioe);
        }
    }

    public static String getBeaconHome() {
        String currentDir = System.getProperty("user.dir");
        return getParamFromEnvOrProps(BEACON_HOME_ENV, BEACON_HOME_PROP, currentDir);
    }

    public static String getBeaconConfDir(String beaconHome) {
        String defConf = beaconHome + File.separator + "conf";
        return getParamFromEnvOrProps(BEACON_CONF_ENV, BEACON_CONF_PROP, defConf);
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public Store getStore() {
        return store;
    }

    public void setStore(Store store) {
        this.store = store;
    }
}
