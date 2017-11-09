/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.config;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;

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
    private DbStore dbStore;
    private boolean initialized;
    private Scheduler scheduler;

    private BeaconConfig() {
        engine = new Engine();
        dbStore = new DbStore();
        scheduler = new Scheduler();
    }

    private static final class Holder {
        private static final BeaconConfig INSTANCE = new BeaconConfig();
    }

    public static BeaconConfig getInstance() {
        if (!Holder.INSTANCE.initialized) {
            Holder.INSTANCE.init();
        }
        return Holder.INSTANCE;
    }

    private static String getParamFromEnvOrProps(String env, String prop, String def) {
        String val = System.getenv(env);
        if (StringUtils.isBlank(val)) {
            val = System.getProperty(prop, def);
        }
        return val;
    }

    private void init() throws IllegalStateException {
        beaconHome = getBeaconHome();
        logger.info("Beacon home set to {}", beaconHome);
        confDir = getBeaconConfDir(beaconHome);
        logger.info("Beacon conf set to {}", confDir);
        File ymlFile = new File(confDir, BEACON_YML_FILE);
        InputStream resourceAsStream = null;
        Yaml yaml = new Yaml();

        try {
            if (!ymlFile.exists()) {
                logger.warn("Beacon properties file {} does not exist in {}", BEACON_YML_FILE, confDir);
                URL resource = BeaconConfig.class.getResource("/" + BEACON_YML_FILE);
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
            BeaconConfig config;
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
                this.getDbStore().copy(config.getDbStore());
                this.getScheduler().copy(config.getScheduler());
            } else {
                throw new IllegalStateException("No properties file loaded");
            }

        } catch (Exception ioe) {
            throw new IllegalStateException(
                StringFormat.format("Unable to load yaml configuration  : {}", BEACON_YML_FILE), ioe);
        } finally {
            if (resourceAsStream != null) {
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    // Ignore.
                }
            }
        }
        initialized = true;
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

    public DbStore getDbStore() {
        return dbStore;
    }

    public void setDbStore(DbStore store) {
        this.dbStore = store;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
}
