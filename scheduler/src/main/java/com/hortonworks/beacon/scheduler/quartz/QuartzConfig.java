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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Store;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * loading quartz.properties and refreshing quartz database config from beacon.yml.
 */
public final class QuartzConfig {

    private static final String QUARTZ_PROP_FILE = "quartz.properties";
    private static final String QUARTZ_PROP_NAME = "org.quartz.properties";
    private static final Logger LOG = LoggerFactory.getLogger(QuartzConfig.class);
    private Properties properties;

    enum QuartzProperties {
        JOB_STORE("org.quartz.jobStore.class"),
        DRIVER("org.quartz.dataSource.beaconDataStore.driver"),
        URL("org.quartz.dataSource.beaconDataStore.URL"),
        USER("org.quartz.dataSource.beaconDataStore.user"),
        PASSWORD("org.quartz.dataSource.beaconDataStore.password"),
        MAX_CONNECTION("org.quartz.dataSource.beaconDataStore.maxConnections");

        private String property;

        QuartzProperties(String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }

    private static final QuartzConfig INSTANCE = new QuartzConfig();

    private QuartzConfig() {
        init();
    }

    public static QuartzConfig get() {
        return INSTANCE;
    }

    void init() {
        String beaconConfDir = BeaconConfig.getBeaconConfDir(BeaconConfig.getBeaconHome());
        String quartzPropPath = System.getProperty(QUARTZ_PROP_NAME);
        // Reading quartz configuration file from 'org.quartz.properties' system property
        if (StringUtils.isNotBlank(quartzPropPath)) {
            File quartzPropFile = new File(quartzPropPath);
            quartzPropPath = quartzPropFile.exists()
                    ? quartzPropFile.getAbsolutePath()
                    : QuartzConfig.class.getResource(("/" + quartzPropPath)).getPath();
        } else {
            // Reading the quartz.properties file from beacon conf or classpath
            File quartzPropFile = new File(beaconConfDir, QUARTZ_PROP_FILE);
            quartzPropPath = quartzPropFile.exists()
                    ? quartzPropFile.getAbsolutePath()
                    : QuartzConfig.class.getResource("/" + QUARTZ_PROP_FILE).getPath();
        }
        LOG.info("Quartz properties file to be used [{}]", quartzPropPath);
        try {
            // Load the properties into Properties object
            properties = new Properties();
            FileReader reader = new FileReader(quartzPropPath);
            properties.load(reader);
            reader.close();
            Store store = BeaconConfig.getInstance().getStore();
            LOG.info("Quartz configuration initialized with [{}={}]", QuartzProperties.JOB_STORE.getProperty(),
                    properties.getProperty(QuartzProperties.JOB_STORE.getProperty()));
            if (store.getDriver() != null) {
                // Update the database configuration from Store into Quartz properties0
                LOG.info("Beacon quartz scheduler database driver: [{}={}]", QuartzProperties.DRIVER.getProperty(),
                        store.getDriver());
                properties.setProperty(QuartzProperties.DRIVER.getProperty(), store.getDriver());
                // remove the "create=true" from derby database
                if (store.getUrl().contains("jdbc:derby")) {
                    properties.setProperty(QuartzProperties.URL.getProperty(), store.getUrl().split(";")[0]);
                } else {
                    properties.setProperty(QuartzProperties.URL.getProperty(), store.getUrl());
                }
                LOG.info("Beacon quartz scheduler database url: [{}={}]", QuartzProperties.URL.getProperty(),
                        QuartzProperties.URL.getProperty());
                LOG.info("Beacon quartz scheduler database user: [{}={}]", QuartzProperties.USER.getProperty(),
                        store.getUser());
                properties.setProperty(QuartzProperties.USER.getProperty(), store.getUser());
                properties.setProperty(QuartzProperties.PASSWORD.getProperty(), store.getPassword());
                properties.setProperty(QuartzProperties.MAX_CONNECTION.getProperty(),
                        String.valueOf(store.getMaxConnections()));
            } else {
                LOG.info("Store is not initialized. Database config will not be updated for quartz.");
            }
        } catch (IOException e) {
            LOG.error("Failed to load and update the Beacon Quartz configuration.", e);
            throw new RuntimeException("Failed to load and update the Beacon Quartz configuration.");
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
