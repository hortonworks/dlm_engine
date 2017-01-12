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

import com.google.common.io.Resources;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Store;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class QuartzConfig {

    private static final String QUARTZ_PROP_FILE = "quartz.properties";
    private static final String QUARTZ_PROP_NAME = "org.quartz.properties";
    private static final Logger LOG = LoggerFactory.getLogger(QuartzConfig.class);

    enum QuartzProperties {
        DRIVER("org.quartz.dataSource.beaconDataStore.driver"),
        URL("org.quartz.dataSource.beaconDataStore.URL"),
        USER("org.quartz.dataSource.beaconDataStore.user"),
        PASSWORD("org.quartz.dataSource.beaconDataStore.password"),
        MAX_CONNECTION("org.quartz.dataSource.beaconDataStore.maxConnections");

        String property;
        QuartzProperties(String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }

    static void init() {
        String beaconConfDir = BeaconConfig.getBeaconConfDir(BeaconConfig.getBeaconHome());
        String quartzPropPath = System.getProperty(QUARTZ_PROP_NAME);
        if (StringUtils.isBlank(quartzPropPath)) {
            File quartzPropFile = new File(beaconConfDir, QUARTZ_PROP_FILE);
            quartzPropPath = quartzPropFile.exists()
                    ? quartzPropFile.getAbsolutePath()
                    : Resources.getResource(QUARTZ_PROP_FILE).getPath();
            try {
                // Load the properties into Properties object
                Properties properties = new Properties();
                FileReader reader = new FileReader(quartzPropPath);
                properties.load(reader);
                reader.close();
                Store store = BeaconConfig.getInstance().getStore();
                // Update the database configuration from Store into Quartz properties0
                properties.setProperty(QuartzProperties.DRIVER.getProperty(), store.getDriver());
                // remove the "create=true" from derby database
                if (store.getUrl().contains("jdbc:derby")) {
                    properties.setProperty(QuartzProperties.URL.getProperty(), store.getUrl().split(";")[0]);
                } else {
                    properties.setProperty(QuartzProperties.URL.getProperty(), store.getUrl());
                }
                properties.setProperty(QuartzProperties.USER.getProperty(), store.getUser());
                properties.setProperty(QuartzProperties.PASSWORD.getProperty(), store.getPassword());
                properties.setProperty(QuartzProperties.MAX_CONNECTION.getProperty(), String.valueOf(store.getMaxConnections()));
                // Store the properties configuration back into properties file.
                String parentDir = new File(quartzPropPath).getParent();
                String quartzCurrent = parentDir + File.separator + "quartz_current.properties";
                FileWriter writer = new FileWriter(quartzCurrent);
                properties.store(writer, "");
                System.setProperty(QUARTZ_PROP_NAME, quartzCurrent);
                LOG.info("Quartz properties file to be used [{}]", quartzCurrent);
                writer.flush();
                writer.close();
            } catch (IOException e) {
                LOG.error("Failed to load and update the Beacon Quartz configuration.", e);
                throw new RuntimeException("Failed to load and update the Beacon Quartz configuration.");
            }
        }
    }
}