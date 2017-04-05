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
import com.hortonworks.beacon.config.Scheduler;
import com.hortonworks.beacon.config.Store;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * loading quartz.properties and refreshing quartz database config from beacon.yml.
 */
public final class QuartzConfig {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzConfig.class);

    private static final String THREAD_POOL_CLASS_VALUE = "org.quartz.simpl.SimpleThreadPool";
    private static final String JOB_FACTORY_CLASS_VALUE = "org.quartz.simpl.SimpleJobFactory";
    private static final String DRIVER_DELEGATION_CLASS_VALUE = "org.quartz.impl.jdbcjobstore.StdJDBCDelegate";
    private static final String JOB_STORE_CLASS_VALUE = "org.quartz.impl.jdbcjobstore.JobStoreTX";
    private static final String DATA_SOURCE = "beaconDataSource";
    private static final String INSTANCE_ID = "beaconScheduler";

    private Properties properties;

    enum QuartzProperties {
        THREAD_POOL_CLASS("org.quartz.threadPool.class"),
        THREAD_POOL_COUNT("org.quartz.threadPool.threadCount"),
        INSTANCE_ID("org.quartz.scheduler.instanceId"),
        JOB_FACTORY_CLASS("org.quartz.scheduler.jobFactory.class"),
        DRIVER_DELEGATE_CLASS("org.quartz.jobStore.driverDelegateClass"),
        TABLE_PREFIX("org.quartz.jobStore.tablePrefix"),
        DATA_SOURCE("org.quartz.jobStore.dataSource"),
        JOB_STORE_CLASS("org.quartz.jobStore.class"),
        DRIVER("org.quartz.dataSource.beaconDataSource.driver"),
        URL("org.quartz.dataSource.beaconDataSource.URL"),
        USER("org.quartz.dataSource.beaconDataSource.user"),
        PASSWORD("org.quartz.dataSource.beaconDataSource.password"),
        MAX_CONNECTION("org.quartz.dataSource.beaconDataSource.maxConnections");

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
        properties = new Properties();
        init();
    }

    public static QuartzConfig get() {
        return INSTANCE;
    }

    private void init() {
        Scheduler scheduler = BeaconConfig.getInstance().getScheduler();
        Store store = BeaconConfig.getInstance().getStore();
        if (scheduler == null) {
            throw new IllegalStateException("Beacon scheduler configuration is not provided.");
        }
        properties.setProperty(QuartzProperties.THREAD_POOL_CLASS.getProperty(), THREAD_POOL_CLASS_VALUE);
        properties.setProperty(QuartzProperties.JOB_FACTORY_CLASS.getProperty(), JOB_FACTORY_CLASS_VALUE);
        properties.setProperty(QuartzProperties.THREAD_POOL_COUNT.getProperty(), scheduler.getQuartzThreadPool());
        if (StringUtils.isNotBlank(scheduler.getQuartzPrefix())) {
            properties.setProperty(QuartzProperties.TABLE_PREFIX.getProperty(), scheduler.getQuartzPrefix());
            properties.setProperty(QuartzProperties.DRIVER_DELEGATE_CLASS.getProperty(), DRIVER_DELEGATION_CLASS_VALUE);
            properties.setProperty(QuartzProperties.JOB_STORE_CLASS.getProperty(), JOB_STORE_CLASS_VALUE);
            properties.setProperty(QuartzProperties.INSTANCE_ID.getProperty(), INSTANCE_ID);
            properties.setProperty(QuartzProperties.DATA_SOURCE.getProperty(), DATA_SOURCE);
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
                    properties.getProperty(QuartzProperties.URL.getProperty()));
            LOG.info("Beacon quartz scheduler database user: [{}={}]", QuartzProperties.USER.getProperty(),
                    store.getUser());
            properties.setProperty(QuartzProperties.USER.getProperty(), store.getUser());
            properties.setProperty(QuartzProperties.PASSWORD.getProperty(), store.getPassword());
            properties.setProperty(QuartzProperties.MAX_CONNECTION.getProperty(),
                    String.valueOf(store.getMaxConnections()));
        }
    }

    Properties getProperties() {
        return properties;
    }
}
