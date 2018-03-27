/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.config.Scheduler;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Beacon scheduler initialization service.
 * Scheduler will be in the stand by mode and needs to be started with {@link SchedulerStartService}.
 * The service depends on the DB to be setup with Quartz tables.
 */
public final class SchedulerInitService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerInitService.class);
    public static final String SERVICE_NAME = SchedulerInitService.class.getName();
    private static final SchedulerInitService INSTANCE = new SchedulerInitService();

    private static final String THREAD_POOL_CLASS_VALUE = "org.quartz.simpl.SimpleThreadPool";
    private static final String JOB_FACTORY_CLASS_VALUE = "org.quartz.simpl.SimpleJobFactory";
    private static final String DRIVER_DELEGATION_CLASS_VALUE = "org.quartz.impl.jdbcjobstore.StdJDBCDelegate";
    private static final String DRIVER_DELEGATION_CLASS_POSTGRESQL = "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate";
    private static final String JOB_STORE_CLASS_VALUE = "org.quartz.impl.jdbcjobstore.JobStoreTX";
    private static final String DATA_SOURCE = "beaconDataSource";
    private static final String INSTANCE_ID = "beaconScheduler";

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
        MAX_CONNECTION("org.quartz.dataSource.beaconDataSource.maxConnections"),
        VALIDATION_QUERY("org.quartz.dataSource.beaconDataSource.validationQuery");

        private String property;

        QuartzProperties(String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }

    private BeaconQuartzScheduler scheduler;

    private SchedulerInitService() {
        scheduler = BeaconQuartzScheduler.get();
    }

    public BeaconQuartzScheduler getScheduler() {
        return scheduler;
    }

    public static SchedulerInitService get() {
        return INSTANCE;
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        BeaconConfig beaconConfig = BeaconConfig.getInstance();
        DbStore dbStore = beaconConfig.getDbStore();
        Scheduler schedulerConfig = beaconConfig.getScheduler();
        if (schedulerConfig == null) {
            throw new IllegalStateException("Beacon scheduler configuration is not provided.");
        }
        Properties properties = new Properties();
        properties.setProperty(QuartzProperties.THREAD_POOL_CLASS.getProperty(), THREAD_POOL_CLASS_VALUE);
        properties.setProperty(QuartzProperties.JOB_FACTORY_CLASS.getProperty(), JOB_FACTORY_CLASS_VALUE);
        properties.setProperty(QuartzProperties.THREAD_POOL_COUNT.getProperty(), schedulerConfig.getQuartzThreadPool());
        if (StringUtils.isNotBlank(schedulerConfig.getQuartzPrefix())) {
            properties.setProperty(QuartzProperties.TABLE_PREFIX.getProperty(), schedulerConfig.getQuartzPrefix());
            properties.setProperty(QuartzProperties.JOB_STORE_CLASS.getProperty(), JOB_STORE_CLASS_VALUE);
            properties.setProperty(QuartzProperties.INSTANCE_ID.getProperty(), INSTANCE_ID);
            properties.setProperty(QuartzProperties.DATA_SOURCE.getProperty(), DATA_SOURCE);
            LOG.info("Beacon quartz scheduler database driver: [{}={}]", QuartzProperties.DRIVER.getProperty(),
                    dbStore.getDriver());
            properties.setProperty(QuartzProperties.DRIVER.getProperty(), dbStore.getDriver());

            // remove the "create=true" from derby database
            DbStore.DBType dbType = dbStore.getDBType();
            if (dbType == DbStore.DBType.DERBY) {
                properties.setProperty(QuartzProperties.URL.getProperty(), dbStore.getUrl().split(";")[0]);
            } else {
                properties.setProperty(QuartzProperties.URL.getProperty(), dbStore.getUrl());
            }

            if (dbType == DbStore.DBType.POSTGRESQL) {
                properties.setProperty(
                        QuartzProperties.DRIVER_DELEGATE_CLASS.getProperty(), DRIVER_DELEGATION_CLASS_POSTGRESQL);
            } else {
                properties.setProperty(
                        QuartzProperties.DRIVER_DELEGATE_CLASS.getProperty(), DRIVER_DELEGATION_CLASS_VALUE);
            }

            LOG.info("Beacon quartz scheduler database url: [{}={}]", QuartzProperties.URL.getProperty(),
                    properties.getProperty(QuartzProperties.URL.getProperty()));
            LOG.info("Beacon quartz scheduler database user: [{}={}]", QuartzProperties.USER.getProperty(),
                    dbStore.getUser());
            properties.setProperty(QuartzProperties.USER.getProperty(), dbStore.getUser());
            properties.setProperty(
                    QuartzProperties.PASSWORD.getProperty(), dbStore.resolvePassword());
            properties.setProperty(QuartzProperties.MAX_CONNECTION.getProperty(),
                    String.valueOf(dbStore.getMaxConnections()));
            if (dbStore.isValidateDbConn()) {
                properties.setProperty(QuartzProperties.VALIDATION_QUERY.getProperty(),
                        BeaconConstants.VALIDATION_QUERY);
            }
        }

        if (scheduler != null && !scheduler.isStarted()) {
            scheduler.initializeScheduler(properties);
        }
    }

    @Override
    public void destroy() throws BeaconException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.stopScheduler();
        }
    }
}
