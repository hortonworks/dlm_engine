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

package com.hortonworks.beacon.scheduler.quartz;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.config.Scheduler;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.nodes.NodeGenerator;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.service.BeaconService;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * BeaconScheduler API implementation for Quartz.
 */
public final class BeaconQuartzScheduler implements BeaconScheduler, BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconQuartzScheduler.class);

    public static final String START_NODE_GROUP = "0";
    static final String BEACON_SCHEDULER_JOB_LISTENER = "beaconSchedulerJobListener";
    private static final String BEACON_SCHEDULER_TRIGGER_LISTENER = "beaconSchedulerTriggerListener";

    private QuartzScheduler scheduler;

    private static final BeaconQuartzScheduler INSTANCE = new BeaconQuartzScheduler();

    private BeaconQuartzScheduler() {
        scheduler = QuartzScheduler.get();
    }

    public static BeaconQuartzScheduler get() {
        return INSTANCE;
    }

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

        try {
            if (!isStarted()) {
                scheduler.initializeScheduler(new QuartzJobListener(BEACON_SCHEDULER_JOB_LISTENER),
                        new QuartzTriggerListener(BEACON_SCHEDULER_TRIGGER_LISTENER), properties);
                LOG.info("Beacon scheduler initialized successfully.");
                startScheduler();
            } else {
                LOG.info("Instance of the beacon scheduler is already running.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void startScheduler() throws BeaconException {
        try {
            scheduler.startScheduler();
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    // using first job for creating trigger.
    @Override
    public String schedulePolicy(List<ReplicationJobDetails> jobs, boolean recovery, String policyId, Date startTime,
                                 Date endTime, int frequency) throws BeaconException {
        jobs = NodeGenerator.appendNodes(jobs);
        List<JobDetail> jobDetails = QuartzJobDetailBuilder.createJobDetailList(jobs, recovery, policyId);
        Trigger trigger = QuartzTriggerBuilder.createTrigger(policyId, START_NODE_GROUP, startTime, endTime,
                frequency);
        try {
            scheduler.scheduleChainedJobs(jobDetails, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return policyId;
    }

    @Override
    public void stopScheduler() throws BeaconException {
        try {
            if (isStarted()) {
                scheduler.stopScheduler();
                LOG.info("Beacon scheduler shutdown successfully.");
            } else {
                LOG.info("Beacon scheduler is not running.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isStarted() throws BeaconException {
        try {
            return scheduler.isStarted();
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean deletePolicy(String id) throws BeaconException {
        LOG.info("Deleting the scheduled replication entity with id: {}", id);
        try {
            return scheduler.deleteJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void suspendPolicy(String id) throws BeaconException {
        try {
            scheduler.suspendJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void resumePolicy(String id) throws BeaconException {
        try {
            scheduler.resumeJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean abortInstance(String id) throws BeaconException {
        try {
            boolean interrupt = scheduler.interrupt(id, START_NODE_GROUP);
            if (!interrupt) {
                interrupt = SchedulerCache.get().registerInterrupt(id);
            }
            if (!interrupt) {
                throw new BeaconException("Failed to interrupt policy {}", id);
            }
            return interrupt;
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean recoverPolicyInstance(String policyId, String offset, String recoverInstance)
            throws BeaconException {
        try {
            return scheduler.recoverPolicyInstance(policyId, offset, recoverInstance);
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public boolean rerunPolicyInstance(String policyId, String offset, String recoverInstance) throws BeaconException {
        try {
            return scheduler.rerunPolicyInstance(policyId, offset, recoverInstance);
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    @VisibleForTesting
    void clear() throws BeaconException {
        try {
            scheduler.clear();
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    public boolean checkExists(String policyId, String group) throws SchedulerException {
        return scheduler.checkExists(policyId, group);
    }

    @Override
    public void destroy() throws BeaconException {
        try {
            scheduler.stopScheduler();
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }
}
