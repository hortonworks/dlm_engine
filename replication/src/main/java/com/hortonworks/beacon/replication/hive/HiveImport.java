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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClientFactory;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Import Hive Replication implementation.
 */
public class HiveImport extends InstanceReplication implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(HiveImport.class);

    private Connection targetConnection = null;
    private Statement targetStatement = null;
    private String database;
    private HiveMetadataClient hiveMetaDataClient = null;

    public HiveImport(ReplicationJobDetails details) {
        super(details);
        this.database = properties.getProperty(HiveDRProperties.TARGET_DATASET.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        BeaconLogUtils.prefixId(jobContext.getJobInstanceId());
        try {
            initializeProperties();
            HiveDRUtils.initializeDriveClass();
            targetConnection = HiveDRUtils.getTargetConnection(properties);
            targetStatement = targetConnection.createStatement();
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw e;
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException("Exception occurred initializing Hive Server: ", e);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        String dumpDirectory = jobContext.getJobContextMap().get(DUMP_DIRECTORY);
        LOG.info("Location of repl dump directory: {}", dumpDirectory);
        try {
            if (StringUtils.isNotBlank(dumpDirectory)) {
                performImport(dumpDirectory, jobContext);
                LOG.info("Beacon Hive replication successful");
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
            } else {
                throw new BeaconException("Repl Dump Directory is null");
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            LOG.error("Exception occurred while performing import: {}", e.getMessage());
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private void performImport(String dumpDirectory, JobContext jobContext) throws BeaconException {
        LOG.info("Performing import for database: {}", database);
        ReplCommand replCommand = new ReplCommand(database);
        String replLoad = replCommand.getReplLoad(dumpDirectory);
        String configParams =  HiveDRUtils.setConfigParameters(properties);
        if (StringUtils.isNotBlank(configParams)) {
            replLoad += " WITH (" + configParams +")";
        }

        LOG.info("REPL Load statement: {}", replLoad);
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException("Interrupt occurred...");
            }
            getHiveReplicationProgress(timer, jobContext, HiveActionType.IMPORT,
                    ReplicationUtils.getReplicationMetricsInterval(), targetStatement);
            targetStatement.execute(replLoad);
        } catch (BeaconException | SQLException  e) {
            LOG.error("Exception occurred for import statement: ", e);
            throw new BeaconException(e.getMessage());
        } finally {
            LOG.debug("Capturing hive import metrics after job execution");
            jobContext.getJobContextMap().put(BeaconConstants.END_TIME,
                    String.valueOf(System.currentTimeMillis()));
            captureHiveReplicationMetrics(jobContext, HiveActionType.IMPORT, targetStatement);
            timer.shutdown();
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        HiveDRUtils.cleanup(targetStatement, targetConnection);
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info("Recover policy instance: [{}]", jobContext.getJobInstanceId());
        boolean isBootStrap = Boolean.parseBoolean(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP));
        LOG.info("Recovering replication in bootstrap process (true|false): {}", isBootStrap);
        if (isBootStrap) {
            String targetCluster = properties.getProperty(HiveDRProperties.TARGET_CLUSTER_NAME.getName());
            Cluster cluster = ClusterHelper.getActiveCluster(targetCluster);
            hiveMetaDataClient = HiveMetadataClientFactory.getClient(cluster);
            try {
                if (database.equals(HiveDRUtils.DEFAULT)) {
                    //default database can't be dropped, so drop each table.
                    List<String> tables = hiveMetaDataClient.getTables(database);
                    for (String table: tables) {
                        hiveMetaDataClient.dropTable(database, table);
                    }

                    //Drop default database user defined functions
                    List<String> functions = hiveMetaDataClient.getFunctions(database);
                    for (String function: functions) {
                        LOG.info("Drop function: {}", function);
                        hiveMetaDataClient.dropFunction(database, function);
                    }
                } else {
                    LOG.info("Drop database: {}", database);
                    hiveMetaDataClient.dropDatabase(database);
                }
            } catch (BeaconException e) {
                LOG.error("Exception occurred while dropping database in recover bootstrap process: {}",
                    e.getMessage());
                setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
                cleanUp(jobContext);
                throw e;
            } finally {
                hiveMetaDataClient.close();
            }
        }
        jobContext.setPerformJobAfterRecovery(true);
    }
}
