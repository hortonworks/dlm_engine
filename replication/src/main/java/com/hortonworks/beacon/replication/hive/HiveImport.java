/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
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
