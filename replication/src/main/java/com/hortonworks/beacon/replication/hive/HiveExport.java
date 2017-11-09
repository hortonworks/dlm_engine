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

import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Export Hive Replication implementation.
 */

public class HiveExport extends InstanceReplication implements BeaconJob  {

    private static final Logger LOG = LoggerFactory.getLogger(HiveExport.class);

    private Connection sourceConnection = null;
    private Connection targetConnection = null;
    private Statement sourceStatement = null;
    private Statement targetStatement = null;
    private String database;


    public HiveExport(ReplicationJobDetails details) {
        super(details);
        database = properties.getProperty(HiveDRProperties.SOURCE_DATASET.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            initializeProperties();
            HiveDRUtils.initializeDriveClass();
            sourceConnection = HiveDRUtils.getDriverManagerConnection(properties, HiveActionType.EXPORT);
            sourceStatement = sourceConnection.createStatement();
            targetConnection = HiveDRUtils.getDriverManagerConnection(properties, HiveActionType.IMPORT);
            targetStatement = targetConnection.createStatement();
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw e;
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException("Exception occurred initializing Hive Server: ", e);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        BeaconLogUtils.createPrefix(jobContext.getJobInstanceId());
        try {
            String dumpDirectory = performExport(jobContext);
            if (StringUtils.isNotBlank(dumpDirectory)) {
                jobContext.getJobContextMap().put(DUMP_DIRECTORY, dumpDirectory);
                LOG.info("Beacon Hive export completed successfully");
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
            } else {
                throw new BeaconException("Repl Dump Directory is null");
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            cleanUp(jobContext);
            throw e;
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            cleanUp(jobContext);
            throw new BeaconException(e);
        } finally{
            BeaconLogUtils.deletePrefix();
        }
    }

    private String performExport(JobContext jobContext) throws BeaconException {
        LOG.info("Performing export for database: {}", database);
        int limit = Integer.parseInt(properties.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        String sourceNN = properties.getProperty(HiveDRProperties.SOURCE_NN.getName());
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

        String dumpDirectory = null;
        ReplCommand replCommand = new ReplCommand(database);
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException("Interrupt occurred...");
            }
            long currReplEventId = 0L;
            long lastReplEventId = replCommand.getReplicatedEventId(targetStatement);
            LOG.debug("Last replicated event id for database: {} is {}", database, lastReplEventId);
            if (lastReplEventId == -1L || lastReplEventId == 0) {
                jobContext.getJobContextMap().put(HiveDRUtils.BOOTSTRAP, "true");
            }
            String replDump = replCommand.getReplDump(lastReplEventId, currReplEventId, limit);
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException("Interrupt occurred...");
            }
            getHiveReplicationProgress(timer, jobContext, HiveActionType.EXPORT,
                    ReplicationUtils.getReplicationMetricsInterval(), sourceStatement);

            ResultSet res = sourceStatement.executeQuery(replDump);
            if (res.next()) {
                dumpDirectory = sourceNN + res.getString(1);
                currReplEventId = Long.parseLong(res.getString(2));
            }

            LOG.info("Source Current Repl Event id : {} , Target Last Repl Event id : {}", currReplEventId,
                lastReplEventId);
            res.close();
        } catch (SQLException e) {
            throw new BeaconException("SQL Exception occurred : ", e);
        } catch (BeaconException e) {
            LOG.error("Exception occurred for export statement", e);
            throw new BeaconException(e.getMessage());
        } finally {
            timer.shutdown();
        }
        return dumpDirectory;
    }


    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        HiveDRUtils.cleanup(sourceStatement, sourceConnection);
        HiveDRUtils.cleanup(targetStatement, targetConnection);
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info("No recovery for hive export job. Instance id [{}]", jobContext.getJobInstanceId());
    }
}
