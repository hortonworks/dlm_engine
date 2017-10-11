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
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Export Hive Replication implementation.
 */

public class HiveExport extends InstanceReplication implements BeaconJob  {

    private static final BeaconLog LOG = BeaconLog.getLog(HiveExport.class);

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
            throw new BeaconException(MessageCode.REPL_000018.name(), e, e.getMessage());
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());
        try {
            String dumpDirectory = performExport(jobContext);
            if (StringUtils.isNotBlank(dumpDirectory)) {
                jobContext.getJobContextMap().put(DUMP_DIRECTORY, dumpDirectory);
                LOG.info(MessageCode.REPL_000059.name());
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
            } else {
                throw new BeaconException(MessageCode.COMM_010008.name(), "Repl Dump Directory");
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            cleanUp(jobContext);
            throw e;
        } catch (Exception e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private String performExport(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.REPL_000061.name(), database);
        int limit = Integer.parseInt(properties.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        String sourceNN = properties.getProperty(HiveDRProperties.SOURCE_NN.getName());
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

        String dumpDirectory = null;
        ReplCommand replCommand = new ReplCommand(database);
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException(MessageCode.REPL_000019.name());
            }
            long currReplEventId = 0L;
            long lastReplEventId = replCommand.getReplicatedEventId(targetStatement);
            LOG.debug(MessageCode.REPL_000062.name(), database, lastReplEventId);
            if (lastReplEventId == -1L || lastReplEventId == 0) {
                jobContext.getJobContextMap().put(HiveDRUtils.BOOTSTRAP, "true");
            }
            String replDump = replCommand.getReplDump(lastReplEventId, currReplEventId, limit);
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException(MessageCode.REPL_000019.name());
            }
            getHiveReplicationProgress(timer, jobContext, HiveActionType.EXPORT,
                    ReplicationUtils.getReplicationMetricsInterval(), sourceStatement);

            ResultSet res = sourceStatement.executeQuery(replDump);
            if (res.next()) {
                dumpDirectory = sourceNN + res.getString(1);
                currReplEventId = Long.parseLong(res.getString(2));
            }

            LOG.info(MessageCode.REPL_000092.name(), currReplEventId, lastReplEventId);
            res.close();
        } catch (SQLException e) {
            throw new BeaconException(MessageCode.REPL_000086.name(), e, e.getMessage());
        } catch (BeaconException e) {
            LOG.error(MessageCode.REPL_000093.name(), e);
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
        LOG.info(MessageCode.REPL_000082.name(), jobContext.getJobInstanceId());
    }
}
