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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Export Hive Replication implementation.
 */

public class HiveExport extends InstanceReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HiveExport.class);

    private String database;
    private String sourceConnectionString;
    private String targetConnectionString;


    public HiveExport(ReplicationJobDetails details) {
        super(details);
        database = properties.getProperty(HiveDRProperties.SOURCE_DATASET.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            jobContext.getJobContextMap().put(BeaconConstants.START_TIME, String.valueOf(System.currentTimeMillis()));
            initializeProperties();
            sourceConnectionString = HiveDRUtils.getConnectionString(properties, HiveActionType.EXPORT);
            targetConnectionString = HiveDRUtils.getTargetConnectionString(properties);
        } catch (Exception e) {
            throw new BeaconException("Exception occurred initializing Hive Server: ", e);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException, InterruptedException {
        String dumpDirectory = performExport(jobContext);
        if (StringUtils.isNotBlank(dumpDirectory)) {
            jobContext.getJobContextMap().put(DUMP_DIRECTORY, dumpDirectory);
            LOG.info("Beacon Hive export completed successfully");
        } else {
            throw new BeaconException("Repl Dump Directory is null");
        }
    }

    private String performExport(JobContext jobContext) throws BeaconException, InterruptedException {
        LOG.info("Performing export for database: {}", database);
        int limit = Integer.parseInt(properties.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        String sourceNN = properties.getProperty(HiveDRProperties.SOURCE_NN.getName());
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

        String dumpDirectory = null;
        ReplCommand replCommand = new ReplCommand(database);
        HiveServerClient sourceHiveClient = null;
        HiveServerClient targetHiveClient = null;
        Statement targetStatement = null;
        Statement sourceStatement = null;
        ResultSet res = null;
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new InterruptedException("before repl status");
            }
            long currReplEventId = 0L;

            sourceHiveClient = HiveClientFactory.getHiveServerClient(sourceConnectionString);
            targetHiveClient = HiveClientFactory.getHiveServerClient(targetConnectionString);

            targetStatement = targetHiveClient.createStatement();
            long lastReplEventId = replCommand.getReplicatedEventId(targetStatement, properties);
            LOG.debug("Last replicated event id for database: {} is {}", database, lastReplEventId);
            if (lastReplEventId == -1L || lastReplEventId == 0) {
                jobContext.getJobContextMap().put(HiveDRUtils.BOOTSTRAP, "true");
            }
            String replDump = replCommand.getReplDump(lastReplEventId, currReplEventId, limit);
            if (jobContext.shouldInterrupt().get()) {
                throw new InterruptedException("before repl dump");
            }
            sourceStatement = sourceHiveClient.createStatement();
            getHiveReplicationProgress(timer, jobContext, HiveActionType.EXPORT,
                    ReplicationUtils.getReplicationMetricsInterval(), sourceStatement);

            res = sourceStatement.executeQuery(replDump);
            if (res.next()) {
                dumpDirectory = sourceNN + res.getString(1);
                currReplEventId = Long.parseLong(res.getString(2));
            }

            LOG.info("Source Current Repl Event id : {} , Target Last Repl Event id : {}", currReplEventId,
                lastReplEventId);
        } catch (SQLException e) {
            throw new BeaconException(e, "SQL Exception occurred");
        } catch (BeaconException e) {
            LOG.error("Exception occurred for export statement", e);
            throw new BeaconException(e.getMessage());
        } finally {
            timer.shutdown();
            close(res);
            close(sourceStatement);
            close(targetStatement);
        }
        return dumpDirectory;
    }


    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info("No recovery for hive export job. Instance id [{}]", jobContext.getJobInstanceId());
    }

    @Override
    public void interrupt() throws BeaconException {
        //do nothing, can't interrupt hive replication
    }
}
