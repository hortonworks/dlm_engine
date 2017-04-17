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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Export Hive Replication implementation.
 */

public class ExportHiveDRImpl extends HiveDRImpl implements BeaconJob  {

    private static final Logger LOG = LoggerFactory.getLogger(ExportHiveDRImpl.class);

    private Connection sourceConnection = null;
    private Connection targetConnection = null;
    private Statement sourceStatement = null;
    private Statement targetStatement = null;

    private boolean skipImport = false;

    public ExportHiveDRImpl(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        HiveDRUtils.initializeDriveClass();
        try {
            sourceConnection = HiveDRUtils.getDriverManagerConnection(getProperties(), HiveActionType.EXPORT);
            sourceStatement = sourceConnection.createStatement();
            targetConnection = HiveDRUtils.getDriverManagerConnection(getProperties(), HiveActionType.IMPORT);
            targetStatement = targetConnection.createStatement();
        } catch (SQLException sqe) {
            throw new BeaconException("Exception occurred initializing Hive Server : {}", sqe);
        }
    }

    @Override
    public void perform(JobContext jobContext) {
        try {
            String dumpDirectory = performExport(jobContext);
            if (StringUtils.isNotBlank(dumpDirectory)) {
                if (skipImport) {
                    getInstanceExecutionDetails().updateJobExecutionDetails(JobStatus.IGNORED.name(),
                            "Current Repl event id must be greater than last repl event id");
                } else {
                    jobContext.getJobContextMap().put(HiveDRUtils.DUMP_DIRECTORY, dumpDirectory);
                    getInstanceExecutionDetails().updateJobExecutionDetails(JobStatus.SUCCESS.name(),
                            "Beacon Hive Export completed successfully");
                }
            } else {
                throw new BeaconException("Repl Dump Directory is null");
            }
        } catch (BeaconException e) {
            getInstanceExecutionDetails().updateJobExecutionDetails(JobStatus.FAILED.name(), e.getMessage());
            LOG.error("Exception occurred while performing Export : {}", e.getMessage());
        }
    }

    private String performExport(JobContext jobContext) throws BeaconException {
        LOG.info("Performing Export for database : {}", getDatabase());
        int limit = Integer.parseInt(getProperties().getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        String sourceNN = getProperties().getProperty(HiveDRProperties.SOURCE_NN.getName());

        String dumpDirectory = null;
        ReplCommand replCommand = new ReplCommand(getDatabase());
        try {
            long currReplEventId = 0L;
            long lastReplEventId = replCommand.getReplicatedEventId(targetStatement);
            String replDump = replCommand.getReplDump(lastReplEventId, currReplEventId, limit);
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException("Interrupt occurred...");
            }
            try (ResultSet res = sourceStatement.executeQuery(replDump)) {
                if (res.next()) {
                    dumpDirectory = sourceNN + res.getString(1);
                    currReplEventId = Long.parseLong(res.getString(2));
                }
                verifyReplEventId(currReplEventId, lastReplEventId);
            }

        } catch (BeaconException | SQLException e) {
            LOG.error("Exception occurred for export statement :", e);
            throw new BeaconException(e.getMessage());
        }

        return dumpDirectory;
    }


    private void verifyReplEventId(long currReplEventId, long lastReplEventId) {
        LOG.info("Source Current Repl Event id : {} , Target Last Repl Event id : {}",
                currReplEventId, lastReplEventId);
        if (currReplEventId <= lastReplEventId) {
            String msg = "Current Repl Event Id: "+currReplEventId+" must be greater "
                    + "than last Repl Event Id: "+lastReplEventId;
            LOG.info(msg);
            skipImport = true;
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        HiveDRUtils.cleanup(sourceStatement, sourceConnection);
        HiveDRUtils.cleanup(targetStatement, targetConnection);
    }
}
