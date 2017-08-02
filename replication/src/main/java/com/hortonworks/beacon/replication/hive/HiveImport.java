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
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Import Hive Replication implementation.
 */
public class HiveImport extends InstanceReplication implements BeaconJob {

    private static final BeaconLog LOG = BeaconLog.getLog(HiveImport.class);

    private Connection targetConnection = null;
    private Statement targetStatement = null;
    private String database;

    public HiveImport(ReplicationJobDetails details) {
        super(details);
        this.database = getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());
        try {
            HiveDRUtils.initializeDriveClass();
            targetConnection = HiveDRUtils.getDriverManagerConnection(getProperties(), HiveActionType.IMPORT);
            targetStatement = targetConnection.createStatement();
        } catch (SQLException sqe) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, sqe.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException(MessageCode.REPL_000018.name(), sqe);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        String dumpDirectory = jobContext.getJobContextMap().get(DUMP_DIRECTORY);
        LOG.info(MessageCode.REPL_000065.name(), dumpDirectory);
        try {
            if (StringUtils.isNotBlank(dumpDirectory)) {
                performImport(dumpDirectory, jobContext);
                LOG.info(MessageCode.REPL_000066.name());
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
            } else {
                throw new BeaconException(MessageCode.COMM_010008.name(), "Repl Dump Directory");
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            LOG.error(MessageCode.REPL_000067.name(), e.getMessage());
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private void performImport(String dumpDirectory, JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.REPL_000068.name(), database);
        ReplCommand replCommand = new ReplCommand(database);
        String replLoad = replCommand.getReplLoad(dumpDirectory);
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException(MessageCode.REPL_000019.name());
            }
            targetStatement.execute(replLoad);
        } catch (BeaconException | SQLException  e) {
            LOG.error(MessageCode.REPL_000069.name(), e);
            throw new BeaconException(e.getMessage());
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        HiveDRUtils.cleanup(targetStatement, targetConnection);
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info(MessageCode.COMM_010012.name(), jobContext.getJobInstanceId());
        boolean isBootStrap = Boolean.parseBoolean(jobContext.getJobContextMap().get(HiveDRUtils.BOOTSTRAP));
        LOG.info(MessageCode.REPL_000070.name(), isBootStrap);
        if (isBootStrap) {
            ReplCommand replCommand = new ReplCommand(database);
            try {
                if (database.equals(HiveDRUtils.DEFAULT)) {
                    //default database can't be dropped, so drop each table.
                    for (String tableDropCommand : replCommand.dropTableList(targetStatement)) {
                        LOG.info(MessageCode.REPL_000071.name(), "table", tableDropCommand);
                        targetStatement.execute(tableDropCommand);
                    }

                    //Drop default database user defined functions
                    for (String functionDropCommand : replCommand.dropFunctionList(targetStatement)) {
                        LOG.info(MessageCode.REPL_000071.name(), "function", functionDropCommand);
                        targetStatement.execute(functionDropCommand);
                    }
                } else {
                    LOG.info(MessageCode.REPL_000072.name(), database);
                    targetStatement.execute(replCommand.dropDatabaseQuery());
                }
            } catch (SQLException e) {
                LOG.error(MessageCode.REPL_000073.name(), e.getMessage());
                throw new BeaconException(e);
            }
        }
        jobContext.setPerformJobAfterRecovery(true);
    }
}
