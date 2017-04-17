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
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Import Hive Replication implementation.
 */
public class ImportHiveDRImpl extends HiveDRImpl implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(ImportHiveDRImpl.class);

    private Connection targetConnection = null;
    private Statement targetStatement = null;

    public ImportHiveDRImpl(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        LOG.info("Establishing connection to Hive Server:");
        HiveDRUtils.initializeDriveClass();

        try {
            targetConnection = HiveDRUtils.getDriverManagerConnection(getProperties(), HiveActionType.IMPORT);
            targetStatement = targetConnection.createStatement();
        } catch (SQLException sqe) {
            throw new BeaconException("Exception occurred initializing Hive Server : {}", sqe);
        }
    }

    @Override
    public void perform(JobContext jobContext) {
        String dumpDirectory = jobContext.getJobContextMap().get(HiveDRUtils.DUMP_DIRECTORY);
        LOG.info("Location of Repl Dump Directory : {}", dumpDirectory);
        try {
            if (StringUtils.isNotBlank(dumpDirectory)) {
                performImport(dumpDirectory, jobContext);
                getInstanceExecutionDetails().updateJobExecutionDetails(
                        JobStatus.SUCCESS.name(), "Beacon Hive Replication Successful");
            } else {
                throw new BeaconException("Repl Dump Directory is null");
            }
        } catch (BeaconException e) {
            getInstanceExecutionDetails().updateJobExecutionDetails(JobStatus.FAILED.name(), e.getMessage());
            LOG.error("Exception occurred while performing Import : {}", e.getMessage());
        }
    }

    private void performImport(String dumpDirectory, JobContext jobContext) throws BeaconException {
        LOG.info("Performing Import for database : {}", getDatabase());
        ReplCommand replCommand = new ReplCommand(getDatabase());
        String replLoad = replCommand.getReplLoad(dumpDirectory);
        try {
            if (jobContext.shouldInterrupt().get()) {
                throw new BeaconException("Interrupt occurred...");
            }
            targetStatement.execute(replLoad);
        } catch (BeaconException | SQLException  e) {
            LOG.error("Exception occurred for import statement : ", e);
            throw new BeaconException(e.getMessage());
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        HiveDRUtils.cleanup(targetStatement, targetConnection);
    }
}
