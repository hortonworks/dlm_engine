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
import com.hortonworks.beacon.replication.InstanceReplication;
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
public class HiveImport extends InstanceReplication implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(HiveImport.class);

    private Connection targetConnection = null;
    private Statement targetStatement = null;
    private String database;

    public HiveImport(ReplicationJobDetails details) {
        super(details);
        this.database = getProperties().getProperty(HiveDRProperties.SOURCE_DATABASE.getName());
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            HiveDRUtils.initializeDriveClass();
            targetConnection = HiveDRUtils.getDriverManagerConnection(getProperties(), HiveActionType.IMPORT);
            targetStatement = targetConnection.createStatement();
        } catch (SQLException sqe) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, sqe.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException("Exception occurred initializing Hive Server : {}", sqe);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        String dumpDirectory = jobContext.getJobContextMap().get(DUMP_DIRECTORY);
        LOG.info("Location of Repl Dump Directory : {}", dumpDirectory);
        try {
            if (StringUtils.isNotBlank(dumpDirectory)) {
                performImport(dumpDirectory, jobContext);
                LOG.info("Beacon Hive Replication Successful");
                setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS);
            } else {
                throw new BeaconException("Repl Dump Directory is null");
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage());
            LOG.error("Exception occurred while performing Import : {}", e.getMessage());
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    private void performImport(String dumpDirectory, JobContext jobContext) throws BeaconException {
        LOG.info("Performing Import for database : {}", database);
        ReplCommand replCommand = new ReplCommand(database);
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
