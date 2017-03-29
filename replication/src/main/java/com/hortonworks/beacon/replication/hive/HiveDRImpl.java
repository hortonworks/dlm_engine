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

import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Hive Replication implementation.
 */

public class HiveDRImpl implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDRImpl.class);

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";

    private Properties properties = null;
    private String sourceHiveServerURL;
    private String targetHiveServerURL;
    private String database;

    private String sourceKerberosCredential;
    private String targetKerberosCredential;

    private Connection sourceConnection = null;
    private Connection targetConnection = null;

    private Statement sourceStatement = null;
    private Statement targetStatement = null;

    private InstanceExecutionDetails instanceExecutionDetails;
    private String replPolicyExecutionType;


    public HiveDRImpl(ReplicationJobDetails details) {
        properties = details.getProperties();
        instanceExecutionDetails = new InstanceExecutionDetails();
        replPolicyExecutionType = details.getProperties().getProperty(PolicyHelper.INSTANCE_EXECUTION_TYPE);
    }

    public InstanceExecutionDetails getInstanceExecutionDetails() {
        return instanceExecutionDetails;
    }

    public void setInstanceExecutionDetails(InstanceExecutionDetails instanceExecutionDetails) {
        this.instanceExecutionDetails = instanceExecutionDetails;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        LOG.info("Establishing connection to Hive Server:");

        try {
            Class.forName(DRIVER_NAME);
            DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
        } catch (ClassNotFoundException e) {
            LOG.error("{} not found : ", DRIVER_NAME);
            e.printStackTrace();
            System.exit(1);
        }

        String authTokenString = ";auth=delegationToken";
        //To bypass findbugs check, need to store empty password in Properties.
        Properties password = new Properties();
        password.put("password", "");
        String user = "";

        String authString = null;
        try {
            String connString = getSourceHS2ConnectionUrl(authString);
            sourceConnection = DriverManager.getConnection(connString, user, password.getProperty("password"));
            sourceStatement = sourceConnection.createStatement();
        } catch (SQLException sqe) {
            LOG.error("Exception occurred initializing source Hive Server : {}", sqe);
        }

        try {
            String connString = getTargetHS2ConnectionUrl(authString);
            targetConnection = DriverManager.getConnection(connString, user, password.getProperty("password"));
            targetStatement = targetConnection.createStatement();
        } catch (SQLException sqe) {
            LOG.error("Exception occurred initializing target Hive Server : {}", sqe);
        }
    }

    private String getSourceHS2ConnectionUrl(final String authTokenString) {
        return getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.SOURCE_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()), authTokenString);
    }

    public static String getHS2ConnectionUrl(final String hs2Uri, final String database,
                                             final String authTokenString) {
        StringBuilder connString = new StringBuilder();
        connString.append(JDBC_PREFIX).append(StringUtils.removeEnd(hs2Uri, "/")).append("/").append(database);

        if (StringUtils.isNotBlank(authTokenString)) {
            connString.append(authTokenString);
        }

        LOG.info("getHS2ConnectionUrl connection uri: {}", connString);
        return connString.toString();
    }

    private String getTargetHS2ConnectionUrl(final String authTokenString) {
        return getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.TARGET_HS2_URI.getName()),
                properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()), authTokenString);

    }

    @Override
    public void perform(JobContext jobContext) {
        database = properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName());
        instanceExecutionDetails.setJobExecutionType(replPolicyExecutionType);
        LOG.info("Prepare Hive Replication on source");
        String dumpDirectory = prepareReplication();
        if (StringUtils.isNotBlank(dumpDirectory)) {
            LOG.info("Pull Replication on target");
            pullReplication(dumpDirectory);
        } else {
            LOG.info("Dump directory is null. Stopping Hive Replication");
            instanceExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(),
                    "Repl Dump Directory is null");
        }

        instanceExecutionDetails.updateJobExecutionDetails(
                JobStatus.SUCCESS.name(), "Copy Successful");
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
    }

    @Override
    public String getJobExecutionContextDetails() throws BeaconException {
        LOG.info("Job status after replication : {}", getInstanceExecutionDetails().toJsonString());
        return getInstanceExecutionDetails().toJsonString();
    }

    private String prepareReplication() {
        LOG.info("Performing Export for database with table : {}", database);
        ResultSet res;
        String dumpDirectory = null;
        String lastReplEventId = "0";          // for bootstrap
        String currReplEventId = "0";

        try {
            /*
            String replStatus = HiveDRUtils.getReplStatus(database);
            LOG.info("Running REPL Status statement on source: {}", replStatus);
            res = sourceStatement.executeQuery(replStatus);
            if (res.next()) {
                LOG.info("ResultSet STATUS Output String : {}" + res.getString(1));
                currReplEventId = res.getString(1).split("\u0001")[1];
                //lastEventId = Long.parseLong(res.getString(2));
                LOG.info("REPL Status curr Repl Event Id : {}", currReplEventId);
            }

            if (Long.parseLong(currReplEventId) < Long.parseLong(lastReplEventId)) {
                String msg = "currReplEventId:"+currReplEventId+" is less than lastReplEventId: "+lastReplEventId;
                throw new SQLException(msg);
            }
            */

            String replDump = HiveDRUtils.getReplDump(database, lastReplEventId, currReplEventId,
                    properties.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
            res = sourceStatement.executeQuery(replDump);
            LOG.info("Running REPL DUMP statement on source: {}", replDump);
            if (res.next()) {
                LOG.info("ResultSet DUMP output String : {} ", res.getString(1));
                LOG.info("Source NN for dump directory : {}",
                        properties.getProperty(HiveDRProperties.SOURCE_NN.getName()));
                dumpDirectory = properties.getProperty(HiveDRProperties.SOURCE_NN.getName())
                        + res.getString(1).split("\u0001")[0];
                //lastEventId = Long.parseLong(res.getString(2));
                LOG.info("REPL DUMP Directory : {}", dumpDirectory);
            }

            res.close();
        } catch (SQLException sqe) {
            LOG.error("SQLException occurred for export statement : {} ", sqe);
            instanceExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), sqe.getMessage());
        }

        return dumpDirectory;
    }

    private void pullReplication(String dumpDirectory) {
        LOG.info("Performing Import for database : {} dumpDirectory: {}", database, dumpDirectory);
        String replLoad = HiveDRUtils.getReplLoad(database, dumpDirectory);
        try {
            LOG.info("Running REPL LOAD statement on target: {}", replLoad);
            targetStatement.executeQuery(replLoad);
        } catch (SQLException sqe) {
            LOG.error("SQLException occurred for import statement : {} ", sqe);
            instanceExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), sqe.getMessage());
        }
    }
}
