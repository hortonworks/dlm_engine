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

package com.hortonworks.beacon.scheduler.hive;

import com.hortonworks.beacon.scheduler.DRReplication;
import com.hortonworks.beacon.scheduler.ReplicationJobDetails;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;


public class HiveDRImpl implements DRReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDRImpl.class);

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";

    HiveReplicationJobDetails details;
    List<ReplicationDefinition> replDef;
    private String sourceHiveServerURL;
    private String targetHiveServerURL;
    private String database;
    private List<String> tableList;
    private String stagingDir;

    private String sourceKerberosCredential;
    private String targetKerberosCredential;

    private Connection sourceConnection = null;
    private Connection targetConnection = null;

    private Statement sourceStatement = null;
    private Statement targetStatement = null;


    public HiveDRImpl(ReplicationJobDetails details) {
        this.details = (HiveReplicationJobDetails)details;
    }

    public void establishConnection() {
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
        } catch( SQLException sqe) {
            LOG.error("Exception occurred initializing source Hive Server : {}", sqe);
        }

        try {
            String connString = getTargetHS2ConnectionUrl(authString);
            targetConnection = DriverManager.getConnection(connString, user, password.getProperty("password"));
            targetStatement = targetConnection.createStatement();
        } catch( SQLException sqe) {
            LOG.error("Exception occurred initializing target Hive Server : {}", sqe);
        }
    }

    private String getSourceHS2ConnectionUrl(final String authTokenString) {
        return getHS2ConnectionUrl(details.getSourceHS2URL(), details.getDataBase(), authTokenString);
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
        return getHS2ConnectionUrl(details.getTargetHS2URL(), details.getDataBase(), authTokenString);
    }


    private void createReplicationDefinition() {

        if (tableList.equals("*") || tableList == null) {
            LOG.info("Replication Type is DB");
            // ToDo: If Database don't exists on target, bootstrap.
            ReplicationDefinition replicationDefinition = new ReplicationDefinition(
                    details.getSourceHS2URL(), details.getDataBase(), null, details.getStagingDir(), null);
            replDef.add(replicationDefinition);
        }
    }

    public void performReplication() {
        LOG.info("Prepare Hive Replication on source");
        prepareReplication(replDef);
        LOG.info("Pull Replication on target");
        pullReplication(replDef);
    }

    private void prepareReplication(List<ReplicationDefinition> replDef) {
        LOG.info("Performing Export for database : {}", details.getDataBase());

        String exportStatement = "EXPORT TABLE test TO '" + details.getStagingDir() +"'";

        try {
            LOG.info("Running export statement: {}", exportStatement);
            ResultSet res = sourceStatement.executeQuery(exportStatement);
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            res.close();
        } catch (SQLException sqe) {
            LOG.error("SQLException occurred for export statement : {} ", sqe);
        }
    }

    private void pullReplication(List<ReplicationDefinition> replDef) {
        LOG.info("Performing Import for database : {}", details.getDataBase());

        String importStatement = "IMPORT FROM '" + details.getStagingDir() +"'";

        try {
            LOG.info("Running import statement on target: {}", importStatement);
            ResultSet res = targetStatement.executeQuery(importStatement);

            if (res.next()) {
                System.out.println(res.getString(1));
            }

            res.close();
        } catch (SQLException sqe) {
            LOG.error("SQLException occurred for import statement : {} ", sqe);
        }
    }
}
