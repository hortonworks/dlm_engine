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
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Utility Class for Hive Repl Status.
 */
public final class HiveDRUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRUtils.class);

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";

    private HiveDRUtils() {}

    private static String getSourceHS2ConnectionUrl(Properties properties, HiveActionType actionType) {
        String connString;
        switch (actionType) {
            case EXPORT:
                connString = getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.SOURCE_HS2_URI.getName()),
                        properties.getProperty(HiveDRProperties.SOURCE_DATASET.getName()));
                break;
            case IMPORT:
                connString =  getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.TARGET_HS2_URI.getName()),
                        properties.getProperty(HiveDRProperties.SOURCE_DATASET.getName()));
                break;
            default:
                throw new IllegalArgumentException("Hive Action Type : " +actionType+ " not supported:");
        }

        return connString;
    }

    private static String getHS2ConnectionUrl(final String hs2Uri, final String database) {
        StringBuilder connString = new StringBuilder();
        connString.append(JDBC_PREFIX).append(StringUtils.removeEnd(hs2Uri, "/")).append("/").append(database);

        LOG.info("getHS2ConnectionUrl connection uri: {}", connString);
        return connString.toString();
    }

    static Connection getDriverManagerConnection(Properties properties, HiveActionType actionType) {
        Connection connection = null;
        //To bypass findbugs check, need to store empty password in Properties.
        Properties password = new Properties();
        password.put("password", "");
        String user = "";

        String connString = getSourceHS2ConnectionUrl(properties, actionType);
        try {
            connection = DriverManager.getConnection(connString, user, password.getProperty("password"));
        } catch (SQLException sqe) {
            LOG.error("Exception occurred initializing Hive Server : {}", sqe);
        }
        return connection;
    }

    static void initializeDriveClass() {
        try {
            Class.forName(DRIVER_NAME);
            DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
        } catch (ClassNotFoundException e) {
            LOG.error("{} not found : ", DRIVER_NAME, e);
        }
    }

    protected static void cleanup(Statement statement, Connection connection) throws BeaconException {
        try {
            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (SQLException sqe) {
            throw new BeaconException("Exception occurred while closing connection : {}", sqe);
        }
    }
}
