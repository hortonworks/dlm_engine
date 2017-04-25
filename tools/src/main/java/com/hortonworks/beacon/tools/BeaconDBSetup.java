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

package com.hortonworks.beacon.tools;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Beacon database setup tool.
 */
public final class BeaconDBSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeaconDBSetup.class);
    private static final String QUERY_SEPARATOR = ";";
    private static final String COMMENT_LINE = "--";
    private static final String SCHEMA_FILE_PREFIX = "tables_";
    private static final String BEACON_SYS_TABLE =
            "create table beacon_sys (name varchar(40), data varchar(250))";
    private static final String INSERT_BUILD_VERSION =
            "insert into beacon_sys (name, data) values ('db_version', '0.1')";

    private BeaconDBSetup() {}

    public static void main(String[] args) {
        setupDB();
    }

    public static void setupDB() {
        BeaconDBSetup dbSetup = new BeaconDBSetup();
        try {
            LOGGER.info("Database setup is starting...");
            BeaconConfig beaconConfig = BeaconConfig.getInstance();
            String sqlFile = dbSetup.getSchemaFile(beaconConfig.getDbStore());
            LOGGER.info("Setting up database with schema file: " + sqlFile);
            dbSetup.setupBeaconDB(sqlFile);
            LOGGER.info("Database setup is completed.");
        } catch (Throwable e) {
            LOGGER.error("Database setup failed with error: " + e.getMessage());
            System.exit(1);
        }
    }

    private String getSchemaFile(DbStore store) {
        String schemaDir = store.getSchemaDirectory();
        if (schemaDir == null || schemaDir.trim().length() == 0) {
            throw new NullPointerException("Schema directory is not specified in the beacon config or empty path.");
        }
        String dbType = getDatabaseType(store);
        File sqlFile = new File(schemaDir, SCHEMA_FILE_PREFIX + dbType + ".sql");
        if (!sqlFile.exists()) {
            throw new IllegalArgumentException("Schema file does not exists: " + sqlFile.getAbsolutePath());
        }
        return sqlFile.getAbsolutePath();
    }

    private void setupBeaconDB(String sqlFile) throws BeaconException {
        BeaconConfig beaconConfig = BeaconConfig.getInstance();
        DbStore store = beaconConfig.getDbStore();
        Connection connection = null;
        try {
            connection = getConnection(store);
            List<String> queries = new ArrayList<>(getQueries(sqlFile));
            boolean exists = checkDatabaseExists(connection);
            if (!exists) {
                LOGGER.info("Creating tables for the database...");
                connection.setAutoCommit(false);
                createSchema(connection, store);
                queries.add(BEACON_SYS_TABLE);
                createTables(connection, queries);
                //TODO Later setup should check the version and update.
                insertBeaconVersion(connection);
                connection.commit();
            } else {
                LOGGER.info("Database setup is already done. Returning...");
            }
        } catch (Throwable e) {
            throw new BeaconException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore.
                }
            }
        }
    }

    private void insertBeaconVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(INSERT_BUILD_VERSION);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    private boolean checkDatabaseExists(Connection connection) throws SQLException {
        LOGGER.info("Checking database is already setup...");
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, "%", null);
        boolean exists = false;
        while (resultSet.next() && !exists) {
            String tableName = resultSet.getString(3);
            exists = tableName.equalsIgnoreCase("beacon_sys");
        }
        return exists;
    }

    private void createSchema(Connection connection, DbStore store) throws SQLException {
        String dbType = getDatabaseType(store);
        if (dbType.equals("derby")) {
            // Create schema with the user for derby db
            String schema = "create schema " + store.getUser();
            LOGGER.info("Derby schema: " + schema);
            try(Statement statement = connection.createStatement()) {
                statement.execute(schema);
            } catch (SQLException e) {
                LOGGER.error("derby schema creation failed: " + e.getMessage());
                throw e;
            }
        }
    }

    private String getDatabaseType(DbStore store) {
        String url = store.getUrl();
        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));
        return dbType;
    }

    private void createTables(Connection connection, List<String> queries) throws SQLException {
        for (String query : queries) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(query);
            } catch (SQLException e) {
                LOGGER.info("Failed table creation query: " + query);
                LOGGER.error("Error message: " + e.getMessage());
                throw e;
            }
        }
    }

    private Connection getConnection(DbStore store) throws ClassNotFoundException, SQLException, BeaconException {
        Class.forName(store.getDriver());
        return DriverManager.getConnection(store.getUrl(), store.getUser(), store.resolvePassword());
    }

    private List<String> getQueries(String sqlFile) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
            StringBuilder sqlBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(COMMENT_LINE)) {
                    continue;
                }
                sqlBuilder.append(line);
            }
            String sql = sqlBuilder.toString();
            return Arrays.asList(sql.split(QUERY_SEPARATOR));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }
}
