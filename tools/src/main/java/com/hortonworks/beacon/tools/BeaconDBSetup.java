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
import com.hortonworks.beacon.config.Store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class BeaconDBSetup {

    public static final String QUERY_SEPARATOR = ";";
    public static final String COMMENT_LINE = "--";

    private BeaconDBSetup() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Sql file is required as input argument.");
        }
        String sqlFile = args[0];
        if (!(new File(sqlFile).exists())) {
            throw new IllegalArgumentException("Input file does not exits. Path: " + sqlFile);
        }
        System.out.println("Starting database setup with sqlFile: " + sqlFile);
        BeaconDBSetup dbcli = new BeaconDBSetup();
        try {
            dbcli.setupBeaconDB(sqlFile);
            System.out.println("Database setup is completed.");
        } catch (Exception e) {
            System.out.println("Database setup failed with error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void setupBeaconDB(String sqlFile) throws Exception {
        BeaconConfig beaconConfig = BeaconConfig.getInstance();
        Store store = beaconConfig.getStore();
        Connection connection = getConnection(store);
        List<String> queries = getQueries(sqlFile);
        connection.setAutoCommit(true);
        createSchema(connection, store);
        createTables(connection, queries);
        connection.close();
    }

    private void createSchema(Connection connection, Store store) {
        String url = store.getUrl();
        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));
        if (dbType.equals("derby")) {
            // Create schema with the user for derby db
            String schema = "create schema " + store.getUser();
            System.out.println("Derby schema: " + schema);
            try {
                Statement statement = connection.createStatement();
                statement.execute(schema);
            } catch (SQLException e) {
                System.out.println("Ignore error: " + e.getMessage());
            }
        }
    }

    private void createTables(Connection connection, List<String> queries) throws SQLException {
        for (String query : queries) {
            try {
                Statement statement = connection.createStatement();
                statement.execute(query);
            } catch (SQLException e) {
                System.out.println("Failed table creation query: " + query);
                throw e;
            }
        }
    }

    private Connection getConnection(Store store) throws ClassNotFoundException, SQLException {
        Class.forName(store.getDriver());
        return DriverManager.getConnection(store.getUrl(), store.getUser(), store.getPassword());
    }

    private List<String> getQueries(String sqlFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(sqlFile));
        StringBuilder sqlBuilder = new StringBuilder();
        String line;
        while ( (line = reader.readLine()) != null) {
            if (line.startsWith(COMMENT_LINE)) {
                continue;
            }
            sqlBuilder.append(line);
        }
        String sql = sqlBuilder.toString();
        return Arrays.asList(sql.split(QUERY_SEPARATOR));
    }
}