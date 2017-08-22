/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.tools;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

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

import org.apache.commons.lang3.StringUtils;

/**
 * Beacon database setup tool.
 */
public final class BeaconDBSetup {

    private static final BeaconLog LOGGER = BeaconLog.getLog(BeaconDBSetup.class);
    private static final String QUERY_SEPARATOR = ";";
    private static final String COMMENT_LINE = "--";
    private static final String SCHEMA_FILE_PREFIX = "tables_";
    private static final String BEACON_SYS_TABLE =
            "create table beacon_sys (name varchar(40), data varchar(250))";
    private static final String INSERT_SCHEMA_VERSION =
            "insert into beacon_sys (name, data) values ('schema_version', '0.1')";

    private BeaconDBSetup() {}

    public static void main(String[] args) {
        setupDB();
    }

    public static void setupDB() {
        BeaconDBSetup dbSetup = new BeaconDBSetup();
        try {
            LOGGER.info(MessageCode.TOOL_000002.name());
            BeaconConfig beaconConfig = BeaconConfig.getInstance();
            String sqlFile = dbSetup.getSchemaFile(beaconConfig.getDbStore());
            LOGGER.info(MessageCode.TOOL_000003.name(), sqlFile);
            dbSetup.setupBeaconDB(sqlFile);
            LOGGER.info(MessageCode.TOOL_000004.name());
        } catch (Throwable e) {
            LOGGER.error(MessageCode.TOOL_000005.name(), e.getMessage());
            System.exit(1);
        }
    }

    private String getSchemaFile(DbStore store) {
        String schemaDir = store.getSchemaDirectory();
        if (StringUtils.isBlank(schemaDir)) {
            throw new IllegalArgumentException(ResourceBundleService.getService()
                    .getString(MessageCode.TOOL_000001.name(), "directory", schemaDir));
        }
        String dbType = getDatabaseType(store);
        File sqlFile = new File(schemaDir, SCHEMA_FILE_PREFIX + dbType + ".sql");
        if (!sqlFile.exists()) {
            throw new IllegalArgumentException(ResourceBundleService.getService()
                    .getString(MessageCode.TOOL_000001.name(), "file", sqlFile.getAbsolutePath()));
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
                LOGGER.info(MessageCode.TOOL_000006.name());
                connection.setAutoCommit(false);
                createSchema(connection, store);
                queries.add(BEACON_SYS_TABLE);
                createTables(connection, queries);
                //TODO Later setup should check the version and update.
                insertBeaconVersion(connection);
                connection.commit();
            } else {
                LOGGER.info(MessageCode.TOOL_000007.name());
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
            statement.executeUpdate(INSERT_SCHEMA_VERSION);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    private boolean checkDatabaseExists(Connection connection) throws SQLException {
        LOGGER.info(MessageCode.TOOL_000008.name());
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
            LOGGER.info(MessageCode.TOOL_000009.name(), schema);
            try(Statement statement = connection.createStatement()) {
                statement.execute(schema);
            } catch (SQLException e) {
                LOGGER.error(MessageCode.TOOL_000010.name(), e.getMessage());
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
                LOGGER.info(MessageCode.TOOL_000011.name(), query);
                LOGGER.error(MessageCode.TOOL_000012.name(), e.getMessage());
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
