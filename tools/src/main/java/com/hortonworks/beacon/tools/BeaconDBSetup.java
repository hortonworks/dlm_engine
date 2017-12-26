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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;

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
    private static final String REPLACE_VERSION = "%SCHEMA_VERSION%";
    private static final String INSERT_SCHEMA_VERSION =
            "insert into beacon_sys (name, data) values ('schema_version', '" + REPLACE_VERSION + "')";
    private static final String UPDATE_SCHEMA_VERSION =
            "update beacon_sys set data = '%SCHEMA_VERSION%' where name = 'schema_version'";

    private static final ArrayList<String> SCHEMA_VERSIONS = new ArrayList<String>() {
        {
            add("0.1");
            add("0.2");
        }
    };
    private static final String SCHEMA_VERSION = SCHEMA_VERSIONS.get(SCHEMA_VERSIONS.size()-1);

    private DbStore dbStore = BeaconConfig.getInstance().getDbStore();

    private BeaconDBSetup() {
    }

    public static void main(String[] args) throws BeaconException, SQLException {
        setupDB();
    }

    public static void setupDB() throws BeaconException, SQLException {
        BeaconDBSetup dbSetup = new BeaconDBSetup();
        try(Connection connection = dbSetup.getConnection(dbSetup.dbStore)) {
            LOGGER.info("Database setup is starting...");
            String version = dbSetup.getSchemaVersion(connection);
            if (StringUtils.isBlank(version)) {
                String sqlFile = dbSetup.getSchemaFile(null);
                dbSetup.bootstrap(connection, sqlFile);
            } else if (!version.equalsIgnoreCase(SCHEMA_VERSION)){
                LOGGER.info("Upgrading database current version [{}] to [{}]", version, SCHEMA_VERSION);
                int index = SCHEMA_VERSIONS.indexOf(version);
                for (int i = index+1; i<SCHEMA_VERSIONS.size(); i++) {
                    String schemaFile = dbSetup.getSchemaFile(SCHEMA_VERSIONS.get(i));
                    dbSetup.upgrade(connection, schemaFile);
                }
                dbSetup.updateVersion(connection, SCHEMA_VERSION);
            } else {
                LOGGER.info("Database schema is already setup with schema version [{}]", version);
            }
            LOGGER.info("Database setup is completed.");
        } catch (Throwable e) {
            LOGGER.error("Database setup failed with error {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private String getSchemaVersion(Connection connection) throws SQLException {
        boolean exists = checkDatabaseExists(connection);
        if (exists) {
            String versionQuery = "Select data from beacon_sys where name=?";
            try(PreparedStatement statement = connection.prepareStatement(versionQuery)) {
                statement.setString(1, "schema_version");
                try(ResultSet resultSet = statement.executeQuery()) {
                    String version = null;
                    if (resultSet.next()) {
                        version = resultSet.getString("data");
                    }
                    return version;
                }
            }
        }
        return null;
    }

    private String getSchemaFile(String version) {
        String schemaDir = dbStore.getSchemaDirectory();
        if (StringUtils.isBlank(schemaDir)) {
            throw new IllegalArgumentException(
                StringFormat.format("Schema directory does not exist: {}", schemaDir));
        }
        String dbType = getDatabaseType(dbStore);
        String schemaFile;
        if (StringUtils.isBlank(version)) {
            schemaFile = SCHEMA_FILE_PREFIX + dbType + ".sql";
        } else {
            schemaFile = SCHEMA_FILE_PREFIX + dbType + "_" + version + ".sql";
        }
        File sqlFile = new File(schemaDir, schemaFile);
        if (!sqlFile.exists()) {
            throw new IllegalArgumentException(
                StringFormat.format("Schema file does not exist: {}", sqlFile.getAbsolutePath()));
        }
        LOGGER.info("Database schema file: [{}]", sqlFile.getAbsolutePath());
        return sqlFile.getAbsolutePath();
    }

    private void bootstrap(Connection connection, String sqlFile) throws BeaconException {
        try {
            LOGGER.info("Setting up database with schema file: {}", sqlFile);
            List<String> queries = new ArrayList<>(getQueries(sqlFile));
            LOGGER.info("Creating schema for the database...");
            connection.setAutoCommit(false);
            createSchema(connection, dbStore);
            queries.add(BEACON_SYS_TABLE);
            executeDDLs(connection, queries);
            insertSchemaVersion(connection, SCHEMA_VERSION);
            connection.commit();
        } catch (Throwable e) {
            throw new BeaconException(e);
        }
    }

    private void upgrade(Connection connection, String schemaFile) throws BeaconException {
        try {
            List<String> queries = new ArrayList<>(getQueries(schemaFile));
            LOGGER.info("Upgrading schema for the database...");
            connection.setAutoCommit(false);
            executeDDLs(connection, queries);
            connection.commit();
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }

    private void insertSchemaVersion(Connection connection, String schemaVersion) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String insertSchemaVersion = INSERT_SCHEMA_VERSION.replace(REPLACE_VERSION, schemaVersion);
            statement.executeUpdate(insertSchemaVersion);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    private void updateVersion(Connection connection, String schemaVersion) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String updateVersion = UPDATE_SCHEMA_VERSION.replace(REPLACE_VERSION, schemaVersion);
            statement.executeUpdate(updateVersion);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    private boolean checkDatabaseExists(Connection connection) throws SQLException {
        LOGGER.info("Checking database schema is already setup...");
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet resultSet = metaData.getTables(null, null, "%", null)) {
            boolean exists = false;
            while (resultSet.next() && !exists) {
                String tableName = resultSet.getString(3);
                exists = tableName.equalsIgnoreCase("beacon_sys");
            }
            return exists;
        }
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
                LOGGER.error("Derby schema creation failed: " + e.getMessage());
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

    private void executeDDLs(Connection connection, List<String> queries) throws SQLException {
        for (String query : queries) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(query);
            } catch (SQLException e) {
                LOGGER.info("Failed DDL query: " + query);
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
