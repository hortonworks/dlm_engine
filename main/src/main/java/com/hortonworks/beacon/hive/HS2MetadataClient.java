/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive server metadata client using jdbc.
 */
public class HS2MetadataClient implements HiveMetadataClient {
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String DESC_DATABASE = "DESC DATABASE ";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final int DB_NOT_EXIST_EC = 10072;
    private static final String DB_NOT_EXIST_STATE = "42000";

    private static final String USE = "USE ";
    private static final Logger LOG = LoggerFactory.getLogger(HS2MetadataClient.class);

    private final Connection connection;
    private final String clusterName;

    public HS2MetadataClient(Cluster cluster) throws BeaconException {
        this.clusterName = cluster.getName();

        HiveDRUtils.initializeDriveClass();
        String connString = HiveDRUtils.getHS2ConnectionUrl(cluster.getHsEndpoint());
        try {
            connection = HiveDRUtils.getConnection(connString);
        } catch (BeaconException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void close() throws BeaconException {
        close(connection);
    }

    @Override
    public List<String> listDatabases() throws BeaconException {
        Statement statement = null;
        List<String> databases = new ArrayList<>();
        try {
            statement = connection.createStatement();
            try (ResultSet res = statement.executeQuery(SHOW_DATABASES)) {
                while (res.next()) {
                    String db = res.getString(1);
                    databases.add(db);
                }
            }
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
        return databases;
    }

    @Override
    public Path getDatabaseLocation(String dbName) throws BeaconException {
        Statement statement = null;
        ResultSet res = null;
        try {
            statement = connection.createStatement();
            String query = DESC_DATABASE + dbName;
            String dbPath = null;
            res = statement.executeQuery(query);
            if (res.next()) {
                dbPath = res.getString(3);
            }
            LOG.debug("Database: {}, path: {}", dbName, dbPath);
            return new Path(dbPath);
        } catch (SQLException e) {
            if (e.getErrorCode() == DB_NOT_EXIST_EC && e.getSQLState().equalsIgnoreCase(DB_NOT_EXIST_STATE)) {
                throw new ValidationException(e, "Database {} doesn't exist on cluster {}", dbName, clusterName);
            }
            throw new BeaconException(e);
        } finally {
            close(res);
            close(statement);
        }
    }

    private void close(AutoCloseable closable) {
        if (closable != null) {
            try {
                closable.close();
            } catch (Exception e) {
                LOG.error("Error while closing resource: {}", e);
            }
        }
    }
    @Override
    public List<String> getTables(String dbName) throws BeaconException {
        List<String> tables = new ArrayList<>();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(USE + dbName);
            try (ResultSet res = statement.executeQuery(SHOW_TABLES)) {
                while (res.next()) {
                    String tableName = res.getString(1);
                    tables.add(tableName);
                }
            }
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
        return tables;
    }

    @Override
    public boolean doesDBExist(String dbName) throws BeaconException {
        Statement statement = null;
        ResultSet res = null;
        try {
            statement = connection.createStatement();
            res = statement.executeQuery(SHOW_DATABASES);
            while (res.next()) {
                String localDBName = res.getString(1);
                if (localDBName.equals(dbName)) {
                    return true;
                }
            }
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(res);
            close(statement);
        }
        return false;
    }
}
