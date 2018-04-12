/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.beacon.entity.util.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.PolicyHelper;
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
    private static final String DESC_DATABASE = "DESC DATABASE ";
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final String SHOW_FUNCTIONS = "SHOW FUNCTIONS";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS";
    private static final String DROP_FUNCTION = "DROP FUNCTION IF EXISTS";
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS";
    private static final String CASCADE = "CASCADE";

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
        dbName = PolicyHelper.escapeDataSet(dbName);
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
        dbName = PolicyHelper.escapeDataSet(dbName);
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

    @Override
    public void dropTable(String dbName, String tableName) throws BeaconException {
        dbName = PolicyHelper.escapeDataSet(dbName);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(DROP_TABLE + ' ' + dbName + '.' + tableName);
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
    }

    @Override
    public void dropDatabase(String dbName) throws BeaconException {
        dbName = PolicyHelper.escapeDataSet(dbName);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(DROP_DATABASE + ' ' + dbName + ' ' + CASCADE);
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
    }

    @Override
    public List<String> getFunctions(String dbName) throws BeaconException {
        dbName = PolicyHelper.escapeDataSet(dbName);
        Statement statement = null;
        List<String> functions = new ArrayList<>();
        try {
            statement = connection.createStatement();
            try (ResultSet res = statement.executeQuery(SHOW_FUNCTIONS)) {
                while (res.next()) {
                    String functionName = res.getString(1);
                    if (functionName.startsWith(dbName + ".")) {
                        functions.add(functionName);
                    }
                }
            }
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
        return functions;
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws BeaconException {
        dbName = PolicyHelper.escapeDataSet(dbName);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(DROP_FUNCTION + ' ' + dbName + '.' + functionName);
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
    }
}
