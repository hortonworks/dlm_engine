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
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive server metadata client using jdbc.
 */
public class HS2Client implements HiveMetadataClient, HiveServerClient {
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
    private static final Logger LOG = LoggerFactory.getLogger(HS2Client.class);

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;

    private Connection connection;
    private String clusterName;
    private  String connectionString;
    private  String knoxGatewayURL;

    public HS2Client(Cluster cluster) throws BeaconException {
        this.clusterName = cluster.getName();
        // target is not data lake.   We will use pull - so update source cluster with knox proxy address
        // if enabled
        Engine engine = BeaconConfig.getInstance().getEngine();

        initializeDriveClass();
        this.knoxGatewayURL = cluster.getKnoxGatewayURL();
        if (cluster.isLocal() || !engine.isKnoxProxyEnabled()) {
            this.connectionString = HiveDRUtils.getHS2ConnectionUrl(cluster.getHsEndpoint());
        } else {
            this.connectionString = HiveDRUtils.getKnoxProxiedURL(cluster);
            LOG.debug("Generated knox propxied hs2 endpoint: {}", connectionString);
        }
    }

    public HS2Client(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public Statement createStatement() throws BeaconException {
        try {
            return getConnection().createStatement();
        } catch (SQLException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void close() {
        close(connection);
    }

    @Override
    public List<String> listDatabases() throws BeaconException {
        Statement statement = null;
        List<String> databases = new ArrayList<>();
        try {
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
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
            statement = getConnection().createStatement();
            statement.execute(DROP_FUNCTION + ' ' + dbName + '.' + functionName);
        } catch (SQLException e) {
            throw new BeaconException(e);
        } finally {
            close(statement);
        }
    }

    private void initializeDriveClass() throws BeaconException {
        try {
            Class.forName(DRIVER_NAME);
            DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
        } catch (ClassNotFoundException e) {
            throw new BeaconException(e, "{} not found: ", DRIVER_NAME);
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("DMI_EMPTY_DB_PASSWORD")
    public Connection getConnection() throws BeaconException {
        if (connection != null) {
            return connection;
        }
        if (connectionString.endsWith(BeaconConstants.HIVE_SSO_COOKIE)) {
            String token =
                    KnoxTokenUtils.getKnoxSSOToken(knoxGatewayURL, true);
            connectionString +=  "=" + token;

        }
        String user = "";
        try {
            if (!connectionString.contains(BeaconConstants.HIVE_SSO_COOKIE)) {
                UserGroupInformation currentUser = UserGroupInformation.getLoginUser();
                if (currentUser != null) {
                    user = currentUser.getShortUserName();
                }
            }
            LOG.debug("Using connection string: {}", connectionString);
            connection = DriverManager.getConnection(connectionString, user, "");
        } catch (IOException | SQLException ex) {
            LOG.error("Exception occurred initializing Hive server: {}", ex);
            throw new BeaconException("Exception occurred initializing Hive server: ", ex);
        }
        return connection;
    }
}
