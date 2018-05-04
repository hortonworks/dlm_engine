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

import com.hortonworks.beacon.HiveServerAuthenticationType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Hive Metastore client.
 */
public class HMSMetadataClient implements HiveMetadataClient {
    private final HiveMetaStoreClient client;
    private String clusterName;
    private static final Logger LOG = LoggerFactory.getLogger(HMSMetadataClient.class);

    public HMSMetadataClient(Cluster cluster) throws BeaconException {
        this.clusterName = cluster.getName();
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, cluster.getHmsEndpoint());
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        if (HiveServerAuthenticationType.valueOf(cluster.getHiveServerAuthentication())
                == HiveServerAuthenticationType.KERBEROS
                && StringUtils.isNotEmpty(cluster.getHiveMetastoreKerberosPrincipal())) {
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
                    cluster.getHiveMetastoreKerberosPrincipal());
        }
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new BeaconException(e, "Exception while getting HiveMetaStoreClient");
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public List<String> listDatabases() throws BeaconException {
        try {
            return client.getAllDatabases();
        } catch (MetaException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public Path getDatabaseLocation(String dbName) throws BeaconException {
        LOG.debug("Listing tables for {}", dbName);
        try {
            Database db = client.getDatabase(dbName);
            if (db == null) {
                throw new ValidationException("Database {} doesn't exists on cluster {}", dbName, clusterName);
            }
            return new Path(db.getLocationUri());
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public List<String> getTables(String dbName) throws BeaconException {
        try {
            return client.getAllTables(dbName);
        } catch (MetaException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public boolean doesDBExist(String dbName) throws BeaconException {
        try {
            Database db = client.getDatabase(dbName);
            return db != null;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void dropTable(String dbName, String tableName) throws BeaconException {
        try {
            client.dropTable(dbName, tableName);
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void dropDatabase(String dbName) throws BeaconException {
        try {
            client.dropDatabase(dbName, true, true, true);
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws BeaconException {
        try {
            client.dropFunction(dbName, functionName);
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public List<String> getFunctions(String dbName) throws BeaconException {
        try {
            // Need to check what kind of pattern are accepted.
            return client.getFunctions(dbName, "*");
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }
}
