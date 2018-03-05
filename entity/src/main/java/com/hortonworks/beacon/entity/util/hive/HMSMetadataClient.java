/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.entity.util.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
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

        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new BeaconException(e, "Exception while getting HiveMetaStoreClient");
        }
    }

    @Override
    public void close() {
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
            return client.getFunctions(dbName, null);
        } catch (TException e) {
            throw new BeaconException(e);
        }
    }
}
