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

package com.hortonworks.dlmengine.hive;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;
import com.hortonworks.dlmengine.DataSet;
import com.hortonworks.dlmengine.fs.FSDataSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Dataset that represents Hive DB.
 */
public class HiveDBDataSet extends DataSet {
    private static final String DB_PATTERN = "^[a-zA-Z0-9_]*$";
    private final HiveMetadataClient metadataClient;
    private final FSDataSet warehouseDataset;
    private final BeaconCluster cluster;

    @VisibleForTesting
    //Used only for tests
    protected HiveDBDataSet(String name) {
        super(name);
        metadataClient = null;
        warehouseDataset = null;
        cluster = null;
    }

    public HiveDBDataSet(String dbName, String clusterName, ReplicationPolicy replicationPolicy)
            throws BeaconException {
        super(dbName);
        validate(dbName);
        cluster = new BeaconCluster(clusterName);
        metadataClient = HiveClientFactory.getMetadataClient(cluster);
        warehouseDataset = FSDataSet.create(cluster.getHiveWarehouseLocation(), clusterName, replicationPolicy);
    }

    private void validate(String dbName) throws ValidationException {
        if (dbName != null) {
            String hiveDBwithoutEscaping = dbName;
            if (hiveDBwithoutEscaping.startsWith("`") && hiveDBwithoutEscaping.endsWith("`")) {
                hiveDBwithoutEscaping = dbName.substring(1, dbName.length()-1);
            }
            if (hiveDBwithoutEscaping.matches(DB_PATTERN)) {
                return;
            }
        }
        throw new ValidationException(StringFormat.format("Hive target dataset name {} is invalid", dbName));
    }

    @Override
    public void create(FileStatus fileStatus) throws BeaconException {
        //do nothing as replication will automatically buildReplicationPolicy target db
    }

    @Override
    public boolean exists() throws BeaconException {
        return metadataClient.doesDBExist(name);
    }

    public boolean isEmpty() throws BeaconException {
        List<String> tables = metadataClient.getTables(name);
        return tables == null || tables.isEmpty();
    }

    @Override
    public void validateWriteAllowed() throws ValidationException {
        warehouseDataset.validateWriteAllowed();
    }

    @Override
    public Configuration getHadoopConf() throws BeaconException {
        return warehouseDataset.getHadoopConf();
    }

    @Override
    public boolean isSnapshottable() {
        return false;
    }

    @Override
    public void deleteAllSnapshots(String snapshotNamePrefix) {
        //do nothing
    }

    @Override
    public void allowSnapshot() {
        //do nothing
    }

    @Override
    public void disallowSnapshot() {
        //do nothing
    }

    @Override
    public boolean isEncrypted() throws BeaconException {
        return warehouseDataset.isEncrypted();
    }

    @Override
    public void close() {
        close(metadataClient);
        close(warehouseDataset);
    }

    @Override
    public boolean conflicts(String otherDB) {
        //TODO should this be equals ignore case?
        return name.equals(otherDB);
    }

    @Override
    public FileStatus getFileStatus() throws BeaconException {
        return null;
    }

    public FSDataSet getWarehouseDataset() {
        return warehouseDataset;
    }

    public BeaconCluster getCluster() {
        return cluster;
    }

    public List<String> listDatabases() throws BeaconException {
        return metadataClient.listDatabases();
    }

    public List<String> listTables() throws BeaconException {
        return metadataClient.getTables(name);
    }

    public Path getLocation(String name) throws BeaconException {
        return metadataClient.getDatabaseLocation(name);
    }
}
