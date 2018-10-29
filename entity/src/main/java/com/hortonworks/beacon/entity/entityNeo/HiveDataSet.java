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

package com.hortonworks.beacon.entity.entityNeo;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Hive implementation of dataset.
 */
public class HiveDataSet extends DataSet {

    private static final Logger LOG = LoggerFactory.getLogger(FSDataSet.class);

    private FSDataSet warehouseFsDataSet;
    private String dbName;
    private HiveMetadataClient metadataClient;

    public HiveDataSet(String dbName, String clusterName, ReplicationPolicy policy)
            throws BeaconException {
        this.dbName = dbName;
        ClusterDao clusterDao = new ClusterDao();
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        metadataClient = HiveClientFactory.getMetadataClient(cluster);
        BeaconCluster beaconCluster = new BeaconCluster(cluster);
        warehouseFsDataSet = FSDataSet.create(beaconCluster.getHiveWarehouseLocation(), clusterName, policy);
    }

    @Override
    public boolean exists() throws BeaconException {
        return metadataClient.doesDBExist(dbName);
    }

    @Override
    public void create() throws IOException {
        //do nothing as replication will automatically create target db
    }

    @Override
    public boolean isEmpty() throws IOException, BeaconException {
        List<String> tables = metadataClient.getTables(dbName);
        return tables == null || tables.isEmpty();
    }

    @Override
    public void isWriteAllowed() throws ValidationException {
        warehouseFsDataSet.isWriteAllowed();
    }

    @Override
    public Configuration getHadoopConf() {
        return warehouseFsDataSet.getHadoopConf();
    }

    @Override
    public boolean isSnapshottable() throws IOException {
        return false;
    }

    @Override
    public void deleteAllSnapshots(String snapshotNamePrefix) throws IOException {
        //Do nothing
    }

    @Override
    public void allowSnapshot() throws IOException {
        //Do nothing
    }

    @Override
    public void disallowSnapshot() throws IOException {
        //Do nothing
    }

    @Override
    public boolean isEncrypted() throws BeaconException {
        return warehouseFsDataSet.isEncrypted();
    }

    @Override
    public void close() {
        try {
            metadataClient.close();
        } catch (Exception ex) {
            LOG.debug("Exception occurred while closing HiveMetaData client ", ex);
        }
        warehouseFsDataSet.close();
    }
}
