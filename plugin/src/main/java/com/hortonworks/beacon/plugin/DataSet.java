/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.plugin;

import com.hortonworks.beacon.client.entity.Cluster;

/**
 * Defines the unit of replication for a plugin.   For V1 it is either a Hive DB or a HDFS folder.
 */
public interface DataSet {

    /**
     * Type of the replication dataset - can be one of Hive, HDFS in v1.
     */
    enum DataSetType {
        HIVE,
        HDFS,
    }

    /**
     * Get the current dataset type.
     *
     * @return dataset type
     */
    DataSetType getType();

    /**
     * DataSet Name.   Either Hive DB or HDFS folder path
     *
     * @return Hive DB or HDFS folder path as defined for the dataset
     */
    String getDataSet();

    /**
     * Source cluster of dataset.
     *
     * @return Cluster
     */
    Cluster getSourceCluster();

    /**
     * Target cluster of dataset.
     *
     * @return Cluster
     */
    Cluster getTargetCluster();
}
