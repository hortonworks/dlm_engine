/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.plugin.DataSet;

/**
 * Plugin dataset impl.
 */
public class DatasetImpl implements DataSet {
    private String sourceDataset;
    private String targetDataset;
    private DataSetType type;
    private Cluster sourceCluster;
    private Cluster targetCluster;

    public DatasetImpl(String sourceDataset, String targetDataset, DataSetType type,
                       Cluster sourceCluster, Cluster targetCluster) {
        this.sourceDataset = sourceDataset;
        this.targetDataset = targetDataset;
        this.type = type;
        this.sourceCluster = sourceCluster;
        this.targetCluster = targetCluster;
    }

    @Override
    public DataSetType getType() {
        return type;
    }


    @Override
    public String getSourceDataSet() {
        return sourceDataset;
    }

    @Override
    public String getTargetDataSet() {
        return targetDataset;
    }

    @Override
    public Cluster getSourceCluster() {
        return sourceCluster;
    }

    @Override
    public Cluster getTargetCluster() {
        return targetCluster;
    }

    @Override
    public String toString() {
        return "DatasetImpl{"
                + "sourceDataset='" + sourceDataset + '\''
                + "targetDataset='" + targetDataset + '\''
                + ", type=" + type
                + '}';
    }
}
