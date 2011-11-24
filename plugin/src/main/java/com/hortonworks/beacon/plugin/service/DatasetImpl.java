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
    private String stagingDir;

    public DatasetImpl(String sourceDataset, String targetDataset, DataSetType type,
                       Cluster sourceCluster, Cluster targetCluster, String stagingDir) {
        this.sourceDataset = sourceDataset;
        this.targetDataset = targetDataset;
        this.type = type;
        this.sourceCluster = sourceCluster;
        this.targetCluster = targetCluster;
        this.stagingDir = stagingDir;
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
    public String getStagingPath() {
        return stagingDir;
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
