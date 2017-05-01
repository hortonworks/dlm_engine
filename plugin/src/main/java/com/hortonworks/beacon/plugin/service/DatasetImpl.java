/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.plugin.service;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.plugin.DataSet;

/**
 * Plugin dataset impl.
 */
public class DatasetImpl implements DataSet {
    private String dataset;
    private DataSetType type;
    private Cluster sourceCluster;
    private Cluster targetCluster;

    public DatasetImpl(String dataset, DataSetType type,
                       Cluster sourceCluster, Cluster targetCluster) {
        this.dataset = dataset;
        this.type = type;
        this.sourceCluster = sourceCluster;
        this.targetCluster = targetCluster;
    }

    @Override
    public DataSetType getType() {
        return type;
    }


    public String getDataSet() {
        return dataset;
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
                + "dataset='" + dataset + '\''
                + ", type=" + type
                + '}';
    }
}
