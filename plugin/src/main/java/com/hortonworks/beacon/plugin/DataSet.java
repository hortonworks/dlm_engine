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
