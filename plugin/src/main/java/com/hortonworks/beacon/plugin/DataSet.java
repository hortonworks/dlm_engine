/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.plugin;

/**
 * Defines the unit of replication for a plugin.   For V1 it is either a Hive DB or a HDFS folder.
 */
public interface DataSet {

    /**
     * Type of the replication dataset - can be one of Hive, HDFS in v1
     */
    public enum  DataSetType {
         HIVE,
        HDFS,
    };

    /**
     * Get the current dataset type
     * @return datasettype
     */
    public DataSetType getType();

    /**
     * DataSet Name.   Either Hive DB or HDFS folder name
     * @return HiveDB or HDFS folder as defined for the dataset
     */
    public String getDataSet();
}
