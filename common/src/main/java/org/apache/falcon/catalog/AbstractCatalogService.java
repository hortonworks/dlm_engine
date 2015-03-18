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

package org.apache.falcon.catalog;

import org.apache.falcon.FalconException;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Interface definition for a catalog registry service
 * such as Hive or HCatalog.
 */
public abstract class AbstractCatalogService {

    /**
     * This method checks if the catalog service is alive.
     *
     * @param conf conf
     * @param catalogUrl url for the catalog service
     * @return if the service was reachable
     * @throws FalconException exception
     */
    public abstract boolean isAlive(Configuration conf, String catalogUrl) throws FalconException;

    /**
     * This method checks if the given table exists in the catalog.
     *
     * @param conf  conf
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @return if the table exists
     * @throws FalconException exception
     */
    public abstract boolean tableExists(Configuration conf, String catalogUrl,
                                        String database, String tableName) throws FalconException;

    /**
     * Returns if the table is external or not. Executed in the workflow engine.
     *
     * @param conf conf object
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @return true if external else false
     * @throws FalconException
     */
    public abstract boolean isTableExternal(Configuration conf, String catalogUrl, String database,
                                            String tableName) throws FalconException;

    /**
     * List partitions by filter. Executed in the workflow engine.
     *
     * @param conf conf object
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @param filter The filter string,
     *    for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
     *    be done only on string partition keys.
     * @return list of partitions
     * @throws FalconException
     */
    public abstract List<CatalogPartition> listPartitionsByFilter(Configuration conf,
                                                                  String catalogUrl,
                                                                  String database,
                                                                  String tableName, String filter)
        throws FalconException;

    /**
     * Drops a given partition. Executed in the workflow engine.
     *
     * @param conf  conf object
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @param partitionValues list of partition values
     * @param deleteData should dropPartition also delete the corresponding data
     * @return if the partition was dropped
     * @throws FalconException
     */
    public abstract boolean dropPartition(Configuration conf, String catalogUrl,
                                           String database, String tableName, List<String> partitionValues,
                                           boolean deleteData) throws FalconException;

    /**
     * Drops the partitions. Executed in the workflow engine.
     *
     * @param conf  conf object
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @param partitionValues list of partition values
     * @param deleteData should dropPartition also delete the corresponding data
     * @return if the partition was dropped
     * @throws FalconException
     */
    public abstract void dropPartitions(Configuration conf, String catalogUrl,
                                        String database, String tableName,
                                        List<String> partitionValues, boolean deleteData) throws FalconException;

    /**
     * Gets the partition. Executed in the workflow engine.
     *
     * @param conf  conf
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @param partitionValues Values for partition columns.
     * @return An instance of CatalogPartition.
     * @throws FalconException
     */
    public abstract CatalogPartition getPartition(Configuration conf, String catalogUrl,
                                                  String database, String tableName,
                                                  List<String> partitionValues)
        throws FalconException;
}
