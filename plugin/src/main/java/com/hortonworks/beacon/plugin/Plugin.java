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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;

import org.apache.hadoop.fs.Path;

/**
 * A simple plugin provider interface for DLM.
 *
 * This is the V1 of the plugin infrastructure.   The plugins will be defined using
 * the interfaces defined as part of  <i>ServiceLoader</i> in JDK.   That is
 * the implementation of the plugins will be  packaged as a  jar file that is
 * available in the beacon server classpath.
 *
 * On startup, beacon server will identify all implementations of the Beacon plugin
 * interface and load them.
 *
 * In V1, we only support system plugins.   These plugins are Ranger and Atlas
 * and they have well defined orchestration properties
 *   Always invokved before the actual data plugin (hive/hdfs) is invoked
 *   Failure to invoke the plugin aborts the replication (should we relax this for
 *   Atlas)
 *
 */
public interface Plugin {

    /**
     * Export the plugin specific data for the given <i>dataset</i> from the <i>srcCluster</i> to
     * the path <i>exportPath</i>  (the path is expected to be a staging folder in the src cluster
     * specific to the plugin.
     * Note that this call is invoked on the plugin in the targetCluster
     * @param srcCluster
     * @param dataset
     * @param exportPath
     * @return
     * @throws BeaconException
     */
    public Path exportData(Cluster srcCluster, DataSet dataset, Path exportPath) throws BeaconException;

    /**
     * Export the plugin specific data for the given <i>dataset</i> from the <i>targetCluster</i> from
     * the path <i>path</i>  (the path is expected to be a staging folder in the target cluster
     * specific to the plugin.
     * @param targetCluster
     * @param dataset
     * @param path
     * @return
     * @throws BeaconException
     */
    public void importData(Cluster targetCluster, DataSet dataset, Path path) throws BeaconException;

    /**
     * The plugin should use its own logic to say if the two paths have data to be replicated
     * @param path1
     * @param path2
     * @return
     * @throws BeaconException
     */
    public int compareData(Path path1, Path path2) throws BeaconException;

    /**
     * Return plugin specific information.
     * @return  Plugin info
     * @throws BeaconException
     */
    public PluginInfo getInfo() throws BeaconException;

    /**
     * Return plugin stats.
     * @return  Plugin stats as a JSON
     * @throws BeaconException
     */
    public PluginStats getStats() throws BeaconException;

    /**
     * Get the status of the plugin
     * @return  Plugin status
     * @throws BeaconException
     */
    public PluginStatus getStatus() throws BeaconException;
}
