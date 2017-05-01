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

import com.hortonworks.beacon.exceptions.BeaconException;

import org.apache.hadoop.fs.Path;

/**
 * A simple plugin provider interface for DLM.
 * <p>
 * This is the V1 of the plugin infrastructure.   The plugins will be defined using
 * the interfaces defined as part of  <i>ServiceLoader</i> in JDK.   That is
 * the implementation of the plugins will be  packaged as a  jar file that is
 * available in the beacon server classpath.
 * <p>
 * On startup, beacon server will identify all implementations of the Beacon plugin
 * interface and load them.
 * <p>
 * In V1, we only support system plugins.   These plugins are Ranger and Atlas
 * and they have well defined orchestration properties
 * Always invokved before the actual data plugin (hive/hdfs) is invoked
 * Failure to invoke the plugin aborts the replication (should we relax this for
 * Atlas)
 */
public interface Plugin {

    /**
     * Get plugin status.   Valid statuses are ACTIVE, INACTIVE, INITIALIZING, ERROR
     */

    enum Status {
        INVALID,
        INITIALIZING,
        ACTIVE,
        INACTIVE,
        FAILED,
    }

    /**
     * Register the plugin with beacon specific information.    The BeaconInfo object will provide
     * the beacon staging directory location and cluster name among others.  Beacon plugin system will
     * call this method on the plugin provider on discovery.   The plugin should make copy the info for
     * future usage.  The plugin should return the information on the plugin as a response.
     *
     * @param info
     * @return
     * @throws BeaconException
     */
    PluginInfo register(BeaconInfo info) throws BeaconException;

    /**
     * Export the plugin specific data for the given <i>dataset</i> from the <i>srcCluster</i> to
     * the path <i>exportPath</i>.   The path returned is managed by the plugin.   Plugin can use
     * the Beacon provided staging dir to create subfolders to manage the plugin specific data, but
     * the lifecycle of the data is managed by the plugin.
     * There can be only one outstanding call to a plugin to export data related to a dataset.
     * Note that this call is invoked on the plugin in the targetCluster
     * A plugin can return an empty path to signify that there is no data to export.
     *
     * @param dataset
     * @return Path where the plugin data is returned.  Empty path means no data.
     * @throws BeaconException
     */
    Path exportData(DataSet dataset) throws BeaconException;

    /**
     * Import the plugin specific data for the given <i>dataset</i> from the <i>targetCluster</i> from
     * the path <i>exportedDataPath</i>
     * There can be only one outstanding call to a plugin to import data related to a dataset.
     * After a successful import, the plugin is responsible for cleanup of the data and the staging paths.
     *
     * @param dataset
     * @param exportedDataPath  Data that was exported by export command.
     * @return
     * @throws BeaconException
     */
    void importData(DataSet dataset, Path exportedDataPath)
            throws BeaconException;

    /**
     * Return plugin specific information.
     *
     * @return Plugin info
     * @throws BeaconException
     */
    PluginInfo getInfo() throws BeaconException;

    /**
     * Return plugin stats.
     *
     * @return Plugin stats as a JSON
     * @throws BeaconException
     */
    PluginStats getStats() throws BeaconException;

    /**
     * Get plugin status.   Valid statuses are ACTIVE, INACTIVE, INITIALIZING, ERROR
     *
     * @return Plugin status
     * @throws BeaconException
     */
    Status getStatus() throws BeaconException;
}
