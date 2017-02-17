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
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.fs.Path;

/**
 * BeaconInfo to be used by the plugin.
 */
public interface BeaconInfo {

    /**
     * Return the current cluster of beacon engine.
     *
     * @return cluster
     * @throws BeaconException
     */
    Cluster getCluster() throws BeaconException;

    /**
     * Return the staging directory that beacon uses.   Plugins can create subdirectories
     * with appropriate permissions underneath this directory.   This directory will be owned by beacon
     * and writable by the hadoop group (the group for services).
     *
     * @return Staging directory
     * @throws BeaconException
     */
    Path getStagingDir() throws BeaconException;

}
