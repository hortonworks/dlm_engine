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
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.BeaconInfo;
import org.apache.hadoop.fs.Path;


/**
 * Beacon Info implementation.
 */
public class BeaconInfoImpl implements BeaconInfo {
    /* TODO- Should this be created as part of management pack? */
    private static final Path BEACON_STAGING_PATH =  new Path("/apps/beacon/plugin/");
    @Override
    public Cluster getCluster() throws BeaconException {
        return ClusterHelper.getLocalCluster();
    }

    @Override
    public Path getStagingDir() throws BeaconException {
        return BEACON_STAGING_PATH;
    }
}
