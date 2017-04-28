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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.exceptions.BeaconException;

import java.util.Properties;

/**
 * Builder class to construct Beacon Cluster resource.
 */
public final class ClusterBuilder {

    private ClusterBuilder() {
    }

    public static Cluster buildCluster(final PropertiesIgnoreCase requestProperties,
                                       final String clusterName) throws BeaconException {
        requestProperties.put(ClusterProperties.NAME.getName(), clusterName);
        for (ClusterProperties property : ClusterProperties.values()) {
            if (requestProperties.getPropertyIgnoreCase(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: " + property.getName());
            }
        }

        String name = requestProperties.getPropertyIgnoreCase(ClusterProperties.NAME.getName());
        String description = requestProperties.getPropertyIgnoreCase(ClusterProperties.DESCRIPTION.getName());
        String fsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.FS_ENDPOINT.getName());
        String beaconEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.BEACON_ENDPOINT.getName());

        String atlasEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.ATLAS_ENDPOINT.getName());
        String rangerEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.RANGER_ENDPOINT.getName());
        String hsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.HS_ENDPOINT.getName());
        String peers = requestProperties.getPropertyIgnoreCase(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getPropertyIgnoreCase(ClusterProperties.TAGS.getName());
        Properties properties = EntityHelper.getCustomProperties(requestProperties,
                ClusterProperties.getClusterElements());


        String user = requestProperties.getPropertyIgnoreCase(ClusterProperties.USER.getName());

        return new Cluster.Builder(name, description, fsEndpoint, beaconEndpoint)
                .hsEndpoint(hsEndpoint).atlasEndpoint(atlasEndpoint).rangerEndpoint(rangerEndpoint).tags(tags)
                .peers(peers).customProperties(properties).user(user).build();
    }
}
