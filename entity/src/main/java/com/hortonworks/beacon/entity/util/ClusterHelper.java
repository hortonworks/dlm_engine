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
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;

/**
 * Helper util class for Beacon Cluster resource.
 */
public final class ClusterHelper {
    private ClusterHelper() {
    }

    public static String[] getPeers(final String clusterName) throws BeaconException {
        Cluster cluster = EntityHelper.getEntity(EntityType.CLUSTER, clusterName);
        String clusterPeers = cluster.getPeers();
        String[] peers = null;
        if (StringUtils.isNotBlank(clusterPeers)) {
            peers = clusterPeers.split(BeaconConstants.COMMA_SEPARATOR);
        }
        return peers;
    }

    public static void updatePeers(final Cluster cluster, final String newPeer) {
        String pairedWith = cluster.getPeers();
        if (StringUtils.isBlank(pairedWith)) {
            cluster.setPeers(newPeer);
        } else {
            cluster.setPeers(pairedWith.concat(BeaconConstants.COMMA_SEPARATOR).concat(newPeer));
        }
    }

    public static void resetPeers(final Cluster cluster, final String peers) {
        cluster.setPeers(peers);
    }

    public static boolean areClustersPaired(final String localCluster,
                                            final String remoteCluster) throws BeaconException {
        String[] peers = getPeers(localCluster);
        if (peers != null && peers.length > 0) {
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(remoteCluster)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isLocalCluster(final String clusterName) {
        return clusterName.equalsIgnoreCase(BeaconConfig.getInstance().getEngine().getLocalClusterName())
                ? true : false;
    }
}
