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
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper util class for Beacon Cluster resource.
 */
public final class ClusterHelper {
    private ClusterHelper() {
    }

    public static boolean areClustersPaired(final Cluster localCluster, final String remoteCluster)
            throws BeaconException {
        String clusterPeers = localCluster.getPeers();
        if (StringUtils.isNotBlank(clusterPeers)) {
            String[] peers = clusterPeers.split(BeaconConstants.COMMA_SEPARATOR);
            for (String peer : peers) {
                if (peer.trim().equalsIgnoreCase(remoteCluster)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isLocalCluster(final String clusterName) {
        return clusterName.equalsIgnoreCase(BeaconConfig.getInstance().getEngine().getLocalClusterName());
    }

    public static Cluster getLocalCluster() throws BeaconException {
        return getActiveCluster(BeaconConfig.getInstance().getEngine().getLocalClusterName());
    }

    static List<String> getTags(Entity entity) {
        String rawTags = entity.getTags();

        List<String> tags = new ArrayList<>();
        if (!StringUtils.isEmpty(rawTags)) {
            for (String tag : rawTags.split(BeaconConstants.COMMA_SEPARATOR)) {
                tags.add(tag.trim());
            }
        }
        return tags;
    }

    public static Cluster getActiveCluster(String clusterName) throws BeaconException {
        if (StringUtils.isBlank(clusterName)) {
            throw new BeaconException(clusterName + " cannot be null or empty");
        }
        return ClusterPersistenceHelper.getActiveCluster(clusterName);
    }

    public static void validateIfClustersPaired(String sourceCluster, String targetCluster) throws BeaconException {
        Cluster cluster = ClusterPersistenceHelper.getActiveCluster(sourceCluster);
        boolean paired = areClustersPaired(cluster, targetCluster);
        if (!paired) {
            throw new ValidationException("Clusters " + sourceCluster + " and " + targetCluster + " are not paired. "
                    + "Pair the clusters before submitting or scheduling the policy");
        }
    }
}
