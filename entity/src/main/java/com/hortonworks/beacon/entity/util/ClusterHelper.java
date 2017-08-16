/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;

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
        String[] parts = clusterName.split(BeaconConstants.CLUSTER_NAME_SEPARATOR_REGEX);
        String name = parts.length == 2 ? parts[1] : parts[0];
        return name.equalsIgnoreCase(BeaconConfig.getInstance().getEngine().getLocalClusterName());
    }

    public static Cluster getLocalCluster() throws BeaconException {
        return ClusterPersistenceHelper.getLocalCluster();
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
            throw new BeaconException(MessageCode.COMM_010008.name(), clusterName);
        }
        return ClusterPersistenceHelper.getActiveCluster(clusterName);
    }

    public static void validateIfClustersPaired(String sourceCluster, String targetCluster) throws BeaconException {
        Cluster cluster = ClusterPersistenceHelper.getActiveCluster(sourceCluster);
        boolean paired = areClustersPaired(cluster, targetCluster);
        if (!paired) {
            throw new ValidationException(MessageCode.ENTI_000004.name(), sourceCluster, targetCluster);
        }
    }
}
