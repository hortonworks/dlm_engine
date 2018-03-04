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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Helper util class for Beacon Cluster resource.
 */
public final class ClusterHelper {

    private static ClusterDao clusterDao = new ClusterDao();

    private ClusterHelper() {
    }

    public static boolean isHighlyAvailableHDFS(Properties properties) {
        return properties.containsKey(BeaconConstants.DFS_NAMESERVICES);
    }

    public static boolean isHighlyAvailableHive(String hsEndPoint) {
        if (StringUtils.isNotBlank(hsEndPoint)) {
            return hsEndPoint.contains("serviceDiscoveryMode=zooKeeper");
        }
        return false;
    }

    public static boolean isKerberized(Cluster cluster) {
        Properties customProperties = cluster.getCustomProperties();
        boolean isKerberized = customProperties.containsKey(BeaconConstants.NN_PRINCIPAL);
        if (isHiveEnabled(cluster.getHsEndpoint())) {
            isKerberized &= customProperties.containsKey(BeaconConstants.HIVE_PRINCIPAL);
        }

        return  isKerberized;
    }

    public static boolean isHiveEnabled(String hsEndPoint) {
        return StringUtils.isNotBlank(hsEndPoint);
    }

    public static boolean isHiveEnabled(Cluster cluster) {
        return StringUtils.isNotBlank(cluster.getHsEndpoint()) || StringUtils.isNotBlank(cluster.getHmsEndpoint());
    }

    public static boolean isRangerEnabled(String rangerEndPoint) {
        return StringUtils.isNotBlank(rangerEndPoint);
    }

    public static Configuration getHAConfigurationOrDefault(String clusterName) throws BeaconException {
        return getHAConfigurationOrDefault(getActiveCluster(clusterName));
    }

    public static Configuration getHAConfigurationOrDefault(Cluster cluster) {
        Configuration conf = new Configuration();
        if (isHighlyAvailableHDFS(cluster.getCustomProperties())) {
            for (Map.Entry<Object, Object> property : cluster.getCustomProperties().entrySet()) {
                if (property.getKey().toString().startsWith("dfs.")) {
                    conf.set(property.getKey().toString(), property.getValue().toString());
                }
            }
        }
        return conf;
    }

    public static boolean areClustersPaired(final String sourceCluster, final String remoteCluster)
            throws BeaconException {
        Cluster cluster = getActiveCluster(sourceCluster);
        return areClustersPaired(cluster, remoteCluster);
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
        return clusterDao.getLocalCluster();
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
            throw new BeaconException("{} cannot be null or empty", clusterName);
        }
        return clusterDao.getActiveCluster(clusterName);
    }

    public static void validateIfClustersPaired(String sourceCluster, String targetCluster) throws BeaconException {
        Cluster cluster = clusterDao.getActiveCluster(sourceCluster);
        boolean paired = areClustersPaired(cluster, targetCluster);
        if (!paired) {
            throw new ValidationException(
                "Clusters {} and {} are not paired. Pair the clusters before submitting or scheduling the policy",
                sourceCluster, targetCluster);
        }
    }
}
