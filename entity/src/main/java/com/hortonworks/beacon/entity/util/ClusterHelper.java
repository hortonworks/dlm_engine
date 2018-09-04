/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.entityNeo.DataSet;
import com.hortonworks.beacon.entity.entityNeo.FSDataSet;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.ClusterStatus;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;

/**
 * Helper util class for Beacon Cluster resource.
 */
public final class ClusterHelper {

    private static ClusterDao clusterDao = new ClusterDao();

    private ClusterHelper() {
    }

    public static List<String> convertToList(String tags) {
        List<String> tagList = new ArrayList<>();
        if (StringUtils.isNotBlank(tags)) {
            String[] strings = tags.split(BeaconConstants.COMMA_SEPARATOR);
            tagList = Arrays.asList(strings);
        }
        return tagList;
    }


    public static String convertToString(List<String> tags) {
        StringBuilder tagString = new StringBuilder();
        if (tags == null) {
            return tagString.toString();
        }
        for(String tag : tags) {
            tagString = tagString.length() > 0 ? tagString.append(BeaconConstants.COMMA_SEPARATOR) : tagString;
            tagString.append(tag.trim());
        }
        return tagString.toString();
    }

    public static boolean isHDFSEnabled(Cluster cluster) {
        return StringUtils.isNotEmpty(cluster.getFsEndpoint());
    }

    public static boolean isHDFSEnabled(Properties properties) {
        return properties.containsKey(Cluster.ClusterFields.FSENDPOINT.getName());
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
        boolean isKerberized = StringUtils.isNotBlank(customProperties.getProperty(BeaconConstants.NN_PRINCIPAL));
        if (isHiveEnabled(cluster.getHsEndpoint())) {
            isKerberized &= StringUtils.isNotBlank(customProperties.getProperty(BeaconConstants.HIVE_PRINCIPAL));
        }

        return  isKerberized;
    }

    public static boolean isHiveEnabled(String hsEndPoint) {
        return StringUtils.isNotBlank(hsEndPoint);
    }

    public static boolean isHiveEnabled(BeaconCluster cluster) {
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

    public static String getHDFSNameservices(Properties properties) {
        String nsEntries = properties.getProperty(BeaconConstants.DFS_INTERNAL_NAMESERVICES,
                properties.getProperty(BeaconConstants.DFS_NAMESERVICES));
        return nsEntries;
    }

    public static List<String> getHDFSNameservicesList(Properties properties) {
        List<String> nsList = new ArrayList<>();
        String nsEntries = getHDFSNameservices(properties);
        if (StringUtils.isNotBlank(nsEntries)) {
            String[] nsIDs = nsEntries.split(BeaconConstants.COMMA_SEPARATOR);
            for (String nsID: nsIDs) {
                nsList.add(nsID.trim());
            }
        }
        return nsList;
    }

    public static boolean areClustersPaired(final String sourceCluster, final String remoteCluster)
            throws BeaconException {
        Cluster cluster = getActiveCluster(sourceCluster);
        return areClustersPaired(cluster, remoteCluster);
    }

    public static boolean areClustersPaired(final Cluster localCluster, final String remoteCluster) {
        List<String> clusterPeers = localCluster.getPeers();
        for (String peer : clusterPeers) {
            if (peer.equalsIgnoreCase(remoteCluster)) {
                return true;
            }
        }
        return false;
    }

    public static boolean areClustersSuspended(final String sourceCluster, final String remoteCluster)
            throws BeaconException {
        ClusterStatus clusterStatus = clusterDao.getPairedClusterStatus(sourceCluster, remoteCluster);
        return ClusterStatus.SUSPENDED.equals(clusterStatus);
    }

    public static boolean isLocalCluster(final String clusterName) {
        String[] parts = clusterName.split(BeaconConstants.CLUSTER_NAME_SEPARATOR_REGEX);
        String name = parts.length == 2 ? parts[1] : parts[0];
        return name.equalsIgnoreCase(BeaconConfig.getInstance().getEngine().getLocalClusterName());
    }

    public static String getKnoxProxyURL(final Cluster cluster) {
        Properties props = cluster.getCustomProperties();
        return props == null ? null : (String)props.get(KnoxTokenUtils.KNOX_GATEWAY_URL);
    }

    public static Cluster getLocalCluster() throws BeaconException {
        return clusterDao.getLocalCluster();
    }

    static List<String> getTags(Entity entity) {
        return entity.getTags();
    }

    public static Cluster getActiveCluster(String clusterName) throws BeaconException {
        if (StringUtils.isBlank(clusterName)) {
            throw new BeaconException("Cluster name cannot be null or empty");
        }
        return clusterDao.getActiveCluster(clusterName);
    }

    public static boolean isCloudEncryptionEnabled(Cluster cluster, ReplicationPolicy policy) throws BeaconException {
        DataSet dataSet = FSDataSet.create(new BeaconCluster(cluster).getHiveWarehouseLocation(),
                cluster.getName(), policy);
        return dataSet.isEncrypted();
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

    public static List<String> getTagsList(String tags) {
        List<String> tagList = new ArrayList<>();
        if (org.apache.commons.lang.StringUtils.isNotBlank(tags)) {
            tags = tags.substring(1, tags.length());
            tags = tags.substring(0, tags.length() - 1);
            String[] strings = tags.split(BeaconConstants.COMMA_SEPARATOR);
            tagList = Arrays.asList(strings);
        }
        return tagList;

    }
}
