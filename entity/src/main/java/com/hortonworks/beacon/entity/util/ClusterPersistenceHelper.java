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
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.ClusterList.ClusterElement;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterKey;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;
import com.hortonworks.beacon.store.executors.ClusterExecutor;
import com.hortonworks.beacon.store.executors.ClusterListExecutor;
import com.hortonworks.beacon.store.executors.ClusterPairExecutor;
import com.hortonworks.beacon.util.ClusterStatus;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * Persistence Cluster helper for Beacon.
 */
public final class ClusterPersistenceHelper {

    private ClusterPersistenceHelper() {
    }

    public static void submitCluster(Cluster cluster) throws BeaconStoreException {
        ClusterBean bean = getClusterBean(cluster);
        ClusterExecutor executor = new ClusterExecutor(bean);
        executor.submitCluster();
    }


    private static ClusterBean getClusterBean(Cluster cluster) {
        ClusterBean bean = new ClusterBean();
        bean.setName(cluster.getName());
        bean.setVersion(cluster.getVersion());
        bean.setDescription(cluster.getDescription());
        bean.setBeaconUri(cluster.getBeaconEndpoint());
        bean.setFsEndpoint(cluster.getFsEndpoint());
        bean.setHsEndpoint(cluster.getHsEndpoint());
        bean.setAtlasEndpoint(cluster.getAtlasEndpoint());
        bean.setRangerEndpoint(cluster.getRangerEndpoint());
        bean.setLocal(cluster.isLocal());
        bean.setTags(cluster.getTags());
        List<ClusterPropertiesBean> propertiesBeans = new ArrayList<>();
        Properties customProperties = cluster.getCustomProperties();
        for (String key : customProperties.stringPropertyNames()) {
            ClusterPropertiesBean propertiesBean = new ClusterPropertiesBean();
            propertiesBean.setName(key);
            propertiesBean.setValue(customProperties.getProperty(key));
            propertiesBeans.add(propertiesBean);
        }
        bean.setCustomProperties(propertiesBeans);
        return bean;
    }

    private static Cluster getCluster(ClusterBean bean) {
        Cluster cluster = new Cluster();
        cluster.setName(bean.getName());
        cluster.setVersion(bean.getVersion());
        cluster.setDescription(bean.getDescription());
        cluster.setBeaconEndpoint(bean.getBeaconUri());
        cluster.setFsEndpoint(bean.getFsEndpoint());
        cluster.setHsEndpoint(bean.getHsEndpoint());
        cluster.setAtlasEndpoint(bean.getAtlasEndpoint());
        cluster.setRangerEndpoint(bean.getRangerEndpoint());
        cluster.setLocal(bean.isLocal());
        cluster.setTags(bean.getTags());
        Properties customProperties = new Properties();
        for (ClusterPropertiesBean propertiesBean : bean.getCustomProperties()) {
            customProperties.put(propertiesBean.getName(), propertiesBean.getValue());
        }
        cluster.setCustomProperties(customProperties);
        // Update the peers information
        List<ClusterPairBean> pairBeans = bean.getClusterPairs();
        ClusterKey key = new ClusterKey(bean.getName(), bean.getVersion());
        StringBuilder peers = new StringBuilder();
        for (ClusterPairBean pairBean : pairBeans) {
            ClusterKey clusterKey = new ClusterKey(pairBean.getClusterName(), pairBean.getClusterVersion());
            ClusterKey pairedClusterKey = new ClusterKey(pairBean.getPairedClusterName(),
                    pairBean.getPairedClusterVersion());
            if (ClusterStatus.PAIRED.name().equals(pairBean.getStatus())) {
                peers = peers.length() > 0 ? peers.append(BeaconConstants.COMMA_SEPARATOR) : peers;
                if (key.equals(clusterKey)) {
                    peers.append(pairedClusterKey.getName());
                } else {
                    peers.append(clusterKey.getName());
                }
            }
        }
        cluster.setPeers(peers.length() > 1 ? peers.toString() : null);
        return cluster;
    }

    public static Cluster getActiveCluster(String clusterName) throws BeaconStoreException {
        ClusterBean bean = new ClusterBean(clusterName);
        ClusterExecutor executor = new ClusterExecutor(bean);
        ClusterBean clusterBean = executor.getActiveCluster();
        return getCluster(clusterBean);
    }

    public static void unpairPairedCluster(Cluster localCluster, Cluster remoteCluster)
            throws BeaconStoreException {
        List<ClusterPairBean> pairedCluster = getPairedCluster(localCluster);
        ClusterKey remoteClusterKey = new ClusterKey(remoteCluster.getName(), remoteCluster.getVersion());

        Date lastModifiedTime = new Date();
        for (ClusterPairBean pairBean : pairedCluster) {
            ClusterKey clusterKey = new ClusterKey(pairBean.getClusterName(), pairBean.getClusterVersion());
            ClusterKey pairedClusterKey = new ClusterKey(pairBean.getPairedClusterName(),
                    pairBean.getPairedClusterVersion());
            if (ClusterStatus.PAIRED.name().equals(pairBean.getStatus())) {
                if (remoteClusterKey.equals(clusterKey) || remoteClusterKey.equals(pairedClusterKey)) {
                    pairBean.setStatus(ClusterStatus.UNPAIRED.name());
                    pairBean.setLastModifiedTime(lastModifiedTime);
                    ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
                    executor.updateStatus();
                }
            }
        }
    }

    private static List<ClusterPairBean> getPairedCluster(Cluster cluster) {
        ClusterPairBean bean = new ClusterPairBean();
        bean.setClusterName(cluster.getName());
        bean.setClusterVersion(cluster.getVersion());
        ClusterPairExecutor executor = new ClusterPairExecutor(bean);
        return executor.getPairedCluster();
    }

    public static void deleteCluster(Cluster cluster) {
        ClusterBean clusterBean = new ClusterBean(cluster.getName());
        clusterBean.setRetirementTime(new Date());
        ClusterExecutor executor = new ClusterExecutor(clusterBean);
        executor.retireCluster();
    }

    public static void unpairAllPairedCluster(Cluster cluster) throws BeaconStoreException {
        List<ClusterPairBean> pairedCluster = getPairedCluster(cluster);
        Date lastModifiedTime = new Date();
        for (ClusterPairBean pairBean : pairedCluster) {
            pairBean.setStatus(ClusterStatus.UNPAIRED.name());
            pairBean.setLastModifiedTime(lastModifiedTime);
            ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
            executor.updateStatus();
        }
    }

    public static ClusterList getFilteredClusters(String fieldStr, String orderBy, String sortOrder,
                                                  Integer offset, Integer resultsPerPage) {
        ClusterListExecutor executor = new ClusterListExecutor();
        long clusterCount = executor.getFilterClusterCount(offset, resultsPerPage);
        List<Cluster> clusters = new ArrayList<>();
        if (clusterCount > 0) {
            List<ClusterBean> filterClusters = executor.getFilterClusters(orderBy, sortOrder, offset, resultsPerPage);
            for (ClusterBean bean : filterClusters) {
                Cluster cluster = new Cluster();
                cluster.setName(bean.getName());
                cluster.setVersion(bean.getVersion());
                List<ClusterPairBean> pairedCluster = getPairedCluster(cluster);
                bean.setClusterPairs(pairedCluster);
                bean.setCustomProperties(Collections.<ClusterPropertiesBean>emptyList());
                cluster = getCluster(bean);
                clusters.add(cluster);
            }
        }
        HashSet<String> fields = new HashSet<>(Arrays.asList(fieldStr.toUpperCase().
                split(BeaconConstants.COMMA_SEPARATOR)));
        return clusters.size() == 0
                ? new ClusterList(new ClusterElement[]{}, 0)
                : new ClusterList(buildClusterElements(fields, clusters), clusterCount);

    }

    private static ClusterElement[] buildClusterElements(HashSet<String> fields, List<Cluster> clusters) {
        ClusterElement[] elements = new ClusterElement[clusters.size()];
        int elementIndex = 0;
        for (Cluster cluster : clusters) {
            elements[elementIndex++] = getClusterElement(cluster, fields);
        }
        return elements;
    }

    private static ClusterElement getClusterElement(Cluster cluster, HashSet<String> fields) {
        ClusterElement elem = new ClusterElement();
        elem.name = cluster.getName();

        if (fields.contains(ClusterList.ClusterFieldList.PEERS.name())) {
            if (StringUtils.isNotBlank(cluster.getPeers())) {
                String[] peers = cluster.getPeers().split(BeaconConstants.COMMA_SEPARATOR);
                elem.peer = Arrays.asList(peers);
            } else {
                elem.peer = new ArrayList<>();
            }
        }

        if (fields.contains(ClusterList.ClusterFieldList.TAGS.name())) {
            elem.tag = ClusterHelper.getTags(cluster);
        }
        return elem;
    }

    public static void pairCluster(Cluster localCluster, Cluster remoteCluster) throws BeaconStoreException {
        ClusterPairBean bean = new ClusterPairBean();
        bean.setClusterName(localCluster.getName());
        bean.setClusterVersion(localCluster.getVersion());
        bean.setPairedClusterName(remoteCluster.getName());
        bean.setPairedClusterVersion(remoteCluster.getVersion());
        bean.setStatus(ClusterStatus.PAIRED.name());
        bean.setLastModifiedTime(new Date());
        ClusterPairExecutor executor = new ClusterPairExecutor(bean);
        executor.pairCluster();
    }

    public static void unPairOrPairCluster(Cluster localCluster, Cluster remoteCluster, ClusterStatus status)
            throws BeaconStoreException {
        ClusterPairBean bean = new ClusterPairBean();
        bean.setClusterName(localCluster.getName());
        bean.setClusterVersion(localCluster.getVersion());
        bean.setPairedClusterName(remoteCluster.getName());
        bean.setPairedClusterVersion(remoteCluster.getVersion());
        bean.setStatus(status.name());
        bean.setLastModifiedTime(new Date());
        ClusterPairExecutor executor = new ClusterPairExecutor(bean);
        executor.updateStatus();
    }

    static Cluster getLocalCluster() throws BeaconException {
        ClusterBean bean = new ClusterBean();
        bean.setLocal(true);
        ClusterExecutor executor = new ClusterExecutor(bean);
        ClusterBean localCluster = executor.getLocalClusterName();
        return getCluster(localCluster);
    }
}
