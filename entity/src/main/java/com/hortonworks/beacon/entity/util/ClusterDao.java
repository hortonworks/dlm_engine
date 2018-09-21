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

import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.PeerInfo;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterKey;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;
import com.hortonworks.beacon.store.executors.ClusterExecutor;
import com.hortonworks.beacon.store.executors.ClusterListExecutor;
import com.hortonworks.beacon.store.executors.ClusterPairExecutor;
import com.hortonworks.beacon.store.executors.ClusterUpdateExecutor;
import com.hortonworks.beacon.util.ClusterStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static com.hortonworks.beacon.entity.util.ClusterHelper.convertToList;
import static com.hortonworks.beacon.entity.util.ClusterHelper.convertToString;

/**
 * Persistence Cluster helper for Beacon.
 */
public final class ClusterDao {

    public void submitCluster(Cluster cluster) {
        ClusterBean bean = getClusterBean(cluster);
        ClusterExecutor executor = new ClusterExecutor(bean);
        executor.submitCluster();
    }

    private ClusterBean getClusterBean(Cluster cluster) {
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
        bean.setTags(convertToString(cluster.getTags()));
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

    private BeaconCluster getCluster(ClusterBean bean) {
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
        cluster.setTags(convertToList(bean.getTags()));
        Properties customProperties = new Properties();
        if (bean.getCustomProperties() != null) {
            for (ClusterPropertiesBean propertiesBean : bean.getCustomProperties()) {
                customProperties.put(propertiesBean.getName(), propertiesBean.getValue());
            }
        }
        cluster.setCustomProperties(customProperties);
        // Update the peers information
        List<ClusterPairBean> pairBeans = bean.getClusterPairs();
        ClusterKey key = new ClusterKey(bean.getName(), bean.getVersion());
        List<String> peers = new ArrayList<>();
        List<PeerInfo> peersInfo = new ArrayList<>();
        if (pairBeans != null) {
            for (ClusterPairBean pairBean : pairBeans) {
                ClusterKey clusterKey = new ClusterKey(pairBean.getClusterName(), pairBean.getClusterVersion());
                ClusterKey pairedClusterKey = new ClusterKey(pairBean.getPairedClusterName(),
                        pairBean.getPairedClusterVersion());
                if (ClusterStatus.PAIRED.name().equals(pairBean.getStatus())
                        || ClusterStatus.SUSPENDED.name().equals(pairBean.getStatus())) {
                    PeerInfo peerInfo = new PeerInfo();
                    if (key.equals(clusterKey)) {
                        peers.add(pairedClusterKey.getName());
                        peerInfo.setClusterName(pairedClusterKey.getName());
                    } else {
                        peers.add(clusterKey.getName());
                        peerInfo.setClusterName(clusterKey.getName());
                    }
                    peerInfo.setPairStatus(pairBean.getStatus());
                    peerInfo.setStatusMessage(pairBean.getStatusMessage());
                    peersInfo.add(peerInfo);
                }
            }
        }
        cluster.setPeers(peers);
        cluster.setPeersInfo(peersInfo);
        return new BeaconCluster(cluster);
    }

    public BeaconCluster getActiveCluster(String clusterName) throws BeaconStoreException {
        ClusterBean bean = new ClusterBean(clusterName);
        ClusterExecutor executor = new ClusterExecutor(bean);
        ClusterBean clusterBean = executor.getActiveCluster();
        return getCluster(clusterBean);
    }

    public void unpairPairedCluster(Cluster localCluster, Cluster remoteCluster)
            throws BeaconStoreException {
        List<ClusterPairBean> pairedCluster = getPairedCluster(localCluster);
        ClusterKey remoteClusterKey = new ClusterKey(remoteCluster.getName(), remoteCluster.getVersion());

        Date lastModifiedTime = new Date();
        for (ClusterPairBean pairBean : pairedCluster) {
            ClusterKey clusterKey = new ClusterKey(pairBean.getClusterName(), pairBean.getClusterVersion());
            ClusterKey pairedClusterKey = new ClusterKey(pairBean.getPairedClusterName(),
                    pairBean.getPairedClusterVersion());
            if (ClusterStatus.PAIRED.name().equals(pairBean.getStatus())
                    || ClusterStatus.SUSPENDED.name().equals(pairBean.getStatus())) {
                if (remoteClusterKey.equals(clusterKey) || remoteClusterKey.equals(pairedClusterKey)) {
                    pairBean.setStatus(ClusterStatus.UNPAIRED.name());
                    pairBean.setLastModifiedTime(lastModifiedTime);
                    ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
                    executor.updateStatus();
                }
            }
        }
    }

    public List<ClusterPairBean> getPairedCluster(Cluster cluster) {
        ClusterPairBean bean = new ClusterPairBean();
        bean.setClusterName(cluster.getName());
        bean.setClusterVersion(cluster.getVersion());
        ClusterPairExecutor executor = new ClusterPairExecutor(bean);
        return executor.getPairedCluster();
    }

    public void deleteCluster(Cluster cluster) {
        ClusterBean clusterBean = new ClusterBean(cluster.getName());
        clusterBean.setRetirementTime(new Date());
        ClusterExecutor executor = new ClusterExecutor(clusterBean);
        executor.retireCluster();
    }

    public void updatePairStatus(ClusterPairBean clusterPairBean, ClusterStatus updatedStatus)
            throws BeaconStoreException {
        updatePairStatus(clusterPairBean, updatedStatus, null);
    }

    public void updatePairStatus(ClusterPairBean clusterPairBean, ClusterStatus updatedStatus, String statusChangeCause)
            throws BeaconStoreException {
        Date lastModifiedTime = new Date();
        clusterPairBean.setStatus(updatedStatus.name());
        clusterPairBean.setLastModifiedTime(lastModifiedTime);
        if (statusChangeCause != null) {
            clusterPairBean.setStatusMessage(statusChangeCause);
        }
        ClusterPairExecutor executor = new ClusterPairExecutor(clusterPairBean);
        executor.updateStatus();
    }

    public ClusterStatus getPairedClusterStatus(String cluster, String pairedCluster) throws BeaconException {
        Cluster curCluster = getActiveCluster(cluster);
        List<ClusterPairBean> pairedClusterBeans = getPairedCluster(curCluster);
        for (ClusterPairBean pairBean : pairedClusterBeans) {
            if (pairBean.getClusterName().equals(pairedCluster)
                    || pairBean.getPairedClusterName().equals(pairedCluster)) {
                try {
                    return ClusterStatus.valueOf(pairBean.getStatus());
                } catch (IllegalArgumentException ex) {
                    throw new BeaconException(ex, "Cluster pairing status for cluster {} and cluster {} invalid:",
                            pairBean.getStatus(), cluster, pairedCluster);
                }
            }
        }
        throw new BeaconException("Cluster pairing for cluster {} and cluster {} not found", cluster, pairedCluster);
    }

    public ClusterList getFilteredClusters(String fieldStr, String orderBy, String sortOrder,
                                                  Integer offset, Integer resultsPerPage) throws BeaconException {
        ClusterListExecutor executor = new ClusterListExecutor();
        long clusterCount = executor.getFilterClusterCount(offset, resultsPerPage);
        List<ClusterBean> filterClusters = new ArrayList<>();
        if (clusterCount > 0) {
            filterClusters = executor.getFilterClusters(orderBy, sortOrder, offset, resultsPerPage);
        }
        HashSet<String> fields = new HashSet<>(Arrays.asList(fieldStr.toUpperCase().
                split(BeaconConstants.COMMA_SEPARATOR)));
        return clusterCount == 0
                ? new ClusterList(new Cluster[]{}, 0)
                : new ClusterList(buildClusterElements(fields, filterClusters), clusterCount);

    }

    private Cluster[] buildClusterElements(HashSet<String> fields, List<ClusterBean> clusterBeans)
            throws BeaconStoreException {
        Cluster[] elements = new Cluster[clusterBeans.size()];
        int elementIndex = 0;
        for (ClusterBean clusterBean : clusterBeans) {
            elements[elementIndex++] = new Cluster(getClusterElement(clusterBean, fields));
        }
        return elements;
    }

    private Cluster getClusterElement(ClusterBean clusterBean, HashSet<String> fields) throws BeaconStoreException {
        Cluster cluster;
        if (fields.contains(ClusterList.ClusterFieldList.ALL.name())) {
            ClusterExecutor clusterExecutor = new ClusterExecutor(clusterBean);
            clusterExecutor.updateClusterPair(clusterBean);
            clusterExecutor.updateClusterProp(clusterBean);
            cluster = getCluster(clusterBean);
        } else {
            if (fields.contains(ClusterList.ClusterFieldList.PEERS.name())
                    || fields.contains(ClusterList.ClusterFieldList.PEERSINFO.name())) {
                ClusterExecutor clusterExecutor = new ClusterExecutor(clusterBean);
                clusterExecutor.updateClusterPair(clusterBean);
            }
            cluster = getCluster(clusterBean);
            if (!fields.contains(ClusterList.ClusterFieldList.PEERS.name())) {
                cluster.setPeers(new ArrayList<String>());
            }

            if (!fields.contains(ClusterList.ClusterFieldList.PEERSINFO.name())) {
                cluster.setPeersInfo(new ArrayList<PeerInfo>());
            }

            if (!fields.contains(ClusterList.ClusterFieldList.TAGS.name())) {
                cluster.setTags(new ArrayList<String>());
            }
        }
        return cluster;
    }

    public void pairCluster(Cluster localCluster, Cluster remoteCluster) throws BeaconStoreException {
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

    BeaconCluster getLocalCluster() throws BeaconException {
        ClusterBean bean = new ClusterBean();
        bean.setLocal(true);
        ClusterExecutor executor = new ClusterExecutor(bean);
        ClusterBean localCluster = executor.getLocalClusterName();
        return getCluster(localCluster);
    }

    public void persistUpdatedCluster(Cluster updatedCluster, PropertiesIgnoreCase updatedProps,
                                             PropertiesIgnoreCase newProps, PropertiesIgnoreCase deletedProps) {
        ClusterUpdateExecutor executor = new ClusterUpdateExecutor();
        executor.persistUpdatedCluster(getClusterBean(updatedCluster), updatedProps, newProps, deletedProps);
    }
}
