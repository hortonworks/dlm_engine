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
import com.hortonworks.beacon.client.entity.PeerInfo;
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
import com.hortonworks.beacon.store.executors.ClusterUpdateExecutor;
import com.hortonworks.beacon.util.ClusterStatus;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Persistence Cluster helper for Beacon.
 */
public final class ClusterDao {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterDao.class);

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

    private Cluster getCluster(ClusterBean bean) {
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
        List<PeerInfo> peersInfo = new ArrayList<>();
        for (ClusterPairBean pairBean : pairBeans) {
            ClusterKey clusterKey = new ClusterKey(pairBean.getClusterName(), pairBean.getClusterVersion());
            ClusterKey pairedClusterKey = new ClusterKey(pairBean.getPairedClusterName(),
                    pairBean.getPairedClusterVersion());
            if (ClusterStatus.PAIRED.name().equals(pairBean.getStatus())
                    || ClusterStatus.SUSPENDED.name().equals(pairBean.getStatus())) {
                PeerInfo peerInfo = new PeerInfo();
                peers = peers.length() > 0 ? peers.append(BeaconConstants.COMMA_SEPARATOR) : peers;
                if (key.equals(clusterKey)) {
                    peers.append(pairedClusterKey.getName());
                    peerInfo.setClusterName(pairedClusterKey.getName());
                } else {
                    peers.append(clusterKey.getName());
                    peerInfo.setClusterName(clusterKey.getName());
                }
                peerInfo.setPairStatus(pairBean.getStatus());
                peersInfo.add(peerInfo);
            }
        }
        cluster.setPeers(peers.length() > 1 ? peers.toString() : null);
        cluster.setPeersInfo(!peersInfo.isEmpty() ? peersInfo : null);
        return cluster;
    }

    public Cluster getActiveCluster(String clusterName) throws BeaconStoreException {
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

    private List<ClusterPairBean> getPairedCluster(Cluster cluster) {
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

    public void unpairAllPairedCluster(Cluster cluster) throws BeaconStoreException {
        List<ClusterPairBean> pairedCluster = getPairedCluster(cluster);
        Date lastModifiedTime = new Date();
        for (ClusterPairBean pairBean : pairedCluster) {
            pairBean.setStatus(ClusterStatus.UNPAIRED.name());
            pairBean.setLastModifiedTime(lastModifiedTime);
            ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
            executor.updateStatus();
        }
    }

    public void movePairStatusForClusters(Cluster cluster, Set<String> peerClusters, ClusterStatus fromStatus,
                                           ClusterStatus toStatus) throws BeaconStoreException {
        List<ClusterPairBean> pairedCluster = getPairedCluster(cluster);
        Date lastModifiedTime = new Date();
        for (ClusterPairBean pairBean : pairedCluster) {
            String pairedClusterName = cluster.getName().equals(pairBean.getClusterName())
                    ? pairBean.getPairedClusterName() :pairBean.getClusterName();
            if (fromStatus.name().equals(pairBean.getStatus())) {
                if (peerClusters.contains(pairedClusterName)) {
                    pairBean.setStatus(toStatus.name());
                    pairBean.setLastModifiedTime(lastModifiedTime);
                    ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
                    executor.updateStatus();
                    LOG.info("Moving the cluster pair status for clusters [{}] and [{}] from [{}] to [{}]",
                            cluster.getName(), pairedClusterName, fromStatus.name(), toStatus.name());
                }
            }
        }
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

    private ClusterElement[] buildClusterElements(HashSet<String> fields, List<Cluster> clusters) {
        ClusterElement[] elements = new ClusterElement[clusters.size()];
        int elementIndex = 0;
        for (Cluster cluster : clusters) {
            elements[elementIndex++] = getClusterElement(cluster, fields);
        }
        return elements;
    }

    private ClusterElement getClusterElement(Cluster cluster, HashSet<String> fields) {
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

        if (fields.contains(ClusterList.ClusterFieldList.PEERSINFO.name())) {
            if (cluster.getPeersInfo() != null && !cluster.getPeersInfo().isEmpty()) {
                elem.peersInfo = cluster.getPeersInfo();
            } else {
                elem.peersInfo = new ArrayList<>();
            }
        }

        if (fields.contains(ClusterList.ClusterFieldList.TAGS.name())) {
            elem.tag = ClusterHelper.getTags(cluster);
        }
        return elem;
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

    Cluster getLocalCluster() throws BeaconException {
        ClusterBean bean = new ClusterBean();
        bean.setLocal(true);
        ClusterExecutor executor = new ClusterExecutor(bean);
        ClusterBean localCluster = executor.getLocalClusterName();
        return getCluster(localCluster);
    }

    public void persistUpdatedCluster(Cluster updatedCluster, PropertiesIgnoreCase updatedProps,
                                             PropertiesIgnoreCase newProps) {
        ClusterUpdateExecutor executor = new ClusterUpdateExecutor();
        executor.persistUpdatedCluster(getClusterBean(updatedCluster), updatedProps, newProps);
    }
}
