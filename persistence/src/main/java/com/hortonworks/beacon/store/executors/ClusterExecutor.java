/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon store executor for Cluster.
 */
public class ClusterExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterExecutor.class);

    /**
     * Enum for Cluster named queries.
     */
    enum ClusterQuery {
        GET_CLUSTER_LATEST,
        GET_CLUSTER_ACTIVE,
        GET_CLUSTER_LOCAL,
        RETIRE_CLUSTER
    }

    private ClusterBean bean;

    public ClusterExecutor(ClusterBean bean) {
        this.bean = bean;
    }

    private void execute() throws BeaconStoreException {
        entityManager.persist(bean);
        String clusterName = bean.getName();
        int version = bean.getVersion();
        Date createdTime = bean.getCreationTime();
        for (ClusterPropertiesBean propertiesBean : bean.getCustomProperties()) {
            propertiesBean.setClusterName(clusterName);
            propertiesBean.setClusterVersion(version);
            propertiesBean.setCreatedTime(createdTime);
            entityManager.persist(propertiesBean);
        }
    }

    public ClusterBean submitCluster() throws BeaconStoreException {
        ClusterBean latestCluster = getLatestCluster();
        if (latestCluster == null) {
            bean.setVersion(1);
        } else if (latestCluster.getRetirementTime() != null) {
            bean.setVersion(latestCluster.getVersion() + 1);
        } else {
            throw new BeaconStoreException(MessageCode.PERS_000001.name(), latestCluster.getName(),
                    latestCluster.getVersion());
        }
        Date time = new Date();
        bean.setCreationTime(time);
        bean.setLastModifiedTime(time);
        bean.setChangeId(1);
        bean.setRetirementTime(null);
        execute();
        LOG.info("ClusterBean name: [{}], version: [{}] is stored.", bean.getName(), bean.getVersion());
        return bean;
    }

    private ClusterBean getLatestCluster() {
        Query query = getQuery(ClusterQuery.GET_CLUSTER_LATEST);
        List resultList = query.getResultList();
        return resultList == null || resultList.isEmpty() ? null : (ClusterBean) resultList.get(0);
    }

    private Query getQuery(ClusterQuery namedQuery) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_CLUSTER_LATEST:
                query.setParameter("name", bean.getName());
                break;
            case GET_CLUSTER_ACTIVE:
                query.setParameter("name", bean.getName());
                break;
            case GET_CLUSTER_LOCAL:
                query.setParameter("local", bean.isLocal());
                break;
            case RETIRE_CLUSTER:
                query.setParameter("name", bean.getName());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return query;
    }

    public ClusterBean getActiveCluster() throws BeaconStoreException {
        Query query = getQuery(ClusterQuery.GET_CLUSTER_ACTIVE);
        List resultList = query.getResultList();
        ClusterBean clusterBean = getSingleResult(resultList);
        updateClusterProp(clusterBean);
        updateClusterPair(clusterBean);
        return clusterBean;
    }

    private void updateClusterPair(final ClusterBean clusterBean) {
        ClusterPairBean pairBean = new ClusterPairBean();
        pairBean.setClusterName(clusterBean.getName());
        pairBean.setClusterVersion(clusterBean.getVersion());
        ClusterPairExecutor executor = new ClusterPairExecutor(pairBean);
        List<ClusterPairBean> pairedCluster = executor.getPairedCluster();
        clusterBean.setClusterPairs(pairedCluster);
    }

    private void updateClusterProp(final ClusterBean clusterBean) throws BeaconStoreException {
        ClusterPropertiesBean propertiesBean = new ClusterPropertiesBean(clusterBean.getName(),
                clusterBean.getVersion());
        ClusterPropertiesExecutor executor = new ClusterPropertiesExecutor(propertiesBean);
        List<ClusterPropertiesBean> clusterProperties = executor.getClusterProperties();
        clusterBean.setCustomProperties(clusterProperties);
    }

    private ClusterBean getSingleResult(List resultList) throws BeaconStoreException {
        if (resultList == null || resultList.isEmpty()) {
            throw new NoSuchElementException(
                    ResourceBundleService.getService().getString(MessageCode.PERS_000003.name(), bean.getName()));
        } else if (resultList.size() > 1) {
            LOG.error("Beacon data store is in inconsistent state. More than 1 result found.Cluster name: {}",
                bean.getName());
            throw new BeaconStoreException(MessageCode.PERS_000009.name(), bean.getName());
        } else {
            return (ClusterBean) resultList.get(0);
        }
    }

    public void retireCluster() {
        Query query = getQuery(ClusterQuery.RETIRE_CLUSTER);
        int executeUpdate = query.executeUpdate();
        LOG.info("Cluster name [{}] deleted, record updated [{}].", bean.getName(), executeUpdate);
    }

    public ClusterBean getLocalClusterName() throws BeaconStoreException {
        Query query = getQuery(ClusterQuery.GET_CLUSTER_LOCAL);
        List resultList = query.getResultList();
        if (resultList == null || resultList.isEmpty()) {
            throw new NoSuchElementException(MessageCode.PERS_000031.name());
        } else {
            ClusterBean clusterBean = getSingleResult(resultList);
            updateClusterProp(clusterBean);
            updateClusterPair(clusterBean);
            return clusterBean;
        }
    }
}
