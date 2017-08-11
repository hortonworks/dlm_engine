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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for Cluster.
 */
public class ClusterExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(ClusterExecutor.class);

    /**
     * Enum for Cluster named queries.
     */
    enum ClusterQuery {
        GET_CLUSTER_LATEST,
        GET_CLUSTER_ACTIVE,
        RETIRE_CLUSTER
    }

    private ClusterBean bean;

    public ClusterExecutor(ClusterBean bean) {
        this.bean = bean;
    }

    private void execute(EntityManager entityManager) throws BeaconStoreException {
        try {
            entityManager.getTransaction().begin();
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
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            throw new BeaconStoreException(e.getMessage(), e);
        }
    }

    private void execute() throws BeaconStoreException {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            execute(entityManager);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
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
        LOG.info(MessageCode.PERS_000013.name(), bean.getName(), bean.getVersion());
        return bean;
    }

    private ClusterBean getLatestCluster() {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(entityManager, ClusterQuery.GET_CLUSTER_LATEST);
            List resultList = query.getResultList();
            return resultList == null || resultList.isEmpty() ? null : (ClusterBean) resultList.get(0);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private Query getQuery(EntityManager entityManager, ClusterQuery namedQuery) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_CLUSTER_LATEST:
                query.setParameter("name", bean.getName());
                break;
            case GET_CLUSTER_ACTIVE:
                query.setParameter("name", bean.getName());
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
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(entityManager, ClusterQuery.GET_CLUSTER_ACTIVE);
            List resultList = query.getResultList();
            ClusterBean clusterBean = getSingleResult(resultList);
            updateClusterProp(clusterBean);
            updateClusterPair(clusterBean);
            return clusterBean;
        } catch (Exception e) {
            LOG.error(MessageCode.PERS_000014.name(), bean.getName());
            throw new BeaconStoreException(e.getMessage(), e);
        } finally {
            STORE.closeEntityManager(entityManager);
        }
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
            throw new BeaconStoreException(MessageCode.PERS_000003.name(), bean.getName());
        } else if (resultList.size() > 1) {
            LOG.error(MessageCode.PERS_000009.name(), bean.getName());
            throw new BeaconStoreException(MessageCode.PERS_000009.name(), bean.getName());
        } else {
            return (ClusterBean) resultList.get(0);
        }
    }

    public void retireCluster() {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(entityManager, ClusterQuery.RETIRE_CLUSTER);
            entityManager.getTransaction().begin();
            int executeUpdate = query.executeUpdate();
            entityManager.getTransaction().commit();
            LOG.info(MessageCode.PERS_000015.name(), bean.getName(), executeUpdate);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
