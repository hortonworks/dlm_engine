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

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for Cluster.
 */
public class ClusterExecutor {

    private BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);

    private static final Logger LOG = LoggerFactory.getLogger(ClusterExecutor.class);

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
        entityManager.close();
    }

    private void execute() throws BeaconStoreException {
        EntityManager entityManager = store.getEntityManager();
        execute(entityManager);
    }

    public ClusterBean submitCluster() throws BeaconStoreException {
        ClusterBean latestCluster = getLatestCluster();
        if (latestCluster == null) {
            bean.setVersion(1);
        } else if (latestCluster.getRetirementTime() != null) {
            bean.setVersion(latestCluster.getVersion() + 1);
        } else {
            throw new BeaconStoreException("Cluster entity already exists with name: " + latestCluster.getName()
                    + " version: " + latestCluster.getVersion());
        }
        Date time = new Date();
        bean.setCreationTime(time);
        bean.setLastModifiedTime(time);
        bean.setChangeId(1);
        bean.setRetirementTime(null);
        execute();
        LOG.info("ClusterBean name: [{}], version [{}] is stored.", bean.getName(), bean.getVersion());
        return bean;
    }

    private ClusterBean getLatestCluster() {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(entityManager, ClusterQuery.GET_CLUSTER_LATEST);
        List resultList = query.getResultList();
        return resultList == null || resultList.isEmpty() ? null : (ClusterBean)resultList.get(0);
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
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }

    public ClusterBean getActiveCluster() throws BeaconStoreException {
        try {
            EntityManager entityManager = store.getEntityManager();
            Query query = getQuery(entityManager, ClusterQuery.GET_CLUSTER_ACTIVE);
            List resultList = query.getResultList();
            ClusterBean clusterBean = getSingleResult(resultList);
            updateClusterProp(clusterBean);
            updateClusterPair(clusterBean);
            entityManager.close();
            return clusterBean;
        } catch (Exception e) {
            LOG.error("Error while getting the active cluster: [{}] from store.", bean.getName());
            throw new BeaconStoreException(e.getMessage(), e);
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
            throw new BeaconStoreException("Cluster entity does not exists name: " + bean.getName());
        } else if (resultList.size() > 1) {
            LOG.error("Beacon data store is in inconsistent state. More than 1 result found. Cluster name: [{}]",
                    bean.getName());
            throw new BeaconStoreException("Beacon data store is in inconsistent state. More than 1 result found. "
                    + "Cluster name: " + bean.getName());
        } else {
            return (ClusterBean) resultList.get(0);
        }
    }

    public void retireCluster() {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(entityManager, ClusterQuery.RETIRE_CLUSTER);
        entityManager.getTransaction().begin();
        int executeUpdate = query.executeUpdate();
        entityManager.getTransaction().commit();
        LOG.info("Cluster name [{}] deleted, record updated [{}].", bean.getName(), executeUpdate);
        entityManager.close();
    }
}
