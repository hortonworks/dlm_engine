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

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.ClusterBean;
import com.hortonworks.beacon.store.bean.ClusterPairBean;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;
import com.hortonworks.beacon.util.StringFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
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

    private void execute() {
        EntityManager entityManager = getEntityManager();
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

    public ClusterBean submitCluster() {
        ClusterBean latestCluster = getLatestCluster();
        if (latestCluster == null) {
            bean.setVersion(1);
        } else if (latestCluster.getRetirementTime() != null) {
            bean.setVersion(latestCluster.getVersion() + 1);
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
        Query query = getEntityManager().createNamedQuery(namedQuery.name());
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
                throw new IllegalArgumentException(
                    StringFormat.format("Invalid named query parameter passed: {}", namedQuery.name()));
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
                StringFormat.format("Cluster entity does not exist with name: {}", bean.getName()));
        } else if (resultList.size() > 1) {
            LOG.error("Beacon data store is in inconsistent state. More than 1 result found.Cluster name: {}",
                bean.getName());
            throw new BeaconStoreException(
                "Beacon data store is in inconsistent state. More than 1 result found.Cluster name: {}",
                bean.getName());
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
            throw new NoSuchElementException("No local cluster found.");
        } else {
            ClusterBean clusterBean = getSingleResult(resultList);
            updateClusterProp(clusterBean);
            updateClusterPair(clusterBean);
            return clusterBean;
        }
    }
}
