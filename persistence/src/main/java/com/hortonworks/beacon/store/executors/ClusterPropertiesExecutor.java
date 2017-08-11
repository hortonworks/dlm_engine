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

import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

/**
 * Beacon store executor for ClusterProperties.
 */
public class ClusterPropertiesExecutor extends BaseExecutor {

    /**
     * Enum of ClusterProperties named queries.
     */
    enum ClusterPropertiesQuery {
        GET_CLUSTER_PROP
    }

    private ClusterPropertiesBean bean;

    public ClusterPropertiesExecutor(ClusterPropertiesBean bean) {
        this.bean = bean;
    }

    List<ClusterPropertiesBean> getClusterProperties() {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = entityManager.createNamedQuery(ClusterPropertiesQuery.GET_CLUSTER_PROP.name());
            query.setParameter("clusterName", bean.getClusterName());
            query.setParameter("clusterVersion", bean.getClusterVersion());
            List resultList = query.getResultList();
            List<ClusterPropertiesBean> beans = new ArrayList<>();
            if (resultList != null && !resultList.isEmpty()) {
                for (Object result : resultList) {
                    beans.add((ClusterPropertiesBean) result);
                }
            }
            return beans;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

}
