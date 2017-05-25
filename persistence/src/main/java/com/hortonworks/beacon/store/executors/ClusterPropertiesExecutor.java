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
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.ClusterPropertiesBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

/**
 * Beacon store executor for ClusterProperties.
 */
public class ClusterPropertiesExecutor {

    private BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);

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
        EntityManager entityManager = store.getEntityManager();
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
    }

}
