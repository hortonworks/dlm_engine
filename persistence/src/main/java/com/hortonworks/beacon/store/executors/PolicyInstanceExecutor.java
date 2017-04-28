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
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Beacon store executor for policy instances.
 */
public class PolicyInstanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyInstanceExecutor.class);
    private BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);

    /**
     * Enums for PolicyInstanceBean.
     */
    public enum PolicyInstanceQuery {
        UPDATE_INSTANCE_COMPLETE,
        UPDATE_CURRENT_OFFSET,
        SELECT_POLICY_INSTANCE,
        DELETE_POLICY_INSTANCE,
        DELETE_RETIRED_INSTANCE,
        UPDATE_INSTANCE_TRACKING_INFO
    }

    private PolicyInstanceBean bean;

    public PolicyInstanceExecutor(PolicyInstanceBean bean) {
        this.bean = bean;
    }

    private void execute(EntityManager entityManager) {
        entityManager.getTransaction().begin();
        entityManager.persist(bean);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public void execute() {
        EntityManager entityManager = store.getEntityManager();
        execute(entityManager);
    }

    public void executeUpdate(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager =  store.getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        int update = query.executeUpdate();
        LOG.debug("Records updated for PolicyInstanceBean table namedQuery [{}], count [{}]", namedQuery, update);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public Query getQuery(PolicyInstanceQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_INSTANCE_COMPLETE:
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case UPDATE_CURRENT_OFFSET:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("currentOffset", bean.getCurrentOffset());
                break;
            case SELECT_POLICY_INSTANCE:
                query.setParameter("policyId", bean.getPolicyId());
                break;
            case DELETE_POLICY_INSTANCE:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("status", bean.getStatus());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case DELETE_RETIRED_INSTANCE:
                query.setParameter("retirementTime", new Timestamp(bean.getRetirementTime().getTime()));
                break;
            case UPDATE_INSTANCE_TRACKING_INFO:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("trackingInfo", bean.getTrackingInfo());
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }

    public List<PolicyInstanceBean> executeSelectQuery(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = store.getEntityManager();
        Query selectQuery = getQuery(namedQuery, entityManager);
        List resultList = selectQuery.getResultList();
        List<PolicyInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((PolicyInstanceBean) result);
        }
        entityManager.close();
        return beanList;
    }
}
