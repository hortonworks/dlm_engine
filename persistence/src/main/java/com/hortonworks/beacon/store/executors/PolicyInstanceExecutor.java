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

import com.hortonworks.beacon.store.BeaconStore;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.ReplicationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy instances.
 */
public class PolicyInstanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyInstanceExecutor.class);

    /**
     * Enums for PolicyInstanceBean.
     */
    public enum PolicyInstanceQuery {
        UPDATE_POLICY_INSTANCE,
        SELECT_POLICY_INSTANCE,
        DELETE_POLICY_INSTANCE
    }

    private PolicyInstanceBean bean;

    public PolicyInstanceExecutor() {
    }

    public PolicyInstanceExecutor(PolicyInstanceBean bean) {
        this.bean = bean;
    }

    public void execute(EntityManager entityManager) {
        entityManager.getTransaction().begin();
        entityManager.persist(bean);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public void execute() {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        execute(entityManager);
    }

    public void executeUpdate(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        query.executeUpdate();
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public Query getQuery(PolicyInstanceQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_POLICY_INSTANCE:
                query.setParameter("jobExecutionType", bean.getJobExecutionType());
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("duration", bean.getDuration());
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("id", bean.getId());
                break;
            case SELECT_POLICY_INSTANCE:
                query.setParameter("name", bean.getName());
                query.setParameter("policyType", bean.getType());
                break;
            case DELETE_POLICY_INSTANCE:
                String newId = bean.getId() + "#" + bean.getDeletionTime().getTime();
                query.setParameter("id", bean.getId());
                query.setParameter("deletionTime", bean.getDeletionTime());
                query.setParameter("id_new", newId);
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }

    public List<PolicyInstanceBean> executeSelectQuery(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query selectQuery = getQuery(namedQuery, entityManager);
        List resultList = selectQuery.getResultList();
        List<PolicyInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((PolicyInstanceBean) result);
        }
        entityManager.close();
        return beanList;
    }

    public List<PolicyInstanceBean> getInstances(String name, String type) {
        LOG.info("Listing job instances for [name: {}, type: {}]", name, type);
        type = ReplicationHelper.getReplicationType(type).getName();
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        instanceBean.setName(name);
        instanceBean.setType(type);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(PolicyInstanceQuery.SELECT_POLICY_INSTANCE);
        LOG.info("Listing job instances completed for [name: {}, type: {}, size: {}]", name, type, beanList.size());
        return beanList;
    }

    public void updatedDeletedInstances(String name, String type) {
        List<PolicyInstanceBean> beanList = getInstances(name, type);
        Date deletionTime = new Date();
        for (PolicyInstanceBean instanceBean : beanList) {
            instanceBean.setDeletionTime(deletionTime);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
            executor.executeUpdate(PolicyInstanceQuery.DELETE_POLICY_INSTANCE);
        }
    }
}
