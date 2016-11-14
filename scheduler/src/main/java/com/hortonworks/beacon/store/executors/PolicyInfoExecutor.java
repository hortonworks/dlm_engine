/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.store.BeaconStore;
import com.hortonworks.beacon.store.bean.PolicyInfoBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;

public class PolicyInfoExecutor {

    public enum PolicyInfoQuery {
        UPDATE_STATUS,
        SELECT_POLICY_INFO
    }

    private final PolicyInfoBean bean;

    public PolicyInfoExecutor(PolicyInfoBean bean) {
        this.bean = bean;
    }

    public void execute(EntityManager entityManager) {
        entityManager.getTransaction().begin();
        entityManager.persist(bean);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public void execute() {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();;
        execute(entityManager);
    }

    public void executeUpdate(PolicyInfoQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();;
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        query.executeUpdate();
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public PolicyInfoBean executeSingleSelectQuery(PolicyInfoQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();;
        Query selectQuery = getQuery(namedQuery, entityManager);
        PolicyInfoBean bean = (PolicyInfoBean) selectQuery.getSingleResult();
        entityManager.close();
        return bean;
    }

    private Query getQuery(PolicyInfoQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_STATUS:
                query.setParameter("status", bean.getStatus());
                query.setParameter("lastModified", bean.getLastModified());
                query.setParameter("name", bean.getName());
                break;
            case SELECT_POLICY_INFO:
                query.setParameter("name", bean.getName());
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }
}
