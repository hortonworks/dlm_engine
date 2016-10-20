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
import com.hortonworks.beacon.store.bean.ChainedJobsBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;

public class ChainedJobsExecutor {

    public enum ChainedJobQuery {
        GET_SECOND_JOB;
    }

    private final ChainedJobsBean bean;

    public ChainedJobsExecutor(ChainedJobsBean bean) {
        this.bean = bean;
    }

    public void execute(EntityManager entityManager) {
        entityManager.getTransaction().begin();
        entityManager.persist(bean);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public void execute() {
        EntityManager entityManager = BeaconStore.getEntityManager();
        execute(entityManager);
    }

    public ChainedJobsBean executeSelectQuery(ChainedJobQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getEntityManager();
        Query selectQuery = getQuery(namedQuery, entityManager);
        Object result = selectQuery.getSingleResult();
        entityManager.close();
        return (ChainedJobsBean) result;
    }

    private Query getQuery(ChainedJobQuery namedQuery, EntityManager em) {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_SECOND_JOB:
                query.setParameter("firstJobName", bean.getFirstJobName());
                query.setParameter("firstJobGroup", bean.getFirstJobGroup());
                break;
        }
        return query;
    }
}
