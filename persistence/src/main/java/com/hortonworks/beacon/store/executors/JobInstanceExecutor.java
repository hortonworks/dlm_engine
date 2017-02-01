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
import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.util.ReplicationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

public class JobInstanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JobInstanceExecutor.class);

    public enum JobInstanceQuery {
        UPDATE_JOB_INSTANCE,
        SELECT_JOB_INSTANCE,
        SET_DELETED;
    }

    private JobInstanceBean bean;

    public JobInstanceExecutor() {
    }

    public JobInstanceExecutor(JobInstanceBean bean) {
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
        ;
        execute(entityManager);
    }

    public void executeUpdate(JobInstanceQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        ;
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        query.executeUpdate();
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public Query getQuery(JobInstanceQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_JOB_INSTANCE:
                query.setParameter("jobExecutionType", bean.getJobExecutionType());
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("duration", bean.getDuration());
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("id", bean.getId());
                break;
            case SELECT_JOB_INSTANCE:
                query.setParameter("name", bean.getName());
                query.setParameter("type", bean.getType());
                query.setParameter("deleted", bean.getDeleted());
                break;
            case SET_DELETED:
                String newId = bean.getId() + "#" + System.currentTimeMillis();
                query.setParameter("id", bean.getId());
                query.setParameter("deleted", bean.getDeleted());
                query.setParameter("id_new", newId);
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }

    public List<JobInstanceBean> executeSelectQuery(JobInstanceQuery namedQuery) {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        ;
        Query selectQuery = getQuery(namedQuery, entityManager);
        List resultList = selectQuery.getResultList();
        List<JobInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((JobInstanceBean) result);
        }
        entityManager.close();
        return beanList;
    }

    public List<JobInstanceBean> getInstances(String name, String type) {
        LOG.info("Listing job instances for [name: {}, type: {}]", name, type);
        type = ReplicationHelper.getReplicationType(type).getName();
        JobInstanceBean bean = new JobInstanceBean();
        bean.setName(name);
        bean.setType(type);
        bean.setDeleted(0);
        JobInstanceExecutor executor = new JobInstanceExecutor(bean);
        List<JobInstanceBean> beanList = executor.executeSelectQuery(JobInstanceQuery.SELECT_JOB_INSTANCE);
        LOG.info("Listing job instances completed for [name: {}, type: {}, size: {}]", name, type, beanList.size());
        return beanList;
    }

    public void updatedDeletedInstances(String name, String type) {
        List<JobInstanceBean> beanList = getInstances(name, type);
        for (JobInstanceBean bean : beanList) {
            bean.setDeleted(1);
            JobInstanceExecutor executor = new JobInstanceExecutor(bean);
            executor.executeUpdate(JobInstanceQuery.SET_DELETED);
        }
    }
}
