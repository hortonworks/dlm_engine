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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.store.bean.InstanceJobBean;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;
import java.sql.Timestamp;

/**
 * Beacon store executor for instance jobs.
 */
public class InstanceJobExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(InstanceJobExecutor.class);
    private InstanceJobBean bean;

    /**
     * Enums for InstanceJob named queries.
     */
    public enum InstanceJobQuery {
        GET_INSTANCE_JOB,
        UPDATE_STATUS_START,
        INSTANCE_JOB_UPDATE_STATUS,
        UPDATE_JOB_COMPLETE,
        UPDATE_JOB_RETRY_COUNT,
        DELETE_INSTANCE_JOB,
        DELETE_RETIRED_JOBS
    }

    public InstanceJobExecutor(InstanceJobBean bean) {
        this.bean = bean;
    }

    public void execute(EntityManager entityManager) {
        entityManager.getTransaction().begin();
        entityManager.persist(bean);
        entityManager.getTransaction().commit();
    }

    public void execute() {
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

    public void executeUpdate(InstanceJobQuery namedQuery) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(namedQuery, entityManager);
            entityManager.getTransaction().begin();
            int update = query.executeUpdate();
            LOG.debug("Records updated for InstanceJobBean table namedQuery [{}], count [{}]", namedQuery, update);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private Query getQuery(InstanceJobQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_STATUS_START:
                query.setParameter("status", bean.getStatus());
                query.setParameter("startTime", bean.getStartTime());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case INSTANCE_JOB_UPDATE_STATUS:
                query.setParameter("status", bean.getStatus());
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case UPDATE_JOB_COMPLETE:
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("contextData", bean.getContextData());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case UPDATE_JOB_RETRY_COUNT:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                query.setParameter("runCount", bean.getRunCount());
                break;
            case GET_INSTANCE_JOB:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("offset", bean.getOffset());
                break;
            case DELETE_INSTANCE_JOB:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case DELETE_RETIRED_JOBS:
                query.setParameter("retirementTime", new Timestamp(bean.getRetirementTime().getTime()));
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return query;
    }

    public InstanceJobBean getInstanceJob(InstanceJobQuery namedQuery) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(namedQuery, entityManager);
            return (InstanceJobBean) query.getSingleResult();
        } catch (NoResultException e) {
            LOG.warn(MessageCode.PERS_000027.name(), bean.getInstanceId(), bean.getOffset());
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
