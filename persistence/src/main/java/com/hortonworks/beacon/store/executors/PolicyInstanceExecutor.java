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
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Beacon store executor for policy instances.
 */
public class PolicyInstanceExecutor extends BaseExecutor {

    private static final BeaconLog LOG = BeaconLog.getLog(PolicyInstanceExecutor.class);

    /**
     * Enums for PolicyInstanceBean.
     */
    public enum PolicyInstanceQuery {
        UPDATE_INSTANCE_COMPLETE,
        UPDATE_CURRENT_OFFSET,
        SELECT_POLICY_INSTANCE,
        DELETE_POLICY_INSTANCE,
        DELETE_RETIRED_INSTANCE,
        GET_INSTANCE_TRACKING_INFO,
        UPDATE_INSTANCE_TRACKING_INFO,
        SELECT_INSTANCE_RUNNING,
        GET_INSTANCE_FAILED,
        GET_INSTANCE_RECENT,
        GET_INSTANCE_BY_ID,
        UPDATE_INSTANCE_RETRY_COUNT,
        UPDATE_INSTANCE_STATUS,
        UPDATE_INSTANCE_RERUN
    }

    private PolicyInstanceBean bean;

    public PolicyInstanceExecutor(PolicyInstanceBean bean) {
        this.bean = bean;
    }

    private void execute(EntityManager entityManager) {
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

    public void executeUpdate(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(namedQuery, entityManager);
            entityManager.getTransaction().begin();
            int update = query.executeUpdate();
            LOG.debug("Records updated for PolicyInstanceBean table namedQuery [{0}], count [{1}]", namedQuery, update);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    private Query getQuery(PolicyInstanceQuery namedQuery, EntityManager entityManager) {
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
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case DELETE_RETIRED_INSTANCE:
                query.setParameter("retirementTime", new Timestamp(bean.getRetirementTime().getTime()));
                break;
            case GET_INSTANCE_TRACKING_INFO:
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case UPDATE_INSTANCE_TRACKING_INFO:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("trackingInfo", bean.getTrackingInfo());
                break;
            case SELECT_INSTANCE_RUNNING:
                query.setParameter("status", bean.getStatus());
                break;
            case GET_INSTANCE_FAILED:
                query.setParameter("policyId", bean.getPolicyId());
                query.setParameter("status", bean.getStatus());
                break;
            case GET_INSTANCE_RECENT:
                query.setParameter("policyId", bean.getPolicyId());
                break;
            case GET_INSTANCE_BY_ID:
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case UPDATE_INSTANCE_RETRY_COUNT:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("runCount", bean.getRunCount());
                break;
            case UPDATE_INSTANCE_STATUS:
                query.setParameter("policyId", bean.getPolicyId());
                query.setParameter("status", bean.getStatus());
                break;
            case UPDATE_INSTANCE_RERUN:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("status", bean.getStatus());
                query.setParameter("startTime", bean.getStartTime());
                query.setParameter("message", bean.getMessage());
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return query;
    }

    public List<PolicyInstanceBean> executeSelectQuery(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query selectQuery = getQuery(namedQuery, entityManager);
            List resultList = selectQuery.getResultList();
            List<PolicyInstanceBean> beanList = new ArrayList<>();
            for (Object result : resultList) {
                beanList.add((PolicyInstanceBean) result);
            }
            return beanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public List<PolicyInstanceBean> getInstanceRecent(PolicyInstanceQuery namedQuery, int results) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(namedQuery, entityManager);
            query.setMaxResults(results);
            List resultList = query.getResultList();
            List<PolicyInstanceBean> beanList = new ArrayList<>();
            for (Object result : resultList) {
                beanList.add((PolicyInstanceBean) result);
            }
            entityManager.close();
            return beanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
