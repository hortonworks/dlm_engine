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
        GET_INSTANCE_FOR_RERUN,
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

    public void executeUpdate(PolicyInstanceQuery namedQuery, EntityManager entityManager) {
        Query query = getQuery(namedQuery, entityManager);
        int update = query.executeUpdate();
        LOG.debug("Records updated for PolicyInstanceBean table namedQuery [{0}], count [{1}]", namedQuery, update);
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
            case GET_INSTANCE_FOR_RERUN:
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
                query.setParameter("message", null);
                query.setParameter("endTime", null);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return query;
    }

    private PolicyInstanceBean constructBean(PolicyInstanceQuery namedQuery, Object [] objects) {
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        if (objects == null) {
            return instanceBean;
        }
        switch (namedQuery) {
            case GET_INSTANCE_FOR_RERUN:
                instanceBean.setInstanceId((String) objects[0]);
                instanceBean.setCurrentOffset((Integer) objects[1]);
                instanceBean.setStatus((String) objects[2]);
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.PERS_000002.name(), namedQuery.name()));
        }
        return instanceBean;
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
            return beanList;
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }

    public PolicyInstanceBean getInstanceForRun(PolicyInstanceQuery namedQuery) {
        EntityManager entityManager = null;
        try {
            entityManager = STORE.getEntityManager();
            Query query = getQuery(namedQuery, entityManager);
            query.setMaxResults(1);
            List<Object[]> resultList = query.getResultList();
            Object[] objects = resultList != null && !resultList.isEmpty() ? resultList.get(0) : null;
            return constructBean(namedQuery, objects);
        } catch (Exception e) {
            throw e;
        } finally {
            STORE.closeEntityManager(entityManager);
        }
    }
}
