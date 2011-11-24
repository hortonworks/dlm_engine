/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.StringFormat;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy instances.
 */
public class PolicyInstanceExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyInstanceExecutor.class);

    /**
     * Enums for PolicyInstanceBean.
     */
    public enum PolicyInstanceQuery {
        UPDATE_INSTANCE_COMPLETE,
        UPDATE_INSTANCE_FAIL_RETIRE,
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
        UPDATE_INSTANCE_RERUN,
        GET_INSTANCE_STATUS_RECENT,
        UPDATE_INSTANCE_STATUS_RETIRE,
        GET_INSTANCE_REPORT
    }

    private PolicyInstanceBean bean;

    public PolicyInstanceExecutor(PolicyInstanceBean bean) {
        this.bean = bean;
    }

    public void execute() {
        getEntityManager().persist(bean);
    }

    public void executeUpdate(PolicyInstanceQuery namedQuery) {
        Query query = getQuery(namedQuery);
        int update = query.executeUpdate();
        LOG.debug("Records updated for PolicyInstanceBean table namedQuery [{}], count [{}]", namedQuery, update);
    }

    private Query getQuery(PolicyInstanceQuery namedQuery) {
        Query query = getEntityManager().createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_INSTANCE_COMPLETE:
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("instanceId", bean.getInstanceId());
                break;
            case UPDATE_INSTANCE_FAIL_RETIRE:
                query.setParameter("endTime", bean.getEndTime());
                query.setParameter("status", bean.getStatus());
                query.setParameter("message", bean.getMessage());
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case UPDATE_CURRENT_OFFSET:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("currentOffset", bean.getCurrentOffset());
                break;
            case SELECT_POLICY_INSTANCE:
                query.setParameter("policyId", bean.getPolicyId());
                break;
            case DELETE_POLICY_INSTANCE:
                query.setParameter("policyId", bean.getPolicyId());
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
            case GET_INSTANCE_STATUS_RECENT:
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
            case UPDATE_INSTANCE_STATUS_RETIRE:
                query.setParameter("instanceId", bean.getInstanceId());
                query.setParameter("status", bean.getStatus());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case GET_INSTANCE_REPORT:
                query.setParameter("policyId", bean.getPolicyId());
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Policy does not exist with name: {}", namedQuery.name()));
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
                throw new IllegalArgumentException(
                    StringFormat.format("Policy does not exist with name: {}", namedQuery.name()));
        }
        return instanceBean;
    }

    public List<PolicyInstanceBean> executeSelectQuery(PolicyInstanceQuery namedQuery) {
        Query selectQuery = getQuery(namedQuery);
        List resultList = selectQuery.getResultList();
        List<PolicyInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((PolicyInstanceBean) result);
        }
        return beanList;
    }

    public List<PolicyInstanceBean> getInstanceRecent(PolicyInstanceQuery namedQuery, int results) {
        Query query = getQuery(namedQuery);
        query.setMaxResults(results);
        List resultList = query.getResultList();
        List<PolicyInstanceBean> beanList = new ArrayList<>();
        for (Object result : resultList) {
            beanList.add((PolicyInstanceBean) result);
        }
        return beanList;
    }

    public PolicyInstanceBean getInstanceForRun(PolicyInstanceQuery namedQuery) {
        Query query = getQuery(namedQuery);
        query.setMaxResults(1);
        List<Object[]> resultList = query.getResultList();
        Object[] objects = resultList != null && !resultList.isEmpty() ? resultList.get(0) : null;
        return constructBean(namedQuery, objects);
    }

    public List<String> getInstanceStatusRecent(PolicyInstanceQuery namedQuery, int count) {
        Query query = getQuery(namedQuery);
        query.setMaxResults(count);
        List<Object[]> resultList = query.getResultList();
        List<String> statusList = new ArrayList<>();
        for (Object []objects : resultList) {
            statusList.add((String) objects[0]);
        }
        return statusList;
    }

    public List<PolicyInstanceBean> getInstanceReport(PolicyInstanceQuery namedQuery, int count) {
        Query query = getQuery(namedQuery);
        query.setMaxResults(count);
        List<Object[]> resultList = query.getResultList();
        List<PolicyInstanceBean> beans = new ArrayList<>();
        for (Object[] objects : resultList) {
            PolicyInstanceBean policyInstanceBean = new PolicyInstanceBean();
            policyInstanceBean.setStatus((String) objects[0]);
            policyInstanceBean.setEndTime((Date) objects[1]);
            beans.add(policyInstanceBean);
        }
        return beans;
    }
}
