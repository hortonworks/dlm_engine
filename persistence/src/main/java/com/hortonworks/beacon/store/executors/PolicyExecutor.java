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

import com.hortonworks.beacon.BeaconIDGenerator;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy.
 */
public class PolicyExecutor {
    private BeaconStoreService store = Services.get().getService(BeaconStoreService.SERVICE_NAME);

    /**
     * Enums for Policy named queries.
     */
    public enum PolicyQuery {
        GET_ACTIVE_POLICY,
        DELETE_POLICY,
        GET_POLICY,
        GET_POLICIES_FOR_TYPE,
        GET_SUBMITTED_POLICY,
        GET_POLICY_BY_ID,
        GET_PAIRED_CLUSTER_POLICY,
        GET_ARCHIVED_POLICY,
        UPDATE_STATUS,
        UPDATE_JOBS,
        UPDATE_POLICY_LAST_INS_STATUS,
        DELETE_RETIRED_POLICY
    }

    private static final BeaconLog LOG = BeaconLog.getLog(PolicyExecutor.class);

    private PolicyBean bean;

    public PolicyExecutor(PolicyBean bean) {
        this.bean = bean;
    }

    public PolicyExecutor(String name) {
        this(new PolicyBean(name));
    }

    private void execute(EntityManager entityManager) throws BeaconStoreException {
        try {
            entityManager.getTransaction().begin();
            entityManager.persist(bean);
            String policyId = bean.getId();
            Date createdTime = bean.getCreationTime();
            List<PolicyPropertiesBean> beanList = bean.getCustomProperties();
            for (PolicyPropertiesBean propertiesBean : beanList) {
                propertiesBean.setPolicyId(policyId);
                propertiesBean.setCreationTime(createdTime);
                entityManager.persist(propertiesBean);
            }
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            LOG.error("Error message: {}", e.getMessage(), e);
            throw new BeaconStoreException(e.getMessage(), e);
        }
        entityManager.close();
    }

    private void execute() throws BeaconStoreException {
        EntityManager entityManager = store.getEntityManager();
        execute(entityManager);
    }

    public int executeUpdate(PolicyQuery namedQuery) {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        int update = query.executeUpdate();
        LOG.debug("Records updated for PolicyBean table namedQuery [{}], count [{}]", namedQuery, update);
        entityManager.getTransaction().commit();
        entityManager.close();
        return update;
    }

    private Query getQuery(PolicyQuery namedQuery, EntityManager entityManager) {
        Query query = entityManager.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_ACTIVE_POLICY:
                query.setParameter("name", bean.getName());
                break;
            case DELETE_POLICY:
                query.setParameter("name", bean.getName());
                query.setParameter("status", bean.getStatus());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case GET_POLICY:
                query.setParameter("name", bean.getName());
                break;
            case GET_POLICY_BY_ID:
                query.setParameter("id", bean.getId());
                break;
            case GET_SUBMITTED_POLICY:
                query.setParameter("name", bean.getName());
                query.setParameter("status", bean.getStatus());
                break;
            case UPDATE_STATUS:
                query.setParameter("name", bean.getName());
                query.setParameter("status", bean.getStatus());
                query.setParameter("policyType", bean.getType());
                query.setParameter("lastModifiedTime", bean.getLastModifiedTime());
                break;
            case UPDATE_JOBS:
                query.setParameter("id", bean.getId());
                query.setParameter("jobs", bean.getJobs());
                query.setParameter("lastModifiedTime", bean.getLastModifiedTime());
                break;
            case UPDATE_POLICY_LAST_INS_STATUS:
                query.setParameter("lastInstanceStatus", bean.getLastInstanceStatus());
                query.setParameter("id", bean.getId());
                break;
            case DELETE_RETIRED_POLICY:
                query.setParameter("retirementTime", new Timestamp(bean.getRetirementTime().getTime()));
                break;
            case GET_POLICIES_FOR_TYPE:
                query.setParameter("policyType", bean.getType());
                break;
            case GET_PAIRED_CLUSTER_POLICY:
                query.setParameter("sourceCluster", bean.getSourceCluster());
                query.setParameter("targetCluster", bean.getTargetCluster());
                break;
            case GET_ARCHIVED_POLICY:
                query.setParameter("name", bean.getName());
                break;
            default:
                throw new IllegalArgumentException("Invalid named query parameter passed: " + namedQuery.name());
        }
        return query;
    }

    public PolicyBean submitPolicy() throws BeaconStoreException {
        PolicyBean policy = getLatestPolicy();
        if (policy == null) {
            bean.setVersion(1);
        } else if (policy.getRetirementTime() != null) {
            bean.setVersion(policy.getVersion() + 1);
        } else {
            throw new BeaconStoreException("Policy already exists with name: " + bean.getName());
        }
        if (StringUtils.isBlank(bean.getId())) {
            bean.setId(BeaconIDGenerator.generatePolicyId(bean.getSourceCluster(), bean.getName(), 0));
        }
        Date time = new Date();
        bean.setCreationTime(time);
        bean.setLastModifiedTime(time);
        bean.setChangeId(1);
        bean.setRetirementTime(null);
        bean.setStatus(JobStatus.SUBMITTED.name());
        execute();
        LOG.info("PolicyBean for name: [{}], type: [{}] stored.", bean.getName(), bean.getType());
        return bean;
    }

    private PolicyBean getLatestPolicy() {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(PolicyQuery.GET_POLICY, entityManager);
        List resultList = query.getResultList();
        return (resultList == null || resultList.isEmpty()) ? null : (PolicyBean) resultList.get(0);
    }

    public PolicyBean getSubmitted() throws BeaconStoreException {
        bean.setStatus(JobStatus.SUBMITTED.name());
        return getPolicy(PolicyQuery.GET_SUBMITTED_POLICY);
    }

    public PolicyBean getPolicy(PolicyQuery namedQuery) throws BeaconStoreException {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        LOG.info("Executing get policy for query: {}", query.toString());
        List resultList = query.getResultList();
        PolicyBean policyBean = getSingleResult(resultList);
        return updatePolicyProp(policyBean);
    }

    public PolicyBean getActivePolicy() throws BeaconStoreException {
        return getPolicy(PolicyQuery.GET_ACTIVE_POLICY);
    }

    private PolicyBean updatePolicyProp(PolicyBean policyBean) throws BeaconStoreException {
        PolicyPropertiesExecutor executor = new PolicyPropertiesExecutor(policyBean.getId());
        List<PolicyPropertiesBean> policyProperties = executor.getPolicyProperties();
        policyBean.setCustomProperties(policyProperties);
        return policyBean;
    }

    private PolicyBean getSingleResult(List resultList) throws BeaconStoreException {
        if (resultList == null || resultList.isEmpty()) {
            throw new BeaconStoreException("Policy does not exists name: " + bean.getName());
        } else if (resultList.size() > 1) {
            LOG.error("Beacon data store is in inconsistent state. More than 1 result found.");
            throw new BeaconStoreException("Beacon data store is in inconsistent state. More than 1 result found.");
        } else {
            return (PolicyBean) resultList.get(0);
        }
    }

    public List<PolicyBean> getPolicies(PolicyQuery namedQuery) throws BeaconStoreException {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        List resultList = query.getResultList();
        List<PolicyBean> policyBeanList = new ArrayList<>();
        for (Object result : resultList) {
            policyBeanList.add((PolicyBean) result);
            updatePolicyProp((PolicyBean) result);
        }
        return policyBeanList;
    }

    public boolean existsClustersPolicies() {
        EntityManager entityManager = store.getEntityManager();
        Query query = getQuery(PolicyQuery.GET_PAIRED_CLUSTER_POLICY, entityManager);
        long result = (long) query.getSingleResult();
        return result > 0;
    }
}
