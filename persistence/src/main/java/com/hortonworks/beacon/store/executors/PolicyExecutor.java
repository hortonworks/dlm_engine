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
import com.hortonworks.beacon.store.BeaconStore;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Date;
import java.util.List;

/**
 * Beacon store executor for policy.
 */
public class PolicyExecutor {

    /**
     * Enums for Policy named queries.
     */
    public enum PolicyQuery {
        GET_ACTIVE_POLICY,
        DELETE_POLICY,
        GET_POLICY,
        GET_SUBMITTED_POLICY,
        UPDATE_STATUS
    }

    private static final Logger LOG = LoggerFactory.getLogger(PolicyExecutor.class);

    private PolicyBean bean;

    public PolicyExecutor(PolicyBean bean) {
        this.bean = bean;
    }

    public PolicyExecutor(String name) {
        this(new PolicyBean(name));
    }

    public void execute(EntityManager entityManager) throws BeaconStoreException {
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

    public void execute() throws BeaconStoreException {
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        execute(entityManager);
    }

    public int executeUpdate(PolicyQuery namedQuery) {
        LOG.info("policyBean update is called for [{}]", namedQuery);
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query query = getQuery(namedQuery, entityManager);
        entityManager.getTransaction().begin();
        int update = query.executeUpdate();
        LOG.info("Records updated for PolicyBean table namedQuery [{}]", namedQuery);
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
        // TODO get the data center and server index and update it.
        bean.setId(BeaconIDGenerator.generatePolicyId(bean.getSourceCluster(), bean.getSourceCluster(),
                bean.getName(), 0));
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
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query query = getQuery(PolicyQuery.GET_POLICY, entityManager);
        List resultList = query.getResultList();
        return (resultList == null || resultList.isEmpty()) ? null : (PolicyBean) resultList.get(0);
    }

    public PolicyBean getSubmitted() throws BeaconStoreException {
        LOG.info("Get policy with submitted status name: [{}]", bean.getName());
        bean.setStatus(JobStatus.SUBMITTED.name());
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query query = getQuery(PolicyQuery.GET_SUBMITTED_POLICY, entityManager);
        List resultList = query.getResultList();
        if (resultList == null || resultList.isEmpty()) {
            throw new BeaconStoreException("Policy does not exists name: " + bean.getName()
                    + ", status: " + bean.getStatus());
        } else {
            if (resultList.size() > 1) {
                LOG.error("Beacon data store is in inconsistent state. More than 1 result found.");
                throw new BeaconStoreException("Beacon data store is in inconsistent state. More than 1 result found.");
            } else {
                return updatePolicyProp((PolicyBean) resultList.get(0));
            }
        }
    }

    public PolicyBean getActivePolicy() throws BeaconStoreException {
        LOG.info("Get active policy for name: [{}]", bean.getName());
        EntityManager entityManager = BeaconStore.getInstance().getEntityManager();
        Query query = getQuery(PolicyQuery.GET_ACTIVE_POLICY, entityManager);
        List resultList = query.getResultList();
        PolicyBean policyBean = getSingleResult(resultList);
        return updatePolicyProp(policyBean);
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
}
