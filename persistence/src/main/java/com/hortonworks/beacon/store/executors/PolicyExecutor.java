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

import com.hortonworks.beacon.BeaconIDGenerator;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import com.hortonworks.beacon.util.StringFormat;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Beacon store executor for policy.
 */
public class PolicyExecutor extends BaseExecutor {

    /**
     * Enums for Policy named queries.
     */
    public enum PolicyQuery {
        GET_ACTIVE_POLICY,
        DELETE_POLICY,
        GET_POLICY,
        GET_POLICIES_FOR_TYPE,
        GET_POLICY_BY_ID,
        GET_PAIRED_CLUSTER_POLICY,
        GET_CLUSTER_CLOUD_POLICY,
        GET_ARCHIVED_POLICY,
        UPDATE_STATUS,
        UPDATE_JOBS,
        UPDATE_POLICY_LAST_INS_STATUS,
        DELETE_RETIRED_POLICY,
        UPDATE_FINAL_STATUS,
        UPDATE_POLICY_RETIREMENT,
        GET_POLICY_RECOVERY
    }

    private static final Logger LOG = LoggerFactory.getLogger(PolicyExecutor.class);

    private PolicyBean bean;

    public PolicyExecutor(PolicyBean bean) {
        this.bean = bean;
    }

    public PolicyExecutor(String name) {
        this(new PolicyBean(name));
    }

    private void execute() throws BeaconStoreException {
        EntityManager entityManager = getEntityManager();
        entityManager.persist(bean);
        String policyId = bean.getId();
        Date createdTime = bean.getCreationTime();
        List<PolicyPropertiesBean> beanList = bean.getCustomProperties();
        for (PolicyPropertiesBean propertiesBean : beanList) {
            propertiesBean.setPolicyId(policyId);
            propertiesBean.setCreationTime(createdTime);
            entityManager.persist(propertiesBean);
        }
    }

    public int executeUpdate(PolicyQuery namedQuery) {
        Query query = getQuery(namedQuery);
        int update = query.executeUpdate();
        LOG.debug("Records updated for PolicyBean table namedQuery [{}], count [{}]", namedQuery, update);
        return update;
    }

    private Query getQuery(PolicyQuery namedQuery) {
        Query query = getEntityManager().createNamedQuery(namedQuery.name());
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
            case GET_CLUSTER_CLOUD_POLICY:
                query.setParameter("cloudCred", getCloudCredId());
                break;
            case GET_ARCHIVED_POLICY:
                query.setParameter("name", bean.getName());
                break;
            case UPDATE_FINAL_STATUS:
                query.setParameter("id", bean.getId());
                query.setParameter("status", bean.getStatus());
                query.setParameter("lastModifiedTime", bean.getLastModifiedTime());
                break;
            case UPDATE_POLICY_RETIREMENT:
                query.setParameter("id", bean.getId());
                query.setParameter("retirementTime", bean.getRetirementTime());
                break;
            case GET_POLICY_RECOVERY:
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Invalid named query parameter passed: {}", namedQuery.name()));
        }
        return query;
    }

    public PolicyBean submitPolicy() throws BeaconStoreException {
        PolicyBean policy = getLatestPolicy();
        if (policy == null) {
            bean.setVersion(1);
        } else if (policy.getRetirementTime() != null
                || JobStatus.getCompletionStatus().contains(policy.getStatus())) {
            bean.setVersion(policy.getVersion() + 1);
        } else {
            throw new BeaconStoreException("Policy already exists with name: {}", bean.getName());
        }
        // In case of HCFS, target cluster info will be null, use source cluster in that case.
        if (StringUtils.isBlank(bean.getId())) {
            bean.setId(BeaconIDGenerator.generatePolicyId(bean.getSourceCluster(),
                    StringUtils.isNotBlank(bean.getTargetCluster()) ? bean.getTargetCluster() : bean.getSourceCluster(),
                    bean.getName(), 0));
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
        Query query = getQuery(PolicyQuery.GET_POLICY);
        List resultList = query.getResultList();
        return (resultList == null || resultList.isEmpty()) ? null : (PolicyBean) resultList.get(0);
    }

    public PolicyBean getPolicy(PolicyQuery namedQuery) throws BeaconStoreException {
        Query query = getQuery(namedQuery);
        LOG.debug("Executing get policy for query: {}", query.toString());
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
            throw new NoSuchElementException(
                StringFormat.format("Policy [{}] does not exist.",
                        StringUtils.isNotBlank(bean.getName()) ? bean.getName() : bean.getId()));
        } else if (resultList.size() > 1) {
            LOG.error("Beacon data store is in inconsistent state. More than 1 result found.");
            throw new BeaconStoreException("Beacon data store is in inconsistent state. More than 1 result found.");
        } else {
            return (PolicyBean) resultList.get(0);
        }
    }

    public List<PolicyBean> getPolicies(PolicyQuery namedQuery) throws BeaconStoreException {
        Query query = getQuery(namedQuery);
        List resultList = query.getResultList();
        List<PolicyBean> policyBeanList = new ArrayList<>();
        for (Object result : resultList) {
            policyBeanList.add((PolicyBean) result);
            updatePolicyProp((PolicyBean) result);
        }
        return policyBeanList;
    }

    public boolean existsClustersPolicies(PolicyQuery namedQuery) {
        Query query = getQuery(namedQuery);
        long result = (long) query.getSingleResult();
        return result > 0;
    }

    private String getCloudCredId() {
        List<PolicyPropertiesBean> customProperties = bean.getCustomProperties();
        for (PolicyPropertiesBean propBean : customProperties) {
            if (propBean.getName().equals(ReplicationPolicy.ReplicationPolicyFields.CLOUDCRED.getName())) {
                return propBean.getValue();
            }
        }
        throw new IllegalArgumentException("Cloud cred id is missing.");
    }
}
