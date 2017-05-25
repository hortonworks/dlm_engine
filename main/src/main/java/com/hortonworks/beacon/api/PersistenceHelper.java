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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.PolicyList.PolicyElement;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import com.hortonworks.beacon.store.executors.PolicyListExecutor;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * Persistence helper for Beacon.
 */
public final class PersistenceHelper {

    private static final BeaconLog LOG = BeaconLog.getLog(PersistenceHelper.class);

    private PersistenceHelper() {
    }

    static void persistPolicy(ReplicationPolicy policy) throws BeaconStoreException {
        PolicyBean bean = getPolicyBean(policy);
        bean.setEndTime(bean.getEndTime() == null
                ? DateUtil.createDate(BeaconConstants.MAX_YEAR, Calendar.DECEMBER, BeaconConstants.MAX_DAY)
                : bean.getEndTime());
        PolicyExecutor executor = new PolicyExecutor(bean);
        bean = executor.submitPolicy();
        policy.setPolicyId(bean.getId());
        policy.setEndTime(bean.getEndTime());
        policy.setStatus(bean.getStatus());
        BeaconEvents.createEvents(Events.SUBMITTED, EventEntityType.POLICY, bean);
    }

    static ReplicationPolicy getPolicyForSchedule(String policyName) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(policyName);
        PolicyBean bean = executor.getSubmitted();
        BeaconEvents.createEvents(Events.SCHEDULED, EventEntityType.POLICY, bean);
        return getReplicationPolicy(bean);
    }

    static void updatePolicyStatus(String name, String type, String status) {
        PolicyBean bean = new PolicyBean(name);
        bean.setType(type);
        bean.setStatus(status);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_STATUS);
    }

    static void updatePolicyJobs(String id, String name, String jobs) {
        PolicyBean bean = new PolicyBean(name);
        bean.setId(id);
        bean.setJobs(jobs);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_JOBS);
    }

    static String getPolicyStatus(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        PolicyBean bean = executor.getActivePolicy();
        return bean.getStatus();
    }

    static ReplicationPolicy getActivePolicy(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        PolicyBean bean = executor.getActivePolicy();
        return getReplicationPolicy(bean);
    }

    static List<PolicyInstanceBean> getPolicyInstance(String policyId) throws BeaconStoreException {
        LOG.info("Listing job instances for policy id: [{}]", policyId);
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        instanceBean.setPolicyId(policyId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(PolicyInstanceQuery.SELECT_POLICY_INSTANCE);
        LOG.info("Listing job instances completed for policy id: [{}], size: [{}]", policyId, beanList.size());
        return beanList;
    }

    static void markPolicyInstanceDeleted(List<PolicyInstanceBean> instances, Date retirementTime)
            throws BeaconStoreException {
        for (PolicyInstanceBean instanceBean : instances) {
            instanceBean.setStatus(JobStatus.DELETED.name());
            instanceBean.setRetirementTime(retirementTime);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
            executor.executeUpdate(PolicyInstanceQuery.DELETE_POLICY_INSTANCE);
        }
    }

    static void markInstanceJobDeleted(List<PolicyInstanceBean> instances, Date retirementTime) {
        for (PolicyInstanceBean instanceBean : instances) {
            InstanceJobBean bean = new InstanceJobBean();
            bean.setInstanceId(instanceBean.getInstanceId());
            bean.setStatus(JobStatus.DELETED.name());
            bean.setRetirementTime(retirementTime);
            InstanceJobExecutor executor = new InstanceJobExecutor(bean);
            executor.executeUpdate(InstanceJobQuery.DELETE_INSTANCE_JOB);
        }
    }

    static int deletePolicy(String name, Date retirementTime) {
        PolicyBean bean = new PolicyBean(name);
        bean.setStatus(JobStatus.DELETED.name());
        bean.setRetirementTime(retirementTime);
        PolicyExecutor executor = new PolicyExecutor(bean);
        return executor.executeUpdate(PolicyExecutor.PolicyQuery.DELETE_POLICY);
    }

    static PolicyList getFilteredPolicy(String fieldStr, String filterBy, String orderBy,
                                        String sortOrder, Integer offset, Integer resultsPerPage) {
        PolicyListExecutor executor = new PolicyListExecutor();
        long totalCount = executor.getFilteredPolicyCount(filterBy, orderBy, sortOrder, resultsPerPage);
        if (totalCount > 0) {
            List<PolicyBean> filteredPolicy = executor.getFilteredPolicy(filterBy, orderBy, sortOrder, offset,
                    resultsPerPage);
            HashSet<String> fields = new HashSet<>(Arrays.asList(fieldStr.toUpperCase().split(",")));
            PolicyElement[] policyElements = buildPolicyElements(fields, filteredPolicy);
            return new PolicyList(policyElements, totalCount);
        }
        return new PolicyList(new PolicyElement[]{}, totalCount);
    }

    private static PolicyList.PolicyElement[] buildPolicyElements(HashSet<String> fields, List<PolicyBean> entities) {
        PolicyElement[] elements = new PolicyElement[entities.size()];
        int elementIndex = 0;
        for (PolicyBean entity : entities) {
            elements[elementIndex++] = getPolicyElement(entity, fields);
        }
        return elements;
    }

    private static PolicyElement getPolicyElement(PolicyBean bean, HashSet<String> fields) {
        PolicyElement elem = new PolicyElement();
        elem.name = bean.getName();
        elem.type = bean.getType();
        if (fields.contains(PolicyList.PolicyFieldList.STATUS.name())) {
            elem.status = bean.getStatus();
        }
        if (fields.contains(PolicyList.PolicyFieldList.FREQUENCY.name())) {
            elem.frequency = bean.getFrequencyInSec();
        }
        if (fields.contains(PolicyList.PolicyFieldList.STARTTIME.name())) {
            elem.startTime = DateUtil.formatDate(bean.getStartTime());
        }
        if (fields.contains(PolicyList.PolicyFieldList.ENDTIME.name())) {
            elem.endTime = DateUtil.formatDate(bean.getEndTime());
        }
        if (fields.contains(PolicyList.PolicyFieldList.TAGS.name())) {
            String rawTags = bean.getTags();
            List<String> tags = new ArrayList<>();
            if (!StringUtils.isBlank(rawTags)) {
                for (String tag : rawTags.split(",")) {
                    tags.add(tag.trim());
                }
            }
            elem.tag = tags;
        }
        if (fields.contains(PolicyList.PolicyFieldList.CLUSTERS.name())) {
            elem.sourceCluster = bean.getSourceCluster();
            elem.targetCluster = bean.getTargetCluster();
        }
        if (fields.contains(PolicyList.PolicyFieldList.DATASETS.name())) {
            elem.sourceDataset = bean.getSourceDataset();
            elem.targetDataset = bean.getTargetDataset();
        }
        return elem;
    }

    private static PolicyBean getPolicyBean(ReplicationPolicy policy) {
        PolicyBean bean = new PolicyBean();
        bean.setId(policy.getPolicyId());
        bean.setName(policy.getName());
        bean.setType(policy.getType());
        bean.setUser(policy.getUser());
        bean.setExecutionType(policy.getExecutionType());
        bean.setStatus(policy.getStatus());
        bean.setSourceCluster(policy.getSourceCluster());
        bean.setTargetCluster(policy.getTargetCluster());
        bean.setSourceDataset(policy.getSourceDataset());
        bean.setTargetDataset(policy.getTargetDataset());
        bean.setStartTime(policy.getStartTime());
        bean.setEndTime(policy.getEndTime());
        bean.setFrequencyInSec(policy.getFrequencyInSec());
        bean.setNotificationType(policy.getNotification().getType());
        bean.setNotificationTo(policy.getNotification().getTo());
        bean.setRetryCount(policy.getRetry().getAttempts());
        bean.setRetryDelay(policy.getRetry().getDelay());
        List<PolicyPropertiesBean> propertiesBeans = new ArrayList<>();
        Properties customProp = policy.getCustomProperties();
        for (String key : customProp.stringPropertyNames()) {
            PolicyPropertiesBean propertiesBean = new PolicyPropertiesBean();
            propertiesBean.setName(key);
            propertiesBean.setValue(customProp.getProperty(key));
            propertiesBeans.add(propertiesBean);
        }
        bean.setCustomProperties(propertiesBeans);
        bean.setTags(policy.getTags());
        return bean;
    }

    private static ReplicationPolicy getReplicationPolicy(PolicyBean bean) {
        ReplicationPolicy policy = new ReplicationPolicy();
        policy.setPolicyId(bean.getId());
        policy.setName(bean.getName());
        policy.setType(bean.getType());
        policy.setUser(bean.getUser());
        policy.setExecutionType(bean.getExecutionType());
        policy.setStatus(bean.getStatus());
        policy.setSourceCluster(bean.getSourceCluster());
        policy.setTargetCluster(bean.getTargetCluster());
        policy.setSourceDataset(bean.getSourceDataset());
        policy.setTargetDataset(bean.getTargetDataset());
        policy.setStartTime(bean.getStartTime());
        policy.setEndTime(bean.getEndTime());
        policy.setFrequencyInSec(bean.getFrequencyInSec());
        policy.setNotification(new Notification(bean.getNotificationType(), bean.getNotificationTo()));
        policy.setRetry(new Retry(bean.getRetryCount(), bean.getRetryDelay()));
        policy.setTags(bean.getTags());
        List<PolicyPropertiesBean> customProp = bean.getCustomProperties();
        Properties prop = new Properties();
        for (PolicyPropertiesBean propertiesBean : customProp) {
            prop.setProperty(propertiesBean.getName(), propertiesBean.getValue());
        }
        policy.setCustomProperties(prop);
        return policy;
    }

    static boolean activePairedClusterPolicies(String localClusterName, String remoteClusterName) {
        PolicyBean bean = new PolicyBean();
        bean.setSourceCluster(localClusterName);
        bean.setTargetCluster(remoteClusterName);
        PolicyExecutor executor = new PolicyExecutor(bean);
        return executor.existsClustersPolicies();
    }
}
