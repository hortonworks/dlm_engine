/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.PolicyReport;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceListExecutor;
import com.hortonworks.beacon.store.executors.PolicyListExecutor;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Persistence helper for policy entities in Beacon.
 */
public final class PolicyDao {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyDao.class);

    public void persistPolicy(ReplicationPolicy policy) throws BeaconStoreException {
        PolicyBean bean = getPolicyBean(policy);
        bean.setEndTime(bean.getEndTime() == null
                ? DateUtil.createDate(BeaconConstants.MAX_YEAR, Calendar.DECEMBER, BeaconConstants.MAX_DAY)
                : bean.getEndTime());
        PolicyExecutor executor = new PolicyExecutor(bean);
        bean = executor.submitPolicy();
        policy.setPolicyId(bean.getId());
        policy.setEndTime(bean.getEndTime());
        policy.setStatus(bean.getStatus());
    }

    public void updatePolicyStatus(String name, String type, String status) {
        PolicyBean bean = new PolicyBean(name);
        bean.setType(type);
        bean.setStatus(status);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_STATUS);
    }

    public void updatePolicyJobs(String id, String name, String jobs) {
        PolicyBean bean = new PolicyBean(name);
        bean.setId(id);
        bean.setJobs(jobs);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_JOBS);
    }

    public String getPolicyStatus(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        PolicyBean bean = executor.getActivePolicy();
        return bean.getStatus();
    }

    public ReplicationPolicy getActivePolicy(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        PolicyBean bean = executor.getActivePolicy();
        return getReplicationPolicy(bean);
    }

    public List<PolicyInstanceBean> getPolicyInstance(String policyId) throws BeaconStoreException {
        LOG.debug("Listing job instances for policy id: [{}]", policyId);
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        instanceBean.setPolicyId(policyId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(
                PolicyInstanceExecutor.PolicyInstanceQuery.SELECT_POLICY_INSTANCE);
        LOG.debug("Listing job instances completed for policy id: [{}], size: [{}]", policyId, beanList.size());
        return beanList;
    }

    public void markPolicyInstanceDeleted(String policyId, Date retirementTime) throws BeaconStoreException {
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        instanceBean.setPolicyId(policyId);
        instanceBean.setRetirementTime(retirementTime);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        executor.executeUpdate(PolicyInstanceExecutor.PolicyInstanceQuery.DELETE_POLICY_INSTANCE);
    }

    public void markInstanceJobDeleted(List<PolicyInstanceBean> instances, Date retirementTime) {
        for (PolicyInstanceBean instanceBean : instances) {
            InstanceJobBean bean = new InstanceJobBean();
            bean.setInstanceId(instanceBean.getInstanceId());
            bean.setRetirementTime(retirementTime);
            InstanceJobExecutor executor = new InstanceJobExecutor(bean);
            executor.executeUpdate(InstanceJobExecutor.InstanceJobQuery.DELETE_INSTANCE_JOB);
        }
    }

    public int deletePolicy(String name, Date retirementTime) {
        PolicyBean bean = new PolicyBean(name);
        bean.setStatus(JobStatus.DELETED.name());
        bean.setRetirementTime(retirementTime);
        PolicyExecutor executor = new PolicyExecutor(bean);
        return executor.executeUpdate(PolicyExecutor.PolicyQuery.DELETE_POLICY);
    }

    public PolicyList getFilteredPolicy(String fieldStr, String filterBy, String orderBy,
                                        String sortOrder, Integer offset, Integer resultsPerPage, int instanceCount) {
        PolicyListExecutor executor = new PolicyListExecutor();
        long totalCount = executor.getFilteredPolicyCount(filterBy, orderBy, sortOrder, resultsPerPage);
        if (totalCount > 0) {
            List<PolicyBean> filteredPolicy = executor.getFilteredPolicy(filterBy, orderBy, sortOrder, offset,
                    resultsPerPage);
            HashSet<String> fields = new HashSet<>(Arrays.asList(fieldStr.toUpperCase().split(",")));
            PolicyList.PolicyElement[] policyElements = buildPolicyElements(fields, filteredPolicy, instanceCount);
            return new PolicyList(policyElements, totalCount);
        }
        return new PolicyList(new PolicyList.PolicyElement[]{}, totalCount);
    }

    private PolicyList.PolicyElement[] buildPolicyElements(HashSet<String> fields, List<PolicyBean> entities,
                                                                  int instanceCount) {
        PolicyList.PolicyElement[] elements = new PolicyList.PolicyElement[entities.size()];
        int elementIndex = 0;
        for (PolicyBean entity : entities) {
            elements[elementIndex++] = getPolicyElement(entity, fields, instanceCount);
        }
        return elements;
    }

    private PolicyList.PolicyElement getPolicyElement(PolicyBean bean, HashSet<String> fields,
                                                             int instanceCount) {
        PolicyList.PolicyElement elem = new PolicyList.PolicyElement();
        elem.policyId = bean.getId();
        elem.name = bean.getName();
        elem.type = bean.getType();
        elem.description = bean.getDescription();
        if (fields.contains(PolicyList.PolicyFieldList.STATUS.name())) {
            elem.status = bean.getStatus();
        }
        if (fields.contains(PolicyList.PolicyFieldList.FREQUENCY.name())) {
            elem.frequencyInSec = bean.getFrequencyInSec();
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
            elem.tags = tags;
        }
        if (fields.contains(PolicyList.PolicyFieldList.CLUSTERS.name())) {
            elem.sourceCluster = bean.getSourceCluster();
            elem.targetCluster = bean.getTargetCluster();
        }
        if (fields.contains(PolicyList.PolicyFieldList.DATASETS.name())) {
            elem.sourceDataset = bean.getSourceDataset();
            elem.targetDataset = bean.getTargetDataset();
        }

        if (fields.contains(PolicyList.PolicyFieldList.INSTANCES.name())) {
            PolicyInstanceBean instanceBean = new PolicyInstanceBean();
            instanceBean.setPolicyId(bean.getId());
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
            List<PolicyInstanceBean> instances = executor.getInstanceRecent(
                    PolicyInstanceExecutor.PolicyInstanceQuery.GET_INSTANCE_RECENT, instanceCount);
            elem.instances = new PolicyInstanceList.InstanceElement[instances.size()];
            for (int i = 0; i < instances.size(); i++) {
                elem.instances[i] = createInstanceElement(bean.getName(), bean.getType(),
                        bean.getExecutionType(), bean.getUser(), instances.get(i));
            }
        }

        if (fields.contains(PolicyList.PolicyFieldList.EXECUTIONTYPE.name())) {
            elem.executionType = bean.getExecutionType();
        }

        if (fields.contains(PolicyList.PolicyFieldList.CUSTOMPROPERTIES.name())) {
            elem.customProperties = getPolicyCustomProperties(bean);
        }

        if (fields.contains(PolicyList.PolicyFieldList.REPORT.name())) {
            PolicyReport report = new PolicyReport();
            PolicyInstanceBean instanceBean = new PolicyInstanceBean();
            String policyId = bean.getId();
            instanceBean.setPolicyId(policyId);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
            List<PolicyInstanceBean> instanceReport = executor.getInstanceReport(
                    PolicyInstanceExecutor.PolicyInstanceQuery.GET_INSTANCE_REPORT, 10);
            Map<String, PolicyInstanceList.InstanceElement> elementMap = new HashMap<>();
            for (PolicyInstanceBean policyInstanceBean : instanceReport) {
                PolicyInstanceList.InstanceElement instanceElement = new PolicyInstanceList.InstanceElement();
                instanceElement.status = policyInstanceBean.getStatus();
                instanceElement.endTime = DateUtil.formatDate(new Date(policyInstanceBean.getEndTime().getTime()));
                elementMap.put(instanceElement.status.toUpperCase(), instanceElement);
            }
            report.setLastSucceededInstance(elementMap.get(JobStatus.SUCCESS.name()));

            PolicyInstanceList.InstanceElement lastFailedInstance = elementMap.get(JobStatus.FAILED.name());
            PolicyInstanceList.InstanceElement lastKilledInstance = elementMap.get(JobStatus.KILLED.name());
            if (lastKilledInstance != null && lastFailedInstance != null) {
                Date failedEndTime = DateUtil.parseDate(lastFailedInstance.endTime);
                Date killedEndTime = DateUtil.parseDate(lastKilledInstance.endTime);
                if (failedEndTime.before(killedEndTime)) {
                    lastFailedInstance = lastKilledInstance;
                }
            } else if (lastKilledInstance != null) {
                lastFailedInstance = lastKilledInstance;
            }

            report.setLastFailedInstance(lastFailedInstance);
            elem.policyReport = report;
        }
        return elem;
    }

    private Properties getPolicyCustomProperties(PolicyBean bean) {
        List<PolicyPropertiesBean> customProp = bean.getCustomProperties();
        Properties prop = new Properties();
        for (PolicyPropertiesBean propertiesBean : customProp) {
            prop.setProperty(propertiesBean.getName(), propertiesBean.getValue());
        }
        return prop;
    }

    public PolicyList getPolicyDefinitions(String name, boolean isArchived) throws BeaconStoreException {
        List<PolicyList.PolicyElement> policyElements = new ArrayList<>();
        if (isArchived) {
            List<PolicyBean> archivedPolicies = getArchivedPolicies(name);
            for (PolicyBean bean : archivedPolicies) {
                policyElements.add(getPolicyElement(bean));
            }
        } else {
            ReplicationPolicy policy = getActivePolicy(name);
            PolicyList.PolicyElement policyElement = getPolicyElement(getPolicyBean(policy));
            policyElements.add(policyElement);
        }
        return new PolicyList(policyElements.toArray(new PolicyList.PolicyElement[policyElements.size()]),
                policyElements.size());
    }

    private List<PolicyBean> getArchivedPolicies(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        return executor.getPolicies(PolicyExecutor.PolicyQuery.GET_ARCHIVED_POLICY);
    }

    private PolicyList.PolicyElement getPolicyElement(PolicyBean bean) {
        PolicyList.PolicyElement element = new PolicyList.PolicyElement();
        element.policyId = bean.getId();
        element.name = bean.getName();
        element.type = bean.getType();
        element.description = bean.getDescription();
        element.status = bean.getStatus();
        element.executionType = bean.getExecutionType();
        element.sourceCluster = bean.getSourceCluster();
        element.targetCluster = bean.getTargetCluster();
        element.sourceDataset = bean.getSourceDataset();
        element.targetDataset = bean.getTargetDataset();
        element.startTime = DateUtil.formatDate(bean.getStartTime());
        element.endTime = DateUtil.formatDate(bean.getEndTime());
        element.retirementTime = DateUtil.formatDate(bean.getRetirementTime());
        element.frequencyInSec = bean.getFrequencyInSec();
        element.user = bean.getUser();
        element.retryAttempts = bean.getRetryCount();
        element.retryDelay = bean.getRetryDelay();
        element.notificationType = bean.getNotificationType();
        element.notificationTo = bean.getNotificationTo();

        Properties prop = getPolicyCustomProperties(bean);
        element.customProperties = prop;
        element.tags = StringUtils.isNotBlank(bean.getTags())
                ? Arrays.asList(bean.getTags().split(BeaconConstants.COMMA_SEPARATOR))
                : null;
        return element;
    }

    public PolicyBean getPolicyBean(ReplicationPolicy policy) {
        PolicyBean bean = new PolicyBean();
        bean.setId(policy.getPolicyId());
        bean.setName(policy.getName());
        bean.setType(policy.getType());
        bean.setDescription(policy.getDescription());
        bean.setUser(policy.getUser());
        bean.setExecutionType(policy.getExecutionType());
        bean.setStatus(policy.getStatus());
        bean.setLastInstanceStatus(policy.getLastInstanceStatus());
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

    private ReplicationPolicy getReplicationPolicy(PolicyBean bean) {
        ReplicationPolicy policy = new ReplicationPolicy();
        policy.setPolicyId(bean.getId());
        policy.setName(bean.getName());
        policy.setDescription(bean.getDescription());
        policy.setType(bean.getType());
        policy.setUser(bean.getUser());
        policy.setExecutionType(bean.getExecutionType());
        policy.setStatus(bean.getStatus());
        policy.setLastInstanceStatus(bean.getLastInstanceStatus());
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
        Properties prop = getPolicyCustomProperties(bean);
        policy.setCustomProperties(prop);
        return policy;
    }

    public boolean activePairedClusterPolicies(String localClusterName, String remoteClusterName) {
        PolicyBean bean = new PolicyBean();
        bean.setSourceCluster(localClusterName);
        bean.setTargetCluster(remoteClusterName);
        PolicyExecutor executor = new PolicyExecutor(bean);
        return executor.existsClustersPolicies();
    }

    public PolicyInstanceList getFilteredJobInstance(String filters, String orderBy, String sortBy,
                                                            Integer offset, Integer resultsPerPage,
                                                            boolean isArchived) throws Exception {
        PolicyInstanceListExecutor executor = new PolicyInstanceListExecutor();
        long totalCount = executor.getFilteredPolicyInstanceCount(filters, orderBy, sortBy,
                resultsPerPage, isArchived);
        List<PolicyInstanceList.InstanceElement> elements = new ArrayList<>();
        if (totalCount > 0) {
            List<Object[]> resultList = executor.getFilteredJobInstance(filters, orderBy, sortBy, offset,
                    resultsPerPage, isArchived);
            for (Object[] objects : resultList) {
                String name = (String) objects[0];
                String type = (String) objects[1];
                String executionType = (String) objects[2];
                String user = (String) objects[3];
                PolicyInstanceBean bean = (PolicyInstanceBean) objects[4];
                PolicyInstanceList.InstanceElement element = createInstanceElement(name, type, executionType, user,
                        bean);
                elements.add(element);
            }
        }
        return new PolicyInstanceList(elements, totalCount);
    }

    private PolicyInstanceList.InstanceElement createInstanceElement(String name, String type,
                                                                            String executionType, String user,
                                                                            PolicyInstanceBean bean) {
        PolicyInstanceList.InstanceElement element = new PolicyInstanceList.InstanceElement();
        element.id = bean.getInstanceId();
        element.policyId = bean.getPolicyId();
        element.name = name;
        element.type = type;
        element.executionType = executionType;
        element.user = user;
        element.status = bean.getStatus();
        element.trackingInfo = bean.getTrackingInfo();
        element.startTime = DateUtil.formatDate(new Date(bean.getStartTime().getTime()));
        element.endTime = DateUtil.formatDate(bean.getEndTime() != null ? new Date(bean.getEndTime().getTime()) : null);
        element.retryAttempted = String.valueOf(bean.getRunCount());
        element.message = bean.getMessage();
        return element;
    }

    public void updateInstanceStatus(String policyId) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(policyId);
        bean.setStatus(JobStatus.DELETED.name());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceExecutor.PolicyInstanceQuery.UPDATE_INSTANCE_STATUS);
    }

    public PolicyInstanceBean getInstanceForRerun(String policyId) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(policyId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        return executor.getInstanceForRun(PolicyInstanceExecutor.PolicyInstanceQuery.GET_INSTANCE_FOR_RERUN);
    }

    public void updateInstanceRerun(String instanceId) {
        PolicyInstanceBean bean = new PolicyInstanceBean(instanceId);
        bean.setStatus(JobStatus.RUNNING.name());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceExecutor.PolicyInstanceQuery.UPDATE_INSTANCE_RERUN);
    }

    public void updateCompletionStatus(String policyId, String status) {
        PolicyBean bean = new PolicyBean();
        bean.setId(policyId);
        bean.setStatus(status);
        Date currentTime = new Date();
        bean.setRetirementTime(currentTime);
        bean.setLastModifiedTime(currentTime);
        PolicyExecutor executor  = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_FINAL_STATUS);
    }

    public void retireCompletedPolicy(String name) throws BeaconStoreException {
        try {
            ReplicationPolicy activePolicy = getActivePolicy(name);
            // Policy with name exists, check it is completed.
            List<String> completionStatus = JobStatus.getCompletionStatus();
            if (completionStatus.contains(activePolicy.getStatus().toUpperCase())) {
                PolicyBean bean = new PolicyBean();
                bean.setId(activePolicy.getPolicyId());
                Date retirementTime = new Date();
                bean.setRetirementTime(retirementTime);
                PolicyExecutor executor = new PolicyExecutor(bean);
                List<PolicyInstanceBean> instances = getPolicyInstance(activePolicy.getPolicyId());
                executor.executeUpdate(PolicyExecutor.PolicyQuery.UPDATE_POLICY_RETIREMENT);
                markPolicyInstanceDeleted(activePolicy.getPolicyId(), retirementTime);
                markInstanceJobDeleted(instances, retirementTime);
            }
        } catch (NoSuchElementException e) {
            // No policy with same exists. Proceed to submit the policy.
        }
    }
}
