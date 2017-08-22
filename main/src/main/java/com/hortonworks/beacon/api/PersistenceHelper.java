/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyInstanceList.InstanceElement;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.PolicyList.PolicyElement;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.BeaconStoreService;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.bean.PolicyPropertiesBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceListExecutor;
import com.hortonworks.beacon.store.executors.PolicyListExecutor;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
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
    }

    static ReplicationPolicy getPolicyForSchedule(String policyName) throws BeaconStoreException {
        try {
            PolicyExecutor executor = new PolicyExecutor(policyName);
            PolicyBean bean = executor.getSubmitted();
            return getReplicationPolicy(bean);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        }
    }

    static void updatePolicyStatus(String name, String type, String status) {
        PolicyBean bean = new PolicyBean(name);
        bean.setType(type);
        bean.setStatus(status);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyQuery.UPDATE_STATUS);
    }

    static void updatePolicyJobs(String id, String name, String jobs) {
        PolicyBean bean = new PolicyBean(name);
        bean.setId(id);
        bean.setJobs(jobs);
        bean.setLastModifiedTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyQuery.UPDATE_JOBS);
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
        LOG.info(MessageCode.MAIN_000073.name(), policyId);
        PolicyInstanceBean instanceBean = new PolicyInstanceBean();
        instanceBean.setPolicyId(policyId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(PolicyInstanceQuery.SELECT_POLICY_INSTANCE);
        LOG.info(MessageCode.MAIN_000074.name(), policyId, beanList.size());
        return beanList;
    }

    static void markPolicyInstanceDeleted(List<PolicyInstanceBean> instances, Date retirementTime)
            throws BeaconStoreException {
        for (PolicyInstanceBean instanceBean : instances) {
            instanceBean.setRetirementTime(retirementTime);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
            executor.executeUpdate(PolicyInstanceQuery.DELETE_POLICY_INSTANCE);
        }
    }

    static void markInstanceJobDeleted(List<PolicyInstanceBean> instances, Date retirementTime) {
        for (PolicyInstanceBean instanceBean : instances) {
            InstanceJobBean bean = new InstanceJobBean();
            bean.setInstanceId(instanceBean.getInstanceId());
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
        return executor.executeUpdate(PolicyQuery.DELETE_POLICY);
    }

    static PolicyList getFilteredPolicy(String fieldStr, String filterBy, String orderBy,
                                        String sortOrder, Integer offset, Integer resultsPerPage, int instanceCount) {
        PolicyListExecutor executor = new PolicyListExecutor();
        long totalCount = executor.getFilteredPolicyCount(filterBy, orderBy, sortOrder, resultsPerPage);
        if (totalCount > 0) {
            List<PolicyBean> filteredPolicy = executor.getFilteredPolicy(filterBy, orderBy, sortOrder, offset,
                    resultsPerPage);
            HashSet<String> fields = new HashSet<>(Arrays.asList(fieldStr.toUpperCase().split(",")));
            PolicyElement[] policyElements = buildPolicyElements(fields, filteredPolicy, instanceCount);
            return new PolicyList(policyElements, totalCount);
        }
        return new PolicyList(new PolicyElement[]{}, totalCount);
    }

    private static PolicyList.PolicyElement[] buildPolicyElements(HashSet<String> fields, List<PolicyBean> entities,
                                                                  int instanceCount) {
        PolicyElement[] elements = new PolicyElement[entities.size()];
        int elementIndex = 0;
        for (PolicyBean entity : entities) {
            elements[elementIndex++] = getPolicyElement(entity, fields, instanceCount);
        }
        return elements;
    }

    private static PolicyElement getPolicyElement(PolicyBean bean, HashSet<String> fields, int instanceCount) {
        PolicyElement elem = new PolicyElement();
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
            List<PolicyInstanceBean> instances = executor.getInstanceRecent(PolicyInstanceQuery.GET_INSTANCE_RECENT,
                    instanceCount);
            elem.instances = new InstanceElement[instances.size()];
            for (int i = 0; i < instances.size(); i++) {
                elem.instances[i] = createInstanceElement(bean.getName(), bean.getType(),
                        bean.getExecutionType(), bean.getUser(), instances.get(i));
            }
        }
        return elem;
    }

    static PolicyList getPolicyDefinitions(String name, boolean isArchived) throws BeaconStoreException {
        List<PolicyElement> policyElements = new ArrayList<>();
        if (isArchived) {
            List<PolicyBean> archivedPolicies = getArchivedPolicies(name);
            for (PolicyBean bean : archivedPolicies) {
                policyElements.add(getPolicyElement(bean));
            }
        } else {
            ReplicationPolicy policy = getActivePolicy(name);
            PolicyElement policyElement = getPolicyElement(getPolicyBean(policy));
            policyElements.add(policyElement);
        }
        return new PolicyList(policyElements.toArray(new PolicyElement[policyElements.size()]), policyElements.size());
    }

    private static List<PolicyBean> getArchivedPolicies(String name) throws BeaconStoreException {
        PolicyExecutor executor = new PolicyExecutor(name);
        return executor.getPolicies(PolicyQuery.GET_ARCHIVED_POLICY);
    }

    private static PolicyElement getPolicyElement(PolicyBean bean) {
        PolicyElement element = new PolicyElement();
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
        element.frequencyInSec = bean.getFrequencyInSec();
        element.user = bean.getUser();
        element.retryAttempts = bean.getRetryCount();
        element.retryDelay = bean.getRetryDelay();
        element.notificationType = bean.getNotificationType();
        element.notificationTo = bean.getNotificationTo();

        List<PolicyPropertiesBean> customProp = bean.getCustomProperties();
        Properties prop = new Properties();
        for (PolicyPropertiesBean propertiesBean : customProp) {
            prop.setProperty(propertiesBean.getName(), propertiesBean.getValue());
        }
        element.customProperties = prop;
        element.tags = StringUtils.isNotBlank(bean.getTags())
                ? Arrays.asList(bean.getTags().split(BeaconConstants.COMMA_SEPARATOR))
                : null;
        return element;
    }

    static PolicyBean getPolicyBean(ReplicationPolicy policy) {
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

    private static ReplicationPolicy getReplicationPolicy(PolicyBean bean) {
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

    static PolicyInstanceList getFilteredJobInstance(String filters, String orderBy, String sortBy, Integer offset,
                                                     Integer resultsPerPage, boolean isArchived) throws Exception {
        PolicyInstanceListExecutor executor = new PolicyInstanceListExecutor();
        try {
            executor.initializeEntityManager();
            long totalCount = executor.getFilteredPolicyInstanceCount(filters, orderBy, sortBy,
                    resultsPerPage, isArchived);
            List<InstanceElement> elements = new ArrayList<>();
            if (totalCount > 0) {
                List<Object[]> resultList = executor.getFilteredJobInstance(filters, orderBy, sortBy, offset,
                        resultsPerPage, isArchived);
                for (Object[] objects : resultList) {
                    String name = (String) objects[0];
                    String type = (String) objects[1];
                    String executionType = (String) objects[2];
                    String user = (String) objects[3];
                    PolicyInstanceBean bean = (PolicyInstanceBean) objects[4];
                    InstanceElement element = createInstanceElement(name, type, executionType, user,
                            bean);
                    elements.add(element);
                }
            }
            return new PolicyInstanceList(elements, totalCount);
        } catch (Exception e) {
            throw e;
        } finally {
            executor.closeEntityManager();
        }
    }

    private static InstanceElement createInstanceElement(String name, String type, String executionType, String user,
                                                        PolicyInstanceBean bean) {
        InstanceElement element = new InstanceElement();
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

    static String getServerVersion() {
        BeaconStoreService service = Services.get().getService(BeaconStoreService.SERVICE_NAME);
        EntityManager entityManager = null;
        try {
            String versionQuery = "select data from beacon_sys where name = 'beacon_version'";
            entityManager = service.getEntityManager();
            Query query = entityManager.createNativeQuery(versionQuery);
            return (String) query.getSingleResult();
        } catch (Exception e) {
            throw e;
        } finally {
            service.closeEntityManager(entityManager);
        }
    }

    static void updateInstanceStatus(String policyId) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(policyId);
        bean.setStatus(JobStatus.DELETED.name());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_STATUS);
    }

    static PolicyInstanceBean getInstanceForRerun(String policyId) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(policyId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        return executor.getInstanceForRun(PolicyInstanceQuery.GET_INSTANCE_FOR_RERUN);
    }

    static void updateInstanceRerun(String instanceId) {
        PolicyInstanceBean bean = new PolicyInstanceBean(instanceId);
        bean.setStatus(JobStatus.RUNNING.name());
        bean.setStartTime(new Date());
        bean.setMessage("");
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_RERUN);
    }
}
