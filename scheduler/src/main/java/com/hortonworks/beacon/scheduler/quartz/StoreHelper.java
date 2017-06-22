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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.events.BeaconEvents;
import com.hortonworks.beacon.events.EventEntityType;
import com.hortonworks.beacon.events.Events;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.SchedulerException;

import java.util.Date;
import java.util.List;

/**
 * Helper class for all the DB interaction from scheduler.
 */
final class StoreHelper {

    private static final BeaconLog LOG = BeaconLog.getLog(StoreHelper.class);

    private StoreHelper() {
    }

    static String insertPolicyInstance(String policyId, int count, String status) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        String instanceId = policyId + "@" + count;
        bean.setInstanceId(instanceId);
        bean.setPolicyId(policyId);
        bean.setStartTime(new Date());
        bean.setRunCount(0);
        bean.setStatus(status);
        bean.setCurrentOffset(0);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.execute();
        return instanceId;
    }

    static void updatePolicyInstanceCompleted(JobContext jobContext, String status, String message) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setStatus(status);
        bean.setMessage(truncateMessage(message));
        bean.setPolicyId(jobContext.getJobInstanceId().split("@")[0]);
        bean.setEndTime(new Date());
        bean.setInstanceId(jobContext.getJobInstanceId());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceExecutor.PolicyInstanceQuery.UPDATE_INSTANCE_COMPLETE);
        generateInstanceEvents(status, bean);
    }

    static void updatePolicyLastInstanceStatus(String policyId, String instanceStatus) {
        PolicyBean bean = new PolicyBean();
        bean.setId(policyId);
        bean.setLastInstanceStatus(instanceStatus);
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyQuery.UPDATE_POLICY_LAST_INS_STATUS);
    }

    static String getLastInstanceStatus(String profileId) throws BeaconStoreException {
        PolicyBean bean = new PolicyBean();
        bean.setId(profileId);
        PolicyExecutor executor = new PolicyExecutor(bean);
        PolicyBean policyBean = executor.getPolicy(PolicyQuery.GET_POLICY_BY_ID);
        return policyBean.getLastInstanceStatus();
    }

    static void updateInstanceJobCompleted(JobContext jobContext, String status, String message) {
        InstanceJobBean bean = new InstanceJobBean(jobContext.getJobInstanceId(), jobContext.getOffset());
        bean.setStatus(status);
        bean.setMessage(truncateMessage(message));
        bean.setEndTime(new Date());
        bean.setContextData(jobContext.toString());
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobExecutor.InstanceJobQuery.UPDATE_JOB_COMPLETE);
    }

    static void updateInstanceCurrentOffset(JobContext jobContext) {
        PolicyInstanceBean bean = new PolicyInstanceBean(jobContext.getJobInstanceId());
        bean.setCurrentOffset(jobContext.getOffset());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceExecutor.PolicyInstanceQuery.UPDATE_CURRENT_OFFSET);
    }

    static void updateInstanceJobStatusStartTime(JobContext jobContext, JobStatus status) {
        String instanceId = jobContext.getJobInstanceId();
        int offset = jobContext.getOffset();
        InstanceJobBean bean = new InstanceJobBean(instanceId, offset);
        bean.setStatus(status.name());
        bean.setStartTime(new Date());
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobExecutor.InstanceJobQuery.UPDATE_STATUS_START);
    }

    static JobContext transferJobContext(JobExecutionContext qContext) throws SchedulerException {
        JobKey jobKey = qContext.getJobDetail().getKey();
        String currentOffset = jobKey.getGroup();
        Integer prevOffset = Integer.parseInt(currentOffset) - 1;
        String instanceId = getInstanceId(qContext.getJobDetail());

        InstanceJobBean bean = new InstanceJobBean(instanceId, prevOffset);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        InstanceJobBean instanceJob = executor.getInstanceJob(InstanceJobExecutor.InstanceJobQuery.GET_INSTANCE_JOB);

        String contextData = instanceJob.getContextData();
        JobContext jobContext = JobContext.parseJobContext(contextData);
        // Update the offset to current for job.
        jobContext.setOffset(Integer.parseInt(currentOffset));
        return jobContext;
    }

    private static String getInstanceId(JobDetail jobDetail) throws SchedulerException {
        JobKey jobKey = jobDetail.getKey();
        String policyId = jobKey.getName();
        int counter = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.COUNTER.getValue());
        return policyId + "@" + counter;
    }

    static void updateRemainingInstanceJobs(JobContext jobContext, String status)
            throws SchedulerException {
        InstanceJobBean bean = new InstanceJobBean();
        bean.setInstanceId(jobContext.getJobInstanceId());
        bean.setStatus(status);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobExecutor.InstanceJobQuery.INSTANCE_JOB_UPDATE_STATUS);
    }

    /**
     * When jobs are not found into the cached map, needs to be retrieved from store.
     *
     * @param instanceId instance id
     * @param offset     offset
     * @param policyId   policy id
     * @return key for the next job
     */
    static JobKey getNextJobFromStore(String instanceId, int offset, String policyId) {
        InstanceJobBean bean = new InstanceJobBean(instanceId, offset + 1);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        InstanceJobBean instanceJob = executor.getInstanceJob(InstanceJobExecutor.InstanceJobQuery.GET_INSTANCE_JOB);
        return instanceJob != null
                ? new JobKey(policyId, String.valueOf(instanceJob.getOffset()))
                : null;
    }

    static void insertJobInstance(String instanceId, int jobCount) {
        int offsetCounter = 0;
        while (offsetCounter < jobCount) {
            InstanceJobBean bean = new InstanceJobBean(instanceId, offsetCounter);
            bean.setStatus(JobStatus.SUBMITTED.name());
            bean.setRunCount(0);
            offsetCounter++;
            InstanceJobExecutor executor = new InstanceJobExecutor(bean);
            executor.execute();
        }
    }

    private static String truncateMessage(String message) {
        return (message.length() > 4000)
                ? message.substring(0, 3899) + " ..."
                : message;
    }

    private static void generateInstanceEvents(String status, PolicyInstanceBean bean) {
        JobStatus jobStatus;

        try {
            jobStatus = JobStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            LOG.error("JobStatus: [{}] is not supported. Message: {}", status, e.getMessage());
            return;
        }

        switch (jobStatus) {
            case SUCCESS:
                BeaconEvents.createEvents(Events.SUCCEEDED, EventEntityType.POLICYINSTANCE, bean);
                break;
            case FAILED:
                BeaconEvents.createEvents(Events.FAILED, EventEntityType.POLICYINSTANCE, bean);
                break;
            case IGNORED:
                BeaconEvents.createEvents(Events.IGNORED, EventEntityType.POLICYINSTANCE, bean);
                break;
            case DELETED:
                BeaconEvents.createEvents(Events.DELETED, EventEntityType.POLICYINSTANCE, bean);
                break;
            case KILLED:
                BeaconEvents.createEvents(Events.KILLED, EventEntityType.POLICYINSTANCE, bean);
                break;
            default:
                LOG.error("Job status: [{}] is not supported.", jobStatus.name());
        }
    }

    static int getJobOffset(String policyId, String lastInstanceStatus) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(policyId);
        bean.setStatus(lastInstanceStatus);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        List<PolicyInstanceBean> instanceBeans = executor.executeSelectQuery(PolicyInstanceQuery.GET_INSTANCE_FAILED);
        if (instanceBeans == null || instanceBeans.isEmpty()) {
            return -1;
        } else {
            PolicyInstanceBean instanceBean = instanceBeans.get(0);
            LOG.info("last instance: {} offset: {} status: {} for policy: {}", instanceBean.getInstanceId(),
                    instanceBean.getCurrentOffset(), lastInstanceStatus, policyId);
            return instanceBean.getCurrentOffset();
        }
    }

    static int getJobRunCount(JobContext jobContext) {
        InstanceJobBean bean = new InstanceJobBean(jobContext.getJobInstanceId(), jobContext.getOffset());
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        InstanceJobBean instanceJob = executor.getInstanceJob(InstanceJobQuery.GET_INSTANCE_JOB);
        return instanceJob.getRunCount();
    }

    static void updateJobRunCount(JobContext jobContext, int runCount) {
        InstanceJobBean bean = new InstanceJobBean(jobContext.getJobInstanceId(), jobContext.getOffset());
        bean.setRunCount(runCount);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobQuery.UPDATE_JOB_RETRY_COUNT);
    }

    static int getInstanceRunCount(JobContext jobContext) {
        PolicyInstanceBean bean = new PolicyInstanceBean(jobContext.getJobInstanceId());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        List<PolicyInstanceBean> instances = executor.executeSelectQuery(PolicyInstanceQuery.GET_INSTANCE_BY_ID);
        return instances.get(0).getRunCount();
    }

    static void updateInstanceRunCount(JobContext jobContext, int runCount) {
        PolicyInstanceBean bean = new PolicyInstanceBean(jobContext.getJobInstanceId());
        bean.setRunCount(runCount);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_RETRY_COUNT);
    }
}
