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

import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.InstanceExecutionDetails;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor.InstanceJobQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.listeners.JobListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Beacon extended implementation for JobListenerSupport.
 */
public class QuartzJobListener extends JobListenerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobListener.class);
    private static final String START_NODE_GROUP = "0";
    private String name;
    private Map<JobKey, JobKey> chainLinks;


    public QuartzJobListener(String name) {
        this.name = name;
        chainLinks = new Hashtable<>();
    }

    public String getName() {
        return name;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        LOG.info("Job [key: {}] to be executed.", context.getJobDetail().getKey());
        checkParallelExecution();
        String instanceId = handleStartNode(context);
        JobContext jobContext;
        if (instanceId != null) {
            jobContext = initializeJobContext(context, instanceId);
        } else {
            // context for non-start nodes gets loaded from DB.
            jobContext = transferJobContext(context);
        }
        updateInstanceJobStatusStartTime(jobContext, JobStatus.RUNNING);
        updateInstanceCurrentOffset(jobContext);
    }

    private void updateInstanceCurrentOffset(JobContext jobContext) {
        PolicyInstanceBean bean = new PolicyInstanceBean(jobContext.getJobInstanceId());
        bean.setCurrentOffset(jobContext.getOffset());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_CURRENT_OFFSET);
    }

    private void updateInstanceJobStatusStartTime(JobContext jobContext, JobStatus status) {
        String instanceId = jobContext.getJobInstanceId();
        int offset = jobContext.getOffset();
        InstanceJobBean bean = new InstanceJobBean(instanceId, offset);
        bean.setStatus(status.name());
        bean.setStartTime(new Date());
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobQuery.UPDATE_STATUS_START);
    }

    private void checkParallelExecution() {
        // TODO check and prevent parallel execution execution of the job instance.
        // there is two cases:
        // 1. previous instance is still running and next instance triggered.
        // 2. After restart, previous instance is still in running state (store) but no actual jobs are running.
    }

    private String handleStartNode(JobExecutionContext context) {
        JobDetail jobDetail = context.getJobDetail();
        JobKey jobKey = jobDetail.getKey();
        if (jobKey.getGroup().equals(START_NODE_GROUP)) {
            String policyId = jobKey.getName();
            String instanceId = insertPolicyInstance(policyId, getAndUpdateCounter(jobDetail));
            int jobCount = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.NO_OF_JOBS.getValue());
            insertJobInstance(instanceId, jobCount);
            return instanceId;
        }
        return null;
    }

    // This is beacon managed job context which is used across all the jobs of a instance.
    private JobContext initializeJobContext(JobExecutionContext quartzContext, String instanceId) {
        JobContext context = new JobContext();
        context.setOffset(0);
        context.setJobInstanceId(instanceId);
        context.setShouldInterrupt(new AtomicBoolean(false));
        context.setJobContextMap(new HashMap<String, String>());
        quartzContext.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.JOB_CONTEXT.getValue(), context);
        return context;
    }

    private JobContext transferJobContext(JobExecutionContext qContext) {
        JobKey jobKey = qContext.getJobDetail().getKey();
        String currentOffset = jobKey.getGroup();
        Integer prevOffset = Integer.parseInt(currentOffset) - 1;
        String instanceId = getInstanceId(qContext.getJobDetail());

        InstanceJobBean bean = new InstanceJobBean(instanceId, prevOffset);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        InstanceJobBean instanceJob = executor.getInstanceJob(InstanceJobQuery.GET_INSTANCE_JOB);

        String contextData = instanceJob.getContextData();
        JobContext jobContext = JobContext.parseJobContext(contextData);
        // Update the offset to current for job.
        jobContext.setOffset(Integer.parseInt(currentOffset));
        qContext.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.JOB_CONTEXT.getValue(), jobContext);
        return jobContext;
    }

    private String getInstanceId(JobDetail jobDetail) {
        JobKey jobKey = jobDetail.getKey();
        String policyId = jobKey.getName();
        int counter = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.COUNTER.getValue());
        return policyId + "@" + counter;
    }

    private JobContext getJobContext(JobExecutionContext context) {
        return (JobContext) context.getJobDetail().getJobDataMap().get(QuartzDataMapEnum.JOB_CONTEXT.getValue());
    }

    private InstanceExecutionDetails getExecutionDetail(JobExecutionContext context) {
        LOG.info("execution detail {}", context.getResult());
        return new InstanceExecutionDetails((String) context.getResult());
    }

    private void insertJobInstance(String instanceId, int jobCount) {
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

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        JobContext jobContext = getJobContext(context);
        InstanceExecutionDetails detail = getExecutionDetail(context);
        boolean jobSuccessful = isJobSuccessful(detail, jobException);
        LOG.info("execution status of the job [{}], isSuccessful: [{}]", jobContext.getJobInstanceId(), jobSuccessful);
        if (jobSuccessful) {
            updateInstanceJobCompleted(jobContext, JobStatus.SUCCESS, detail.getJobMessage());
            boolean chainNextJob = chainNextJob(context, jobContext);
            if (!chainNextJob) {
                updatePolicyInstanceCompleted(jobContext, detail);
            }
        } else {
            updatePolicyInstanceCompleted(jobContext, detail);
            updateInstanceJobCompleted(jobContext, JobStatus.FAILED, detail.getJobMessage());
            // update all the instance job to failed/aborted.
        }
        //Clean up the job context so it does not get stored into the Quartz tables.
        context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.JOB_CONTEXT.getValue());
    }

    private boolean chainNextJob(JobExecutionContext context, JobContext jobContext) {
        boolean chained = false;
        JobKey currentJobKey = context.getJobDetail().getKey();
        JobKey nextJobKey = chainLinks.get(currentJobKey);
        boolean isChained = context.getJobDetail().getJobDataMap().getBoolean(QuartzDataMapEnum.CHAINED.getValue());
        // next job is available in the cache and it is chain job.
        if (nextJobKey == null && !isChained) {
            return chained;
        }
        // Get the next job from store when it is chained.
        if (nextJobKey == null) {
            nextJobKey = getNextJobFromStore(jobContext.getJobInstanceId(), jobContext.getOffset(),
                    currentJobKey.getName());
        }
        if (nextJobKey == null) {
            LOG.error("this should never happen. next chained job not found for instance id: [{}], offset: [{}]",
                    jobContext.getJobInstanceId(), jobContext.getOffset());
            return false;
        } else {
            chainLinks.put(currentJobKey, nextJobKey);
        }
        try {
            JobDetail nextJobDetail = context.getScheduler().getJobDetail(nextJobKey);
            nextJobDetail.getJobDataMap().put(QuartzDataMapEnum.COUNTER.getValue(),
                    context.getJobDetail().getJobDataMap().getInt(QuartzDataMapEnum.COUNTER.getValue()));
            context.getScheduler().addJob(nextJobDetail, true);
            context.getScheduler().triggerJob(nextJobKey);
            LOG.info("Job [{}] is now chained to job [{}]", currentJobKey, nextJobKey);
            chained = true;
        } catch (SchedulerException se) {
            chained = false;
            LOG.error("Error encountered during chaining to Job [{}]", nextJobKey, se);
        }
        return chained;
    }

    private void updateInstanceJobCompleted(JobContext jobContext, JobStatus status, String message) {
        InstanceJobBean bean = new InstanceJobBean(jobContext.getJobInstanceId(), jobContext.getOffset());
        bean.setStatus(status.name());
        bean.setMessage(truncateMessage(message));
        bean.setEndTime(new Date());
        bean.setContextData(jobContext.toString());
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        executor.executeUpdate(InstanceJobQuery.UPDATE_JOB_COMPLETE);
    }

    private boolean isJobSuccessful(InstanceExecutionDetails detail, JobExecutionException jobException) {
        return !(jobException != null || detail.getJobStatus().equals(JobStatus.FAILED.name()));
    }

    /**
     * When jobs are not found into the cached map, needs to be retrieved from store.
     * @param instanceId instance id
     * @param offset offset
     * @param policyId
     * @return key for the next job
     */
    private JobKey getNextJobFromStore(String instanceId, int offset, String policyId) {
        InstanceJobBean bean = new InstanceJobBean(instanceId, offset + 1);
        InstanceJobExecutor executor = new InstanceJobExecutor(bean);
        InstanceJobBean instanceJob = executor.getInstanceJob(InstanceJobQuery.GET_INSTANCE_JOB);
        return instanceJob != null
                ? new JobKey(policyId, String.valueOf(instanceJob.getOffset()))
                : null;
    }

    private void updatePolicyInstanceCompleted(JobContext jobContext, InstanceExecutionDetails details) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setStatus(details.getJobStatus());
        bean.setMessage(truncateMessage(details.getJobMessage()));
        LOG.info("Setting job execution type: [{}]", details.getJobExecutionType());
        if (StringUtils.isNotBlank(details.getJobExecutionType())) {
            bean.setJobExecutionType(details.getJobExecutionType().toLowerCase());
        }
        bean.setEndTime(new Date());
        bean.setInstanceId(jobContext.getJobInstanceId());
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_COMPLETE);
    }

    void addJobChainLink(JobKey firstJob, JobKey secondJob) {
        if (firstJob == null || secondJob == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }

        if (firstJob.getName() == null || secondJob.getName() == null) {
            throw new IllegalArgumentException("Key cannot have a null name!");
        }
        LOG.info("Job [key: {}] is chained with Job [key: {}]", firstJob, secondJob);
        chainLinks.put(firstJob, secondJob);
    }

    private String insertPolicyInstance(String policyId, int count) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        String instanceId = policyId + "@" + count;
        bean.setInstanceId(instanceId);
        bean.setPolicyId(policyId);
        bean.setStartTime(new Date());
        bean.setRunCount(0);
        bean.setStatus(JobStatus.RUNNING.name());
        bean.setCurrentOffset(0);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.execute();
        return instanceId;
    }

    private int getAndUpdateCounter(JobDetail jobDetail) {
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        int count = jobDataMap.getInt(QuartzDataMapEnum.COUNTER.getValue());
        count++;
        jobDataMap.put(QuartzDataMapEnum.COUNTER.getValue(), count);
        return count;
    }

    private String truncateMessage(String message) {
        return (message.length() > 4000)
                ? message.substring(0, 3899) + " ..."
                : message;
    }
}
