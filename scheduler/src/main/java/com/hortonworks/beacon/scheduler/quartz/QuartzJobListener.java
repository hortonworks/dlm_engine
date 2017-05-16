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

import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.InstanceReplication;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.listeners.JobListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Beacon extended implementation for JobListenerSupport.
 */
public class QuartzJobListener extends JobListenerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobListener.class);
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
        try {
            String instanceId = handleStartNode(context);
            JobContext jobContext;
            if (instanceId != null) {
                jobContext = initializeJobContext(context, instanceId);
            } else {
                // context for non-start nodes gets loaded from DB.
                jobContext = StoreHelper.transferJobContext(context);
                instanceId = jobContext.getJobInstanceId();
            }
            LOG.info("policy instance [{}] to be executed.", instanceId);
            StoreHelper.updateInstanceCurrentOffset(jobContext);
            boolean parallelExecution = ParallelExecution.checkParallelExecution(context);
            if (!parallelExecution) {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.RUNNING);
            } else {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.IGNORED);
                LOG.info("policy instance [{}] will be ignored with status [{}]", instanceId, JobStatus.IGNORED.name());
            }
        } catch (Throwable e) {
            LOG.error("error while processing jobToBeExecuted. Message: {}", e.getMessage(), e);
        }
    }

    private String handleStartNode(JobExecutionContext context) {
        JobDetail jobDetail = context.getJobDetail();
        JobKey jobKey = jobDetail.getKey();
        if (jobKey.getGroup().equals(BeaconQuartzScheduler.START_NODE_GROUP)) {
            String policyId = jobKey.getName();
            String instanceId = StoreHelper.insertPolicyInstance(policyId, getAndUpdateCounter(jobDetail),
                    JobStatus.RUNNING.name());
            int jobCount = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.NO_OF_JOBS.getValue());
            StoreHelper.insertJobInstance(instanceId, jobCount);
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


    private JobContext getJobContext(JobExecutionContext context) {
        //Clean up the job context so it does not get stored into the Quartz tables.
        return (JobContext) context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.JOB_CONTEXT.getValue());
    }

    private InstanceExecutionDetails extractExecutionDetail(JobContext jobContext) {
        String instanceDetail = jobContext.getJobContextMap().remove(
                InstanceReplication.INSTANCE_EXECUTION_STATUS);
        LOG.info("Instance Detail : {}", instanceDetail);
        return InstanceExecutionDetails.getInstanceExecutionDetails(instanceDetail);
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        try {
            JobContext jobContext = getJobContext(context);
            boolean isParallel = context.getJobDetail().getJobDataMap()
                    .getBoolean(QuartzDataMapEnum.IS_PARALLEL.getValue());
            if (isParallel) {
                context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.IS_PARALLEL.getValue());
                String parallelId = (String) context.getJobDetail().getJobDataMap()
                        .remove(QuartzDataMapEnum.PARALLEL_INSTANCE.getValue());
                String message = "Parallel instance in execution was: " + parallelId;
                StoreHelper.updatePolicyInstanceCompleted(jobContext, JobStatus.IGNORED.name(), message);
                StoreHelper.updateInstanceJobCompleted(jobContext, JobStatus.IGNORED.name(), message);
                StoreHelper.updateRemainingInstanceJobs(jobContext, JobStatus.IGNORED.name());
                return;
            }
            InstanceExecutionDetails detail = extractExecutionDetail(jobContext);
            boolean jobSuccessful = isJobSuccessful(detail, jobException);
            LOG.info("execution status of the job [instance: {}, offset: {}], isSuccessful: [{}]",
                    jobContext.getJobInstanceId(), jobContext.getOffset(), jobSuccessful);
            if (jobSuccessful) {
                StoreHelper.updateInstanceJobCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                boolean chainNextJob = chainNextJob(context, jobContext);
                if (!chainNextJob) {
                    StoreHelper.updatePolicyInstanceCompleted(jobContext,
                            detail.getJobStatus(), detail.getJobMessage());
                }
            } else {
                StoreHelper.updatePolicyInstanceCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                StoreHelper.updateInstanceJobCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                StoreHelper.updateRemainingInstanceJobs(jobContext, detail.getJobStatus());
                context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_FAILURE.getValue(), true);
                // update all the instance job to failed/aborted.
            }
        } catch (Throwable e) {
            LOG.error("error while processing jobWasExecuted. Message: {}", e.getMessage(), e);
        }
    }

    private boolean chainNextJob(JobExecutionContext context, JobContext jobContext) throws SchedulerException {
        JobKey currentJobKey = context.getJobDetail().getKey();
        JobKey nextJobKey = chainLinks.get(currentJobKey);
        boolean isChained = context.getJobDetail().getJobDataMap().getBoolean(QuartzDataMapEnum.CHAINED.getValue());
        // next job is not available in the cache and it is not chained job.
        if (nextJobKey == null && !isChained) {
            return false;
        }
        // Get the next job from store when it is chained.
        if (nextJobKey == null) {
            nextJobKey = StoreHelper.getNextJobFromStore(jobContext.getJobInstanceId(), jobContext.getOffset(),
                    currentJobKey.getName());
        }
        if (nextJobKey == null) {
            LOG.error("this should never happen. next chained job not found for instance id: [{}], offset: [{}]",
                    jobContext.getJobInstanceId(), jobContext.getOffset());
            return false;
        } else {
            chainLinks.put(currentJobKey, nextJobKey);
        }
        // This passing of the counter is required to load the context for next job.
        // (check: StoreHelper#transferJobContext)
        JobDetail nextJobDetail = context.getScheduler().getJobDetail(nextJobKey);
        nextJobDetail.getJobDataMap().put(QuartzDataMapEnum.COUNTER.getValue(),
                context.getJobDetail().getJobDataMap().getInt(QuartzDataMapEnum.COUNTER.getValue()));
        context.getScheduler().addJob(nextJobDetail, true);
        context.getScheduler().triggerJob(nextJobKey);
        LOG.info("Job [{}] is now chained to job [{}]", currentJobKey, nextJobKey);
        return true;
    }

    private boolean isJobSuccessful(InstanceExecutionDetails detail, JobExecutionException jobException) {
        return !(jobException != null
                || detail.getJobStatus().equals(JobStatus.FAILED.name())
                || detail.getJobStatus().equals(JobStatus.KILLED.name()));
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

    private int getAndUpdateCounter(JobDetail jobDetail) {
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        int count = jobDataMap.getInt(QuartzDataMapEnum.COUNTER.getValue());
        count++;
        jobDataMap.put(QuartzDataMapEnum.COUNTER.getValue(), count);
        return count;
    }
}
