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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.scheduler.internal.AdminJobService;
import com.hortonworks.beacon.scheduler.internal.SyncStatusJob;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.listeners.JobListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler.START_NODE_GROUP;

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
            RequestContext.get().startTransaction();
            boolean isRetry = getFlag(QuartzDataMapEnum.IS_RETRY.getValue(), context.getJobDetail().getJobDataMap());
            String instanceId = null;
            JobContext jobContext;
            if (!isRetry) {
                instanceId = handleStartNode(context);
            }

            if (instanceId != null) {
                jobContext = initializeJobContext(instanceId);
            } else {
                // context for non-start nodes gets loaded from DB.
                jobContext = StoreHelper.transferJobContext(context);
                instanceId = jobContext.getJobInstanceId();
            }
            BeaconLogUtils.prefixId(instanceId);

            if (isRetry) {
                int instanceRunCount = StoreHelper.getInstanceRunCount(jobContext);
                int jobRunCount = StoreHelper.getJobRunCount(jobContext);
                StoreHelper.updateJobRunCount(jobContext, ++jobRunCount);
                StoreHelper.updateInstanceRunCount(jobContext, ++instanceRunCount);
            }

            recoveryFlag(context, jobContext);
            context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.JOB_CONTEXT.getValue(), jobContext);
            LOG.info("Policy instance [{}] to be executed. isRetry: [{}]", instanceId, isRetry);
            StoreHelper.updateInstanceCurrentOffset(jobContext);
            boolean parallelExecution = ParallelExecution.checkParallelExecution(context);
            if (!parallelExecution) {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.RUNNING);
                SchedulerCache.get().updateInstanceSchedulerDetail(context.getJobDetail().getKey().getName(),
                        instanceId);
            } else {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.SKIPPED);
                LOG.info("Policy instance [{}] will be skipped with status [{}]", instanceId, JobStatus.SKIPPED.name());
            }
            RequestContext.get().commitTransaction();
        } catch (Throwable e) {
            LOG.error("Error while processing jobToBeExecuted", e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void recoveryFlag(JobExecutionContext context, JobContext jobContext) throws BeaconStoreException {
        boolean recovery = getFlag(QuartzDataMapEnum.IS_RECOVERY.getValue(), context.getJobDetail().getJobDataMap());
        jobContext.setRecovery(recovery);
        if (!recovery) {
            String policyId = context.getJobDetail().getKey().getName();
            String jobOffset = context.getJobDetail().getKey().getGroup();
            String lastInstanceStatus = StoreHelper.getLastInstanceStatus(policyId);
            boolean isRecovery = lastInstanceStatus != null && isJobFailed(null, lastInstanceStatus);
            if (isRecovery) {
                int offset = StoreHelper.getJobOffset(policyId, lastInstanceStatus);
                isRecovery = offset > 0 && Integer.parseInt(jobOffset) == offset;
                jobContext.setRecovery(isRecovery);
            }
        }
    }

    private String handleStartNode(JobExecutionContext context) {
        JobDetail jobDetail = context.getJobDetail();
        JobKey jobKey = jobDetail.getKey();
        if (jobKey.getGroup().equals(START_NODE_GROUP)) {
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
    private JobContext initializeJobContext(String instanceId) {
        JobContext context = new JobContext();
        context.setOffset(0);
        context.setJobInstanceId(instanceId);
        context.setShouldInterrupt(new AtomicBoolean(false));
        context.setRecovery(false);
        return context;
    }


    private JobContext getJobContext(JobExecutionContext context) {
        //Clean up the job context so it does not get stored into the Quartz tables.
        return (JobContext) context.getJobDetail().getJobDataMap().remove(QuartzDataMapEnum.JOB_CONTEXT.getValue());
    }

    private InstanceExecutionDetails extractExecutionDetail(JobContext jobContext) {
        String instanceDetail = jobContext.getJobContextMap().remove(
                InstanceReplication.INSTANCE_EXECUTION_STATUS);
        LOG.info("Instance detail: {}", instanceDetail);
        InstanceExecutionDetails detail = InstanceExecutionDetails.getInstanceExecutionDetails(instanceDetail);
        if (detail == null) {
            // Forced initialization of the instance execution details.
            detail = new InstanceExecutionDetails();
            detail.setJobStatus(JobStatus.FAILED.name());
            detail.setJobMessage("Instance execution detail was not updated properly. Check beacon log for more info.");
        }
        return detail;
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        try {
            RequestContext.get().startTransaction();
            // remove up the recovery related data post execution.
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            jobDataMap.remove(QuartzDataMapEnum.RECOVER_INSTANCE.getValue());
            jobDataMap.remove(QuartzDataMapEnum.IS_RECOVERY.getValue());
            JobContext jobContext = getJobContext(context);
            boolean isParallel = getFlag(QuartzDataMapEnum.IS_PARALLEL.getValue(), jobDataMap);
            if (isParallel) {
                jobDataMap.remove(QuartzDataMapEnum.IS_PARALLEL.getValue());
                String parallelId = (String) jobDataMap.remove(QuartzDataMapEnum.PARALLEL_INSTANCE.getValue());
                String message = StringUtils.isBlank(parallelId)
                        ? "Could not get the parallel instance id, which can happen in some rare cases."
                        : "Parallel instance in execution was: " +parallelId;
                StoreHelper.updatePolicyInstanceCompleted(jobContext, JobStatus.SKIPPED.name(), message);
                StoreHelper.updateInstanceJobCompleted(jobContext, JobStatus.SKIPPED.name(), message);
                StoreHelper.updateRemainingInstanceJobs(jobContext, JobStatus.SKIPPED.name());
                RequestContext.get().commitTransaction();
                return;
            }
            InstanceExecutionDetails detail = extractExecutionDetail(jobContext);
            boolean jobFailed = isJobFailed(jobException, detail.getJobStatus());
            boolean isRetry = getFlag(QuartzDataMapEnum.IS_RETRY.getValue(), jobDataMap);
            LOG.info("Execution status of the job offset: [{}], jobFailed: [{}], isRetry: [{}]",
                    jobContext.getOffset(), jobFailed, isRetry);
            if (isRetry && detail.getJobStatus().equalsIgnoreCase(JobStatus.FAILED.name())) {
                //If retry is set then add the recovery flags.
                jobDataMap.put(QuartzDataMapEnum.IS_RECOVERY.getValue(), true);
                jobDataMap.put(QuartzDataMapEnum.RECOVER_INSTANCE.getValue(), jobContext.getJobInstanceId());
                RequestContext.get().commitTransaction();
                return;
            } else if (isRetry) {
                //If retry is set and job has succeeded remove the flags.
                jobDataMap.remove(QuartzDataMapEnum.IS_RECOVERY.getValue());
                jobDataMap.remove(QuartzDataMapEnum.RECOVER_INSTANCE.getValue());
                jobDataMap.remove(QuartzDataMapEnum.IS_RETRY.getValue());
                jobDataMap.remove(QuartzDataMapEnum.RETRY_MARKER.getValue());
            }
            if (!jobFailed) {
                StoreHelper.updateInstanceJobCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                boolean chainNextJob = chainNextJob(context, jobContext);
                if (!chainNextJob) {
                    StoreHelper.updatePolicyInstanceCompleted(jobContext,
                            detail.getJobStatus(), detail.getJobMessage());
                    StoreHelper.updatePolicyLastInstanceStatus(context.getJobDetail().getKey().getName(),
                            detail.getJobStatus());
                }
            } else {
                StoreHelper.updatePolicyInstanceCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                StoreHelper.updatePolicyLastInstanceStatus(context.getJobDetail().getKey().getName(),
                        detail.getJobStatus());
                StoreHelper.updateInstanceJobCompleted(jobContext, detail.getJobStatus(), detail.getJobMessage());
                StoreHelper.updateRemainingInstanceJobs(jobContext, detail.getJobStatus());
                jobDataMap.put(QuartzDataMapEnum.IS_FAILURE.getValue(), true);
                // update all the instance job to failed/aborted.
            }
            // For the successful jobs END job will have isChained as false.
            // For the failed jobs, as further jobs for the instance will not be launched so should if it was last one.
            // We should compare the current time with policy end time.
            boolean isChained = getFlag(QuartzDataMapEnum.CHAINED.getValue(), context.getJobDetail().getJobDataMap());
            JobKey key = context.getJobDetail().getKey();
            if ((!isChained || jobFailed) && StoreHelper.isEndTimeReached(key.getName())) {
                TriggerKey triggerKey = new TriggerKey(key.getName(), START_NODE_GROUP);
                Trigger trigger = context.getScheduler().getTrigger(triggerKey);
                if (trigger == null) {
                    LOG.info("Trigger [{}] is finalized and removed.", triggerKey);
                    String status = StoreHelper.updatePolicyStatus(key.getName());
                    syncPolicyCompletionStatus(key.getName(), status);
                    LOG.info("Policy completed with final status: [{}].", status);
                }
            }
            RequestContext.get().commitTransaction();
        } catch (Throwable e) {
            LOG.error("Error while processing jobWasExecuted", e);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void syncPolicyCompletionStatus(String policyId, String status) throws BeaconException {
        SyncStatusJob syncStatusJob = StoreHelper.getSyncStatusJob(policyId, status);
        if (syncStatusJob == null) {
            return;
        }
        AdminJobService adminJobService = Services.get().getService(AdminJobService.class);
        int frequency = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncFrequency();
        int maxRetry = BeaconConfig.getInstance().getScheduler().getHousekeepingSyncMaxRetry();
        adminJobService.checkAndSchedule(syncStatusJob, frequency, maxRetry);
    }

    private boolean getFlag(String value, JobDataMap jobDataMap) {
        return jobDataMap.getBoolean(value);
    }

    private boolean chainNextJob(JobExecutionContext context, JobContext jobContext)
            throws SchedulerException, BeaconException {
        JobKey currentJobKey = context.getJobDetail().getKey();
        JobKey nextJobKey = chainLinks.get(currentJobKey);
        boolean isChained = getFlag(QuartzDataMapEnum.CHAINED.getValue(), context.getJobDetail().getJobDataMap());
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
            String msg = "Internal error. Next chained job not found for current job: [" + currentJobKey + "]";
            failCurrentInstance(context, jobContext, msg);
            throw new BeaconException(msg);
        } else {
            chainLinks.put(currentJobKey, nextJobKey);
        }
        // This passing of the counter is required to load the context for next job.
        // (check: StoreHelper#transferJobContext)
        JobDetail nextJobDetail = context.getScheduler().getJobDetail(nextJobKey);
        if (nextJobDetail == null) {
            // The next job is not found in the scheduler, consider the job as failed and update accordingly.
            // Usually it can happen when policy is delete operation executed and its respective jobs are removed
            // scheduler, otherwise there is some issue with scheduler.
            String msg = "Could not find job [" + nextJobKey + "] in the scheduler.";
            failCurrentInstance(context, jobContext, msg);
            throw new BeaconException(msg);
        }
        nextJobDetail.getJobDataMap().put(QuartzDataMapEnum.COUNTER.getValue(),
                context.getJobDetail().getJobDataMap().getInt(QuartzDataMapEnum.COUNTER.getValue()));
        context.getScheduler().addJob(nextJobDetail, true);
        context.getScheduler().triggerJob(nextJobKey);
        LOG.info("Job [{}] is now chained to job [{}]", currentJobKey, nextJobKey);
        return true;
    }

    private void failCurrentInstance(JobExecutionContext context, JobContext jobContext, String message)
            throws SchedulerException {
        Date retireDate = new Date();
        StoreHelper.updatePolicyInstanceFailRetire(jobContext, JobStatus.FAILED.name(), message, retireDate);
        StoreHelper.updatePolicyLastInstanceStatus(context.getJobDetail().getKey().getName(),
                JobStatus.FAILED.name());
        StoreHelper.updateInstanceJobFailRetire(jobContext, JobStatus.FAILED.name(), message, retireDate);
        StoreHelper.retireRemainingInstanceJobs(jobContext, JobStatus.FAILED.name(), retireDate);
        context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.IS_FAILURE.getValue(), true);
    }

    private boolean isJobFailed(JobExecutionException jobException, String jobStatus) {
        return (jobException != null
                || jobStatus.equals(JobStatus.FAILED.name())
                || jobStatus.equals(JobStatus.KILLED.name()));
    }

    void addJobChainLink(JobKey firstJob, JobKey secondJob) {
        if (firstJob == null || secondJob == null) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        if (firstJob.getName() == null || secondJob.getName() == null) {
            throw new IllegalArgumentException("Key cannot have a null name!");
        }
        LOG.info("Job [key: {}] is chained with job [key: {}]", firstJob, secondJob);
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
