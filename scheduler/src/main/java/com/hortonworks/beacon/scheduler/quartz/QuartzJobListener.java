/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.store.BeaconStoreException;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.listeners.JobListenerSupport;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Beacon extended implementation for JobListenerSupport.
 */
public class QuartzJobListener extends JobListenerSupport {

    private static final BeaconLog LOG = BeaconLog.getLog(QuartzJobListener.class);
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
            BeaconLogUtils.setLogInfo(instanceId);

            if (isRetry) {
                int instanceRunCount = StoreHelper.getInstanceRunCount(jobContext);
                int jobRunCount = StoreHelper.getJobRunCount(jobContext);
                StoreHelper.updateJobRunCount(jobContext, ++jobRunCount);
                StoreHelper.updateInstanceRunCount(jobContext, ++instanceRunCount);
            }

            recoveryFlag(context, jobContext);
            context.getJobDetail().getJobDataMap().put(QuartzDataMapEnum.JOB_CONTEXT.getValue(), jobContext);
            LOG.info(MessageCode.SCHD_000042.name(), instanceId, isRetry);
            StoreHelper.updateInstanceCurrentOffset(jobContext);
            boolean parallelExecution = ParallelExecution.checkParallelExecution(context);
            if (!parallelExecution) {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.RUNNING);
                SchedulerCache.get().updateInstanceSchedulerDetail(context.getJobDetail().getKey().getName(),
                        instanceId);
            } else {
                StoreHelper.updateInstanceJobStatusStartTime(jobContext, JobStatus.IGNORED);
                LOG.info(MessageCode.SCHD_000043.name(), instanceId, JobStatus.IGNORED.name());
            }
        } catch (Throwable e) {
            LOG.error(MessageCode.SCHD_000068.name(), e.getMessage(), e);
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
    private JobContext initializeJobContext(String instanceId) {
        JobContext context = new JobContext();
        context.setOffset(0);
        context.setJobInstanceId(instanceId);
        context.setShouldInterrupt(new AtomicBoolean(false));
        context.setJobContextMap(new HashMap<String, String>());
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
        LOG.info(MessageCode.SCHD_000044.name(), instanceDetail);
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
                StoreHelper.updatePolicyInstanceCompleted(jobContext, JobStatus.IGNORED.name(), message);
                StoreHelper.updateInstanceJobCompleted(jobContext, JobStatus.IGNORED.name(), message);
                StoreHelper.updateRemainingInstanceJobs(jobContext, JobStatus.IGNORED.name());
                return;
            }
            InstanceExecutionDetails detail = extractExecutionDetail(jobContext);
            boolean jobFailed = isJobFailed(jobException, detail.getJobStatus());
            boolean isRetry = getFlag(QuartzDataMapEnum.IS_RETRY.getValue(), jobDataMap);
            LOG.info(MessageCode.SCHD_000045.name(),
                    jobContext.getOffset(), jobFailed, isRetry);
            if (isRetry && detail.getJobStatus().equalsIgnoreCase(JobStatus.FAILED.name())) {
                //If retry is set then add the recovery flags.
                jobDataMap.put(QuartzDataMapEnum.IS_RECOVERY.getValue(), true);
                jobDataMap.put(QuartzDataMapEnum.RECOVER_INSTANCE.getValue(), jobContext.getJobInstanceId());
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
        } catch (Throwable e) {
            LOG.error(MessageCode.SCHD_000046.name(), e.getMessage(), e);
        }
    }

    private boolean getFlag(String value, JobDataMap jobDataMap) {
        return jobDataMap.getBoolean(value);
    }

    private boolean chainNextJob(JobExecutionContext context, JobContext jobContext) throws SchedulerException {
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
            LOG.error(MessageCode.SCHD_000047.name(), jobContext.getOffset());
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
        LOG.info(MessageCode.SCHD_000048.name(), currentJobKey, nextJobKey);
        return true;
    }

    private boolean isJobFailed(JobExecutionException jobException, String jobStatus) {
        return (jobException != null
                || jobStatus.equals(JobStatus.FAILED.name())
                || jobStatus.equals(JobStatus.KILLED.name()));
    }

    void addJobChainLink(JobKey firstJob, JobKey secondJob) {
        if (firstJob == null || secondJob == null) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_010008.name(), "Key"));
        }

        if (firstJob.getName() == null || secondJob.getName() == null) {
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.SCHD_000003.name()));
        }
        LOG.info(MessageCode.SCHD_000049.name(), firstJob, secondJob);
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
