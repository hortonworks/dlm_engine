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

import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.BeaconJobImplFactory;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang.StringUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Beacon job for Quartz.
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class QuartzJob implements InterruptableJob {

    private AtomicReference<Thread> runningThread = new AtomicReference<>();
    private AtomicBoolean interruptFlag = new AtomicBoolean(false);
    private static final Logger LOG = LoggerFactory.getLogger(QuartzJob.class);

    private JobContext jobContext;
    private ReplicationJobDetails jobDetail = null;

    public void execute(JobExecutionContext context) {
        this.runningThread.set(Thread.currentThread());
        JobDataMap qJobDataMap = context.getJobDetail().getJobDataMap();

        // check parallel execution and return immediately if yes.
        boolean isParallel = qJobDataMap.getBoolean(QuartzDataMapEnum.IS_PARALLEL.getValue());
        if (isParallel) {
            return;
        }

        jobContext = (JobContext) qJobDataMap.get(QuartzDataMapEnum.JOB_CONTEXT.getValue());
        jobDetail = (ReplicationJobDetails) qJobDataMap.get(QuartzDataMapEnum.DETAILS.getValue());
        BeaconLogUtils.createPrefix(jobContext.getJobInstanceId());

        JobKey jobKey = context.getJobDetail().getKey();
        LOG.info("Job [instance: {}, offset: {}, type: {}] execution started.", jobContext.getJobInstanceId(),
                jobContext.getOffset(), jobDetail.getType());
        BeaconJob drReplication = BeaconJobImplFactory.getBeaconJobImpl(jobDetail);

        // Check for any interrupt which occurred before starting the execution.
        if (SchedulerCache.get().getInterrupt(jobKey.getName())) {
            processInterrupt(jobKey, "Interrupt detected at the start.");
            BeaconLogUtils.deletePrefix();
            return;
        }
        if (drReplication != null) {
            try {
                // loop is to skip the further checking of interrupt, so break;
                String interruptPoint = null;
                do {
                    if (checkInterruption()) {
                        interruptPoint = "quartz interrupt detected before init()";
                        break;
                    }
                    drReplication.init(jobContext);

                    if (jobContext.isRecovery()) {
                        if (checkInterruption()) {
                            interruptPoint = "quartz interrupt detected before recover()";
                            break;
                        }
                        drReplication.recover(jobContext);
                    }

                    if (checkInterruption()) {
                        interruptPoint = "quartz interrupt detected before perform()";
                        break;
                    }
                    if (jobContext.isPerformJobAfterRecovery()) {
                        drReplication.perform(jobContext);
                    } else {
                        LOG.info("Skipping perform for instance: {}, type: {}", jobContext.getJobInstanceId(),
                            jobDetail.getType());
                    }

                    if (checkInterruption()){
                        interruptPoint = "quartz interrupt detected before cleanUp()";
                        break;
                    }
                    drReplication.cleanUp(jobContext);
                } while (false);

                if (checkInterruption()) {
                    processInterrupt(jobKey, interruptPoint);
                }
            } catch (BeaconException ex) {
                LOG.error("Exception occurred while doing replication instance execution: {}", ex);

                // No retry for interrupted (killed) jobs.
                if (checkInterruption()) {
                    processInterrupt(jobKey, null);
                } else {
                    Properties jobProperties = jobDetail.getProperties();
                    Retry retry = new Retry(
                            Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_ATTEMPTS.getName())),
                            Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_DELAY.getName())));
                    RetryReplicationJob.retry(retry, context, jobContext);
                }
            }
            LOG.info("Job [key: {}] [type: {}] execution finished.", jobKey, jobDetail.getType());

        }
        BeaconLogUtils.deletePrefix();
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptFlag.set(true);
        jobContext.shouldInterrupt().set(true);
        LOG.info("Setting the interruptFlag: [{}] and JobContext interrupt flag: [{}]",
                interruptFlag.get(), jobContext.shouldInterrupt().get());
        Thread thread = runningThread.get();
        if (thread != null) {
            // In case of Hive, we do not interrupt the running thread as it create issue for beacon job management.
            if (jobDetail != null && !jobDetail.getType().equalsIgnoreCase(ReplicationType.HIVE.name())) {
                thread.interrupt();
                LOG.info("Interrupted the replication executing thread: [{}]", thread.getName());
            }
        }
    }

    private boolean checkInterruption() {
        return interruptFlag.get() || runningThread.get().isInterrupted();
    }

    // In case of interruption instance should be marked as KILLED.
    private void processInterrupt(JobKey jobKey, String interruptPoint) {
        String message = interruptPoint != null ? interruptPoint : "Interrupt occurred";
        LOG.info("Processing interrupt for job: [{}]", jobKey);
        try {
            String executionStatus = jobContext.getJobContextMap().get(InstanceReplication.INSTANCE_EXECUTION_STATUS);
            if (StringUtils.isBlank(executionStatus)) {
                setInstanceExecDetail(JobStatus.KILLED, message);
            } else {
                InstanceExecutionDetails detail = InstanceExecutionDetails.getInstanceExecutionDetails(executionStatus);
                String jobStatus = detail.getJobStatus();
                switch (JobStatus.valueOf(jobStatus)) {
                    case SUCCESS:
                        detail.setJobStatus(JobStatus.KILLED.name());
                        detail.setJobMessage(message);
                        break;
                    case FAILED:
                        detail.setJobStatus(JobStatus.KILLED.name());
                        break;
                    default:
                        // Nothing to do.
                }
                jobContext.getJobContextMap().put(InstanceReplication.INSTANCE_EXECUTION_STATUS,
                        detail.toJsonString());
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while processing interrupt. Message: {}", e.getMessage(), e);
            //It should not throw any exception, As it can cause error into job listener handler.
        }
    }

    private void setInstanceExecDetail(JobStatus jobStatus, String jobMessage) throws BeaconException {
        InstanceExecutionDetails executionDetails = new InstanceExecutionDetails();
        executionDetails.setJobStatus(jobStatus.name());
        executionDetails.setJobMessage(jobMessage);
        jobContext.getJobContextMap().put(InstanceReplication.INSTANCE_EXECUTION_STATUS,
                executionDetails.toJsonString());
    }
}
