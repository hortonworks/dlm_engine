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

import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.BeaconJobImplFactory;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.fs.FSDRProperties;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import org.apache.commons.lang.StringUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;

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
    private static final BeaconLog LOG = BeaconLog.getLog(QuartzJob.class);

    private JobContext jobContext;

    public void execute(JobExecutionContext context) {
        this.runningThread.set(Thread.currentThread());
        JobDataMap qJobDataMap = context.getJobDetail().getJobDataMap();

        // check parallel execution and return immediately if yes.
        boolean isParallel = qJobDataMap.getBoolean(QuartzDataMapEnum.IS_PARALLEL.getValue());
        if (isParallel) {
            return;
        }

        jobContext = (JobContext) qJobDataMap.get(QuartzDataMapEnum.JOB_CONTEXT.getValue());
        ReplicationJobDetails details = (ReplicationJobDetails) qJobDataMap.get(QuartzDataMapEnum.DETAILS.getValue());
        BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());

        JobKey jobKey = context.getJobDetail().getKey();
        LOG.info("Job [instance: {}, offset: {}, type: {}] execution started.", jobContext.getJobInstanceId(),
                jobContext.getOffset(), details.getType());
        BeaconJob drReplication = BeaconJobImplFactory.getBeaconJobImpl(details);

        // Check for any interrupt which occurred before starting the execution.
        if (SchedulerCache.get().getInterrupt(jobKey.getName())) {
            processInterrupt(jobKey, "Interrupt detected at the start.");
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
                    drReplication.perform(jobContext);

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
                LOG.error("Exception occurred while doing replication instance execution : {}", ex);

                // No retry for interrupted (killed) jobs.
                if (checkInterruption()) {
                    processInterrupt(jobKey, null);
                } else {
                    Properties jobProperties = details.getProperties();
                    Retry retry = new Retry(
                            Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_ATTEMPTS.getName())),
                            Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_DELAY.getName())));
                    RetryReplicationJob.retry(retry, context, jobContext);
                }
            }
            LOG.info("Job [key: {}] [type: {}] execution finished.", jobKey, details.getType());
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptFlag.set(true);
        jobContext.shouldInterrupt().set(true);
        LOG.info("Setting the interruptFlag: [{}] and JobContext interrupt flag: [{}]",
                interruptFlag.get(), jobContext.shouldInterrupt().get());
        Thread thread = runningThread.get();
        if (thread != null) {
            thread.interrupt();
            LOG.info("Interrupted the replication executing thread: [{}]", thread.getName());
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
