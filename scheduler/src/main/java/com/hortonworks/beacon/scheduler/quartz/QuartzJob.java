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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.InstanceExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationImplFactory;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.store.JobStatus;
import org.apache.commons.lang.StringUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private ReplicationJobDetails details;

    public void setDetails(ReplicationJobDetails details) {
        this.details = details;
    }

    public void execute(JobExecutionContext context) {
        this.runningThread.set(Thread.currentThread());
        JobKey jobKey = context.getJobDetail().getKey();
        details = (ReplicationJobDetails) context.getJobDetail().getJobDataMap().
                get(QuartzDataMapEnum.DETAILS.getValue());
        LOG.info("Job [key: {}] [type: {}] execution started.", jobKey, details.getType());
        DRReplication drReplication = ReplicationImplFactory.getReplicationImpl(details);
        String jobContext;
        if (drReplication != null) {
            try {
                // loop is to skip the further checking of interrupt, so break;
                do {
                    if (checkInterruption()) {
                        LOG.info("quartz interrupt detected before inti()");
                        break;
                    }
                    drReplication.init();

                    if (checkInterruption()) {
                        LOG.info("quartz interrupt detected before performReplication()");
                        break;
                    }
                    drReplication.performReplication();

                    if (checkInterruption()) {
                        LOG.info("quartz interrupt detected before getJobExecutionContextDetails()");
                        break;
                    }
                    jobContext = drReplication.getJobExecutionContextDetails();
                    if (StringUtils.isNotBlank(jobContext)) {
                        context.setResult(jobContext);
                    }
                } while (false);

                if (checkInterruption()) {
                    processInterrupt(jobKey, context);
                }
            } catch (BeaconException ex) {
                LOG.error("Exception occurred while doing replication instance execution :" + ex);
                try {
                    jobContext = drReplication.getJobExecutionContextDetails();
                    if (StringUtils.isNotBlank(jobContext)) {
                        context.setResult(jobContext);
                    }
                } catch (BeaconException e) {
                    LOG.error(e.getMessage(), e);
                }
                if (checkInterruption()) {
                    processInterrupt(jobKey, context);
                }
            }
            LOG.info("Job [key: {}] [type: {}] execution finished.", jobKey, details.getType());
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptFlag.set(true);
        Thread thread = runningThread.get();
        if (thread != null) {
            thread.interrupt();
        }
    }

    private boolean checkInterruption() {
        return interruptFlag.get() || runningThread.get().isInterrupted();
    }

    // In case of interruption instance should be marked as KILLED.
    private void processInterrupt(JobKey jobKey, JobExecutionContext context) {
        LOG.info("Processing interrupt for job: {}, type {}.", jobKey.getName(), jobKey.getGroup());
        try {
            String result = (String) context.getResult();
            if (StringUtils.isNotBlank(result)) {
                InstanceExecutionDetails executionDetails = new InstanceExecutionDetails(result);
                executionDetails.setJobStatus(JobStatus.KILLED.name());
                context.setResult(executionDetails.toJsonString());
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while processing interrupt. Message: {}", e.getMessage(), e);
            //It should not throw any exception, As it can cause error into job listener handler.
        }
    }
}
