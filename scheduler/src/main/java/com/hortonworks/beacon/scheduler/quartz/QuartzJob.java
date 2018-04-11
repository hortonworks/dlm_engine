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

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.BeaconJobImplFactory;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.plugin.service.PluginJobProperties;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.beacon.util.StringFormat;
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
    private PolicyDao policyDao = new PolicyDao();

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

        BeaconLogUtils.prefixId(jobContext.getJobInstanceId());

        JobKey jobKey = context.getJobDetail().getKey();
        LOG.info("Job [instance: {}, offset: {}, type: {}] execution started.", jobContext.getJobInstanceId(),
                jobContext.getOffset(), jobDetail.getType());
        try {
            jobDetail.setProperties(buildProperties(jobDetail));
        } catch (BeaconException ex) {
            LOG.error(ex.getMessage(), ex);
            BeaconLogUtils.deletePrefix();
            setInstanceExecDetail(JobStatus.FAILED, ex.getMessage());
            return;
        }
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
                LOG.error("Exception occurred while doing replication instance execution: ", ex);

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
            LOG.error("Exception occurred while processing interrupt", e);
            //It should not throw any exception, As it can cause error into job listener handler.
        }
    }

    private void setInstanceExecDetail(JobStatus jobStatus, String jobMessage) {
        InstanceExecutionDetails executionDetails = new InstanceExecutionDetails();
        executionDetails.setJobStatus(jobStatus.name());
        executionDetails.setJobMessage(jobMessage);
        jobContext.getJobContextMap().put(InstanceReplication.INSTANCE_EXECUTION_STATUS,
                executionDetails.toJsonString());
    }

    private Properties buildProperties(ReplicationJobDetails details) throws BeaconException {
        ReplicationPolicy policy = policyDao.getActivePolicy(details.getName());
        boolean policyHCFS = PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset());
        if (!policyHCFS) {
            checkClustersPairingStatus(policy.getSourceCluster(), policy.getTargetCluster());
        }
        ReplicationType replicationType = ReplicationHelper.getReplicationType(details.getType());
        Properties localProperties;
        switch (replicationType) {
            case FS:
                localProperties = FSPolicyHelper.buildFSReplicationProperties(policy);
                break;
            case HIVE:
                if (details.getProperties().containsKey(HiveDRProperties.JOB_ACTION_TYPE.getName())) {
                    String hiveJobType = details.getProperties().getProperty(
                            HiveDRProperties.JOB_ACTION_TYPE.getName());
                    localProperties = HivePolicyHelper.buildHiveReplicationProperties(policy, hiveJobType);
                } else {
                    localProperties = HivePolicyHelper.buildHiveReplicationProperties(policy);
                }
                break;
            case PLUGIN:
                String pluginType = details.getProperties().getProperty(PluginJobProperties.JOB_TYPE.getName());
                String actionType = details.getProperties().getProperty(PluginJobProperties.JOBACTION_TYPE.getName());
                localProperties = PluginJobBuilder.buildPluginProperties(policy, pluginType, actionType);
                break;
            default:
                localProperties = new Properties();
        }
        return localProperties;
    }

    private void checkClustersPairingStatus(String source, String target) throws BeaconException {
        boolean paired = ClusterHelper.areClustersPaired(source, target);
        if (!paired) {
            String message = StringFormat.format("Cluster [{}] and [{}] are not paired.", source, target);
            throw  new BeaconException(message);
        }
        boolean suspended = ClusterHelper.areClustersSuspended(source, target);
        if (suspended) {
            String message = StringFormat.format("Cluster pair for [{}] and [{}] is suspended.", source, target);
            throw  new BeaconException(message);
        }
    }
}
