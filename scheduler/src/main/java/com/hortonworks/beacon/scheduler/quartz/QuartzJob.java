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
import com.hortonworks.beacon.entity.FSDRProperties;
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
import com.hortonworks.beacon.plugin.service.PluginJobBuilder;
import com.hortonworks.beacon.plugin.service.PluginJobProperties;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
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
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Beacon job for Quartz.
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class QuartzJob implements InterruptableJob {
    private AtomicBoolean interruptFlag = new AtomicBoolean(false);
    private static final Logger LOG = LoggerFactory.getLogger(QuartzJob.class);

    private JobContext jobContext;
    private ReplicationJobDetails jobDetail = null;
    private PolicyDao policyDao = new PolicyDao();
    private BeaconJob replicationJob = null;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobKey jobKey = null;
        try {
            JobDataMap qJobDataMap = context.getJobDetail().getJobDataMap();
            jobContext = (JobContext) qJobDataMap.get(QuartzDataMapEnum.JOB_CONTEXT.getValue());

            // check parallel execution and return immediately if yes.
            boolean isParallel = qJobDataMap.getBoolean(QuartzDataMapEnum.IS_PARALLEL.getValue());
            if (isParallel) {
                return;
            }

            jobDetail = (ReplicationJobDetails) qJobDataMap.get(QuartzDataMapEnum.DETAILS.getValue());
            jobKey = context.getJobDetail().getKey();
            LOG.info("Job [instance: {}, offset: {}, type: {}] execution started.", jobContext.getJobInstanceId(),
                jobContext.getOffset(), jobDetail.getType());
            jobDetail.setProperties(buildProperties(jobDetail));

            replicationJob = BeaconJobImplFactory.getBeaconJobImpl(jobDetail);

            checkInterruption(jobKey, "before init");
            replicationJob.init(jobContext);

            checkInterruption(jobKey, "before recover");
            replicationJob.recover(jobContext);

            checkInterruption(jobKey, "before perform");
            if (jobContext.isPerformJobAfterRecovery()) {
                replicationJob.perform(jobContext);
            } else {
                LOG.info("Skipping perform for instance: {}, type: {}", jobContext.getJobInstanceId(),
                    jobDetail.getType());
            }

            LOG.info("Job [key: {}] [type: {}] execution finished.", jobKey, jobDetail.getType());
            setInstanceExecDetail(JobStatus.SUCCESS, "Instance succeeded");
        } catch (InterruptedException e) {
            LOG.info("Handling interrupt", e);
            processInterrupt(jobKey, e.getMessage());
            throw new JobExecutionException(e);
        } catch (Throwable ex) {
            LOG.error("Exception occurred while doing replication instance execution: ", ex);

            try {
                checkInterruption(jobKey, "handle failure");
            } catch (InterruptedException e) {
                LOG.info("Handling interrupt", e);
                processInterrupt(jobKey, "after failure");
                throw new JobExecutionException(e);
            }

            Properties jobProperties = jobDetail.getProperties();
            Retry retry = new Retry(
                    Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_ATTEMPTS.getName())),
                    Integer.parseInt(jobProperties.getProperty(FSDRProperties.RETRY_DELAY.getName())));
            RetryReplicationJob.retry(retry, context, jobContext);
            setInstanceExecDetail(JobStatus.FAILED, ex.getMessage());
            throw new JobExecutionException(ex);
        } finally {
            if (replicationJob != null) {
                try {
                    replicationJob.cleanUp(jobContext);
                } catch (Throwable t) {
                    LOG.warn("Ignoring cleanup failure", t);
                }
            }
        }
    }

    private void checkInterruption(JobKey jobKey, String interruptPoint) throws InterruptedException {
        if (interruptFlag.get() || SchedulerCache.get().getInterrupt(jobKey.getName())) {
            throw new InterruptedException("Interrupt detected " + interruptPoint);
        }
    }

    // In case of interruption instance should be marked as KILLED.
    private void processInterrupt(JobKey jobKey, String interruptPoint) {
        String message = "Interrupt occurred " + (interruptPoint != null ? interruptPoint : "");
        LOG.info("Processing interrupt for job: [{}]", jobKey);
        try {
            if (replicationJob != null) {
                try {
                    replicationJob.interrupt();
                } catch (BeaconException e) {
                    LOG.warn("Error in replicationJob.interrupt", e);
                }
            }

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

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptFlag.set(true);                //For QuartzJob to check if its interrupted
        jobContext.shouldInterrupt().set(true); //For BeaconJob to check if its interrupted

        if (replicationJob != null) {
            try {
                replicationJob.interrupt();     //If BeaconJob is interruptable, interrupt
            } catch (BeaconException e) {
                LOG.warn("Failed to interrupt ");
            }
        }
    }
}
