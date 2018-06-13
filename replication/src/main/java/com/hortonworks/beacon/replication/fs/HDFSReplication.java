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

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DefaultFilter;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_FILTERS_CLASS;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS;

/**
 * HDFS FileSystem Replication implementation.
 */
public class HDFSReplication extends FSReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSReplication.class);
    private static final String RAW_NAMESPACE_PATH = "/.reserved/raw";

    public HDFSReplication(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        super.init(jobContext);

        try {
            String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
            String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
            if (Boolean.valueOf(properties.getProperty(FSDRProperties.TDE_SAMEKEY.getName()))) {
                sourceDataset = RAW_NAMESPACE_PATH + sourceDataset;
                targetDataset = RAW_NAMESPACE_PATH + targetDataset;
            }
            sourceStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.SOURCE_NN.getName()),
                    sourceDataset);
            targetStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.TARGET_NN.getName()),
                    targetDataset);
            boolean isTDEon = Boolean.valueOf(properties.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
            if (!isTDEon) {
                isSnapshot = FSSnapshotUtils.isDirectorySnapshottable(properties.getProperty(FSDRProperties
                                .SOURCE_CLUSTER_NAME.getName()), properties.getProperty(FSDRProperties
                                .TARGET_CLUSTER_NAME.getName()), sourceStagingUri, targetStagingUri);
            }
            initializeCustomProperties();
        } catch (Exception e) {
            throw new BeaconException("Exception occurred in HDFS init: ", e);
        }
    }

    @Override
    protected void initializeFileSystem() throws BeaconException {
        super.initializeFileSystem();
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException, InterruptedException {
        String fsReplicationName = getFSReplicationName(sourceFs, sourceStagingUri);
        performCopy(jobContext, fsReplicationName, ReplicationMetrics.JobType.MAIN);
        performPostReplJobExecution(jobContext, job, fsReplicationName,
                ReplicationMetrics.JobType.MAIN);
    }

    String getFSReplicationName(FileSystem fileSystem, String stagingUri)
            throws BeaconException {
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(properties.getProperty(
                FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        LOG.debug("TDE encryption enabled: {}", tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        String fsReplicationName = null;
        if (!tdeEncryptionEnabled) {
            if (isSnapshot
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                fsReplicationName = FSSnapshotUtils.getSnapshotName(getDetails().getName());
                FSSnapshotUtils.handleSnapshotCreation(fileSystem, stagingUri, fsReplicationName);
            }
        }
        return fsReplicationName;
    }

    Job performCopy(JobContext jobContext, String toSnapshot,
                    ReplicationMetrics.JobType jobType) throws BeaconException, InterruptedException {
        return performCopy(jobContext, toSnapshot, getLatestSnapshotOnTargetAvailableOnSource(), jobType);
    }

    Job performCopy(JobContext jobContext, String toSnapshot, String fromSnapshot,
                    ReplicationMetrics.JobType jobType) throws BeaconException, InterruptedException {
        boolean isInRecoveryMode = jobType == ReplicationMetrics.JobType.RECOVERY;
        DistCpOptions options = null;
        try {
            options = getDistCpOptions(toSnapshot, fromSnapshot, isInRecoveryMode);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        Configuration conf = getConfiguration();
        return performCopy(jobContext, options, conf, jobType);
    }

    private Configuration getConfiguration() {
        Configuration conf = getHAConfigOrDefault();
        String queueName = properties.getProperty(FSDRProperties.QUEUE_NAME.getName());
        if (queueName != null) {
            conf.set(BeaconConstants.MAPRED_QUEUE_NAME, queueName);
        }
        if (UserGroupInformation.isSecurityEnabled()) {
            conf.set(BeaconConstants.MAPREDUCE_JOB_HDFS_SERVERS,
                    properties.getProperty(FSDRProperties.SOURCE_NN.getName())
                            + "," + properties.getProperty(FSDRProperties.TARGET_NN.getName()));
            conf.set(BeaconConstants.MAPREDUCE_JOB_SEND_TOKEN_CONF, PolicyHelper.getRMTokenConf());
        }
        conf.set(CONF_LABEL_FILTERS_CLASS, DefaultFilter.class.getName());
        conf.setInt(CONF_LABEL_LISTSTATUS_THREADS, 20);
        conf.set(DistCpConstants.DISTCP_EXCLUDE_FILE_REGEX, BeaconConfig.getInstance()
                .getEngine().getExcludeFileRegex());
        return conf;
    }

    private Configuration getHAConfigOrDefault() {
        Configuration conf = new Configuration();
        if (properties.containsKey(BeaconConstants.HA_CONFIG_KEYS)) {
            String haConfigKeys = properties.getProperty(BeaconConstants.HA_CONFIG_KEYS);
            for(String haConfigKey: haConfigKeys.split(BeaconConstants.COMMA_SEPARATOR)) {
                conf.set(haConfigKey, properties.getProperty(haConfigKey));
            }
        }
        return conf;
    }

    private static ReplicationMetrics getCurrentJobDetails(JobContext jobContext) throws BeaconException {
        String instanceId = jobContext.getJobInstanceId();
        String trackingInfo = ReplicationUtils.getInstanceTrackingInfo(instanceId);

        List<ReplicationMetrics> metrics = ReplicationMetricsUtils.getListOfReplicationMetrics(trackingInfo);
        if (metrics == null || metrics.isEmpty()) {
            LOG.info("No replication job detail found.");
            return null;
        }

        // List can have only 2 jobs: one main job and one recovery distcp job
        if (metrics.size() > 1) {
            // Recovery has kicked in, return recovery job id
            return metrics.get(1);
        } else {
            return metrics.get(0);
        }
    }

    private String getLatestSnapshotOnTargetAvailableOnSource() throws BeaconException {
        String fromSnapshot = null;

        try {
            LOG.debug("Checking snapshot directory on source and target");
            if (isSnapshot && targetFs.exists(new Path(targetStagingUri))) {
                fromSnapshot = FSSnapshotUtils.findLatestReplicatedSnapshot((DistributedFileSystem) sourceFs,
                        (DistributedFileSystem) targetFs, sourceStagingUri, targetStagingUri);
            }
        } catch (IOException e) {
            throw new BeaconException(e, "Error occurred when checking target dir : {} exists ", targetStagingUri);
        }
        return fromSnapshot;
    }

    protected void initializeProperties() throws BeaconException {
        String sourceDS = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDS = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        String sourceCN = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
        String targetCN = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());

        if (!FSUtils.isHCFS(new Path(sourceDS))) {
            Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        } else {
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceDS);
        }

        if (!FSUtils.isHCFS(new Path(targetDS))) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        } else {
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetDS);
        }

        Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
        if (ClusterHelper.isHighlyAvailableHDFS(sourceCluster.getCustomProperties())) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            Map<String, String> haConfigs = getHAConfigs(sourceCluster.getCustomProperties(),
                    targetCluster.getCustomProperties());
            for (Map.Entry<String, String> haConfig : haConfigs.entrySet()) {
                properties.setProperty(haConfig.getKey(), haConfig.getValue());
            }
        }
    }

    private DistCpOptions getDistCpOptions(String toSnapshot, String fromSnapshot, boolean isInRecoveryMode)
            throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        if (isInRecoveryMode) {
            sourceUris.add(new Path(targetStagingUri));
        } else {
            sourceUris.add(new Path(sourceStagingUri));
        }

        return DistCpOptionsUtil.getDistCpOptions(properties, sourceUris, new Path(targetStagingUri),
                isSnapshot, fromSnapshot, toSnapshot, isInRecoveryMode);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job, String fsReplicationName,
                                             ReplicationMetrics.JobType jobType) throws BeaconException {
        try {
            if (job.isComplete() && job.isSuccessful()) {
                if (isSnapshot) {
                    try {
                        FSSnapshotUtils.handleSnapshotCreation(targetFs, targetStagingUri, fsReplicationName);
                        FSSnapshotUtils.handleSnapshotEviction(sourceFs, properties, sourceStagingUri);
                        FSSnapshotUtils.handleSnapshotEviction(targetFs, properties, targetStagingUri);
                    } catch (BeaconException e) {
                        throw new BeaconException("Exception occurred while handling snapshot: ", e);
                    }
                }
                LOG.info("HDFS DistCp copy is successful.");
            }
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        LOG.info("Recover policy instance: [{}]", jobContext.getJobInstanceId());

        ReplicationMetrics currentJobMetric = getCurrentJobDetails(jobContext);
        if (currentJobMetric == null) {
            //Case, when previous instance was failed/killed.
            jobContext.setPerformJobAfterRecovery(true);
            if (!isSnapshot) {
                LOG.info("Policy instance: [{}] is not snapshottable, return", jobContext.getJobInstanceId());
                return;
            }
            handleRecovery(jobContext);
            return;
        }

        LOG.info("Recover job [{}] and job type [{}]", currentJobMetric.getJobId(), currentJobMetric.getJobType());

        RunningJob runningJob = getJobWithRetries(currentJobMetric.getJobId());
        if (runningJob != null) {
            org.apache.hadoop.mapred.JobStatus jobStatus;
            try {
                jobStatus = runningJob.getJobStatus();
                this.job = getJobClient().getClusterHandle().getJob(runningJob.getID());
            } catch (IOException | InterruptedException e) {
                throw new BeaconException(e);
            }

            if (org.apache.hadoop.mapred.JobStatus.State.RUNNING.getValue() == jobStatus.getRunState()
                    || org.apache.hadoop.mapred.JobStatus.State.PREP.getValue() == jobStatus.getRunState()) {
                ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
                try {
                    new DistCp(getConfiguration(), getDistCpOptions(null, null, false));
                    handlePostSubmit(timer, jobContext, ReplicationMetrics.JobType.MAIN);
                    performPostReplJobExecution(jobContext, job,
                            getFSReplicationName(sourceFs, sourceStagingUri),
                            ReplicationMetrics.JobType.MAIN);
                    jobContext.setPerformJobAfterRecovery(false);
                } catch (Exception e) {
                    throw new BeaconException(e);
                } finally {
                    timer.shutdown();
                }
            } else if (org.apache.hadoop.mapred.JobStatus.State.SUCCEEDED.getValue() == jobStatus.getRunState()) {
                performPostReplJobExecution(jobContext, job,
                        getFSReplicationName(sourceFs, sourceStagingUri), ReplicationMetrics.JobType.MAIN);
                jobContext.setPerformJobAfterRecovery(false);
            } else {
                jobContext.setPerformJobAfterRecovery(true);
                if (!isSnapshot) {
                    LOG.info("Policy instance: [{}] is not snapshottable, return", jobContext.getJobInstanceId());
                    return;
                }
                // Job failed for snapshot based replication. Try recovering.
                handleRecovery(jobContext);
            }
        } else {
            if (!isSnapshot) {
                LOG.info("Policy instance: [{}] is not snapshottable, return", jobContext.getJobInstanceId());
                jobContext.setPerformJobAfterRecovery(true);
                return;
            }
            throw new BeaconException("Could not connect to the previous hadoop job {}", currentJobMetric.getJobId());
        }
    }

    private void handleRecovery(JobContext jobContext) throws BeaconException {
        String fromSnapshot = getLatestSnapshotOnTargetAvailableOnSource();
        if (StringUtils.isBlank(fromSnapshot)) {
            LOG.info("ReplicatedSnapshotName is null. No recovery needed for policy instance: [{}], return",
                jobContext.getJobInstanceId());
            return;
        }
        // Create current state on the target cluster
        String toSnapshot = "tempRecoverySnapshot";
        FSSnapshotUtils.checkAndCreateSnapshot(targetFs, targetStagingUri, toSnapshot);

        // toSnapshot is created for recovery so swap the parameter between (toSnapshot and fromSnapshot)
        Job job = null;
        try {
            SnapshotDiffReport diffReport = ((DistributedFileSystem) targetFs).getSnapshotDiffReport(
                    new Path(targetStagingUri), fromSnapshot, toSnapshot);
            List diffList = diffReport.getDiffList();
            if (diffList == null || diffList.isEmpty()) {
                LOG.info("No recovery needed for policy instance: [{}], return", jobContext.getJobInstanceId());
                FSSnapshotUtils.checkAndDeleteSnapshot(targetFs, targetStagingUri, toSnapshot);
                return;
            }
            LOG.info("Recovery needed for policy instance: [{}]. Start recovery!", jobContext.getJobInstanceId());
            job = performCopy(jobContext, fromSnapshot, toSnapshot, ReplicationMetrics.JobType.RECOVERY);
            FSSnapshotUtils.checkAndDeleteSnapshot(targetFs, targetStagingUri, toSnapshot);
            // Re-create the same snapshot for replication purpose.
            FSSnapshotUtils.checkAndCreateSnapshot(targetFs, targetStagingUri, fromSnapshot);
            if (job == null) {
                throw new BeaconException("FS Replication recovery job is null");
            }
        } catch (Exception e) {
            String msg = "Error occurred when getting diff report for target dir: " + targetStagingUri + ", "
                    + "fromSnapshot: " + fromSnapshot + " & toSnapshot: " + toSnapshot;
            throw new BeaconException(msg, e);
        }
    }
}
